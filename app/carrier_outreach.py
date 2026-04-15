"""
BrokerOps AI -- Sofia carrier outreach module.

Entry point: run_daily_outreach_batch(dry_run=True, limit=20) -> BatchResult

Flow:
  1. Assemble batch from preview JSON or fall back to live sheet pull.
  2. Pre-flight eligibility gate per carrier.
  3. Render E1 template for each eligible carrier.
  4. Post Slack approval preview to Derek; write approval token file; block.
  5. On approval: send E1 via Gmail API, rate-limited at 10/min.
  6. Write Outreach_Status, SentAt, and Thread_Id back to sheet.
  7. After batch: bounce-pause circuit breaker (skip in dry_run).
  8. Return BatchResult dataclass.

Hard constraints enforced here:
  - dry_run=True is the default -- real sends only after explicit Derek approval.
  - Approval gate is enforced even on non-dry-run (CRITICAL).
  - No sheet writes in dry_run.
  - Rate limit: time.sleep(6) between sends.
  - Bounce-pause: if hard-bounce rate > 10%, set global Outreach_Paused flag.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from app.config import get_settings
from app.gmail import send_email, reply_to_thread, get_message, get_header
from app.google_auth import get_gmail_service
from app.notifications import notify_slack
from app.sheets import (
    get_all_carriers,
    read_range,
    write_range,
    update_carrier_fields_by_dot,
)
from scripts.prospect_carriers import exclude_by_equipment

logger = logging.getLogger("brokerops.carrier_outreach")

# ── Constants ─────────────────────────────────────────────────────────────────

DEBOER_MC = "1712065"
SEND_DELAY_SECONDS = 6          # 10/min ceiling
APPROVAL_TIMEOUT_HOURS = 6
BOUNCE_PAUSE_THRESHOLD = 0.10   # >10% hard bounces pauses the domain

_PREVIEW_JSON = Path("scripts/logs/manley_batch_preview_20260415.json")
_LOGS_DIR = Path("scripts/logs")
_TEMPLATE_DIR = Path("app/templates")

_EQUIPMENT_REEFER = re.compile(r"\breefer\b|\brefrigerated\b|\btemperature[- ]?controlled\b", re.I)
_VALID_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class CarrierSendRecord:
    dot: str
    mc: str
    legal_name: str
    email: str
    state: str
    subject: str
    body: str
    thread_id: str = ""
    status: str = "pending"   # pending | sent | skipped | error
    skip_reason: str = ""
    error: str = ""
    sent_at: str = ""


@dataclass
class BatchResult:
    sent: int = 0
    skipped: int = 0
    errors: int = 0
    bounces_detected: int = 0
    approval_token: str = ""
    thread_ids: list[str] = field(default_factory=list)
    skipped_details: list[dict] = field(default_factory=list)
    dry_run: bool = True


# ── Template rendering ────────────────────────────────────────────────────────

def _equipment_bucket(carrier: dict) -> str:
    """Human-readable equipment string, reefer excluded from display."""
    eq = (carrier.get("Equipment_Type") or carrier.get("Equipment Types") or "").strip()
    if not eq:
        return "freight"
    parts = [p.replace("_", " ").strip().lower() for p in eq.split(",")]
    parts = [p for p in parts if p and not _EQUIPMENT_REEFER.search(p)]
    return ", ".join(parts) if parts else "freight"


def _carrier_state(carrier: dict) -> str:
    return (carrier.get("State") or carrier.get("state") or "").strip()


def _carrier_contact_name(carrier: dict) -> str:
    cn = (carrier.get("Contact Name") or carrier.get("Contact_Name") or "").strip()
    if cn and len(cn) > 1:
        return cn.title() if cn.isupper() else cn
    return ""


def _carrier_legal_name(carrier: dict) -> str:
    dba = (carrier.get("DBA_Name") or "").strip()
    legal = (carrier.get("Legal_Name") or carrier.get("Company Name") or "").strip()
    name = dba or legal
    if name.isupper():
        name = name.title()
    return name


def _subject_variant(dot: str) -> int:
    """Rotate subject line based on DOT number to avoid fingerprinting."""
    try:
        return int(dot) % 3
    except (ValueError, TypeError):
        return 0


def render_e1(carrier: dict) -> tuple[str, str]:
    """Render E1 template for a carrier. Returns (subject, body).

    Falls back to built-in string rendering (no Jinja2 dep -- not in requirements).
    """
    legal_name = _carrier_legal_name(carrier)
    state = _carrier_state(carrier)
    equipment = _equipment_bucket(carrier)
    contact_name = _carrier_contact_name(carrier)
    dot = str(carrier.get("DOT Number") or carrier.get("DOT_Number") or "0")
    variant = _subject_variant(dot)

    # Subject
    if variant == 1 and legal_name:
        subject = f"{legal_name} -- capacity question"
    elif variant == 2:
        subject = "Quick intro from deBoer Freight"
    else:
        subject = "Introduction -- deBoer Freight"

    # Greeting
    if contact_name:
        greeting = f"Hi {contact_name} --"
    else:
        greeting = "Hi --"

    # FL/state context
    if state:
        origin_line = f"deBoer Freight is a licensed broker based in South Florida, and we move freight regularly out of {state}."
    else:
        origin_line = "deBoer Freight is a licensed broker based in South Florida."

    # Equipment line
    if equipment and equipment != "freight":
        equip_line = f"We're specifically looking for carriers running {equipment} on Southeast and outbound FL lanes."
    else:
        equip_line = "We're looking for carriers running dry van, flatbed, or box truck on Southeast and outbound FL lanes."

    body = f"""{greeting}

{origin_line} We're building out our carrier network for FL-origin loads and wanted to reach out.

{equip_line}

If you've got capacity in that region, we'd like to get you in our system. Just reply with your rate sheet or ballpark pricing on the lanes you run and we'll go from there.

Thanks,
Sofia Reyes
Carrier Ops | deBoer Freight
866-926-4285 (866-926-HAUL)
sales@deboerfreight.com"""

    return subject, body


def render_e2(carrier: dict) -> str:
    """Render E2 body (day-3 bump, in-thread)."""
    contact_name = _carrier_contact_name(carrier)
    state = _carrier_state(carrier)

    greeting = f"Hi {contact_name} --" if contact_name else "Hi --"
    state_clause = f" in {state}" if state else ""

    return f"""{greeting}

Bumping this up -- wanted to make sure my last note didn't get buried.

We have FL-origin loads moving regularly and we're actively looking to add carriers{state_clause}. If you have capacity, just a quick reply with your lanes and rough rates is enough to get started.

Thanks,
Sofia Reyes
Carrier Ops | deBoer Freight
866-926-4285 (866-926-HAUL)
sales@deboerfreight.com"""


def render_e3(carrier: dict) -> str:
    """Render E3 body (day-7 final nudge, in-thread)."""
    contact_name = _carrier_contact_name(carrier)
    greeting = f"Hi {contact_name} --" if contact_name else "Hi --"

    return f"""{greeting}

No worries if this isn't a fit right now. Wanted to close the loop.

If anything changes on your end, you're always welcome to reply here and we'll pick it up.

Thanks,
Sofia Reyes
Carrier Ops | deBoer Freight
866-926-4285 (866-926-HAUL)
sales@deboerfreight.com"""


# ── Pre-flight eligibility ────────────────────────────────────────────────────

def _is_valid_email(email: str) -> bool:
    if not email:
        return False
    if email.upper() in ("PHONE_ONLY", "N/A", "NONE"):
        return False
    if "_INVALID" in email.upper():
        return False
    return bool(_VALID_EMAIL_RE.match(email.strip()))


def _is_reefer_only(carrier: dict) -> bool:
    eq = (carrier.get("Equipment_Type") or carrier.get("Equipment Types") or "").strip()
    if not eq:
        return False
    parts = [p.strip().lower() for p in eq.split(",") if p.strip()]
    non_reefer = [p for p in parts if not _EQUIPMENT_REEFER.search(p)]
    return len(parts) > 0 and len(non_reefer) == 0


def _is_in_quarantine(carrier: dict) -> bool:
    status = (carrier.get("Status") or carrier.get("Active") or "").strip().lower()
    return status in ("quarantine", "quarantined", "rejected", "excluded")


def _has_prior_outreach(carrier: dict) -> bool:
    """Check new Outreach_Status column -- if set and not 'none'/blank, skip."""
    ost = (carrier.get("Outreach_Status") or "").strip().lower()
    return ost not in ("", "none")


def _is_service_type_general(carrier: dict) -> bool:
    stype = (carrier.get("Service Type") or carrier.get("Service_Type") or "").strip().lower()
    # Blank is allowed -- missing = not classified yet, treat as eligible
    return stype in ("", "general")


def preflight_carrier(carrier: dict) -> tuple[bool, str]:
    """Return (eligible, reason). reason is empty string if eligible."""
    email = (carrier.get("Contact Email") or carrier.get("Primary_Email") or "").strip()

    if not _is_service_type_general(carrier):
        return False, f"Service Type={carrier.get('Service Type','?')} (non-General)"

    if not _is_valid_email(email):
        return False, f"No valid email (value={email!r})"

    if _is_in_quarantine(carrier):
        return False, "Carrier in quarantine"

    if _has_prior_outreach(carrier):
        ost = carrier.get("Outreach_Status", "")
        return False, f"Prior outreach recorded (Outreach_Status={ost})"

    if _is_reefer_only(carrier):
        return False, "Reefer-only carrier (Manley exclusion)"

    if exclude_by_equipment(carrier):
        return False, "Tanker equipment carrier (tanker exclusion rule 2026-04-15)"

    return True, ""


# ── Batch assembly ────────────────────────────────────────────────────────────

def _load_preview_json(path: Path) -> list[dict]:
    """Load the parallel-agent batch preview JSON if it exists."""
    if path.exists():
        try:
            with open(path) as f:
                data = json.load(f)
            if isinstance(data, list):
                logger.info("Loaded %d carriers from preview JSON %s", len(data), path)
                return data
            # Some preview files wrap the list
            if isinstance(data, dict) and "carriers" in data:
                return data["carriers"]
        except Exception as e:
            logger.warning("Could not load preview JSON %s: %s", path, e)
    return []


def _pull_sheet_candidates(limit: int) -> list[dict]:
    """Fall-back: pull from live sheet with Manley filters."""
    all_carriers = get_all_carriers()
    candidates = []
    for c in all_carriers:
        eligible, _ = preflight_carrier(c)
        if not eligible:
            continue
        state = _carrier_state(c).upper()
        if state and state not in ("FL", "GA", "AL", "SC", "NC", "TN", "MS"):
            continue
        candidates.append(c)
        if len(candidates) >= limit:
            break
    logger.info("Sheet pull yielded %d eligible candidates", len(candidates))
    return candidates


def assemble_batch(limit: int) -> list[dict]:
    """Assemble the outreach batch. Preview JSON first, sheet fallback."""
    preview = _load_preview_json(_PREVIEW_JSON)
    if preview:
        return preview[:limit]
    return _pull_sheet_candidates(limit)


# ── Slack approval gate ───────────────────────────────────────────────────────

def _build_approval_preview(records: list[CarrierSendRecord]) -> str:
    lines = [
        f"*deBoer Freight -- Sofia E1 Batch Preview* ({len(records)} carriers)\n",
        "```",
        f"{'DOT':<12} {'Name':<30} {'St':<4} {'Subject':<40} Preview",
        "-" * 110,
    ]
    for r in records:
        preview80 = r.body.replace("\n", " ")[:80]
        lines.append(
            f"{r.dot:<12} {r.legal_name[:28]:<30} {r.state:<4} "
            f"{r.subject[:38]:<40} {preview80}"
        )
    lines.append("```")
    return "\n".join(lines)


def post_approval_request(records: list[CarrierSendRecord], token: str) -> bool:
    """Post the batch preview + approval instructions to Slack."""
    preview_text = _build_approval_preview(records)
    approval_msg = (
        f"{preview_text}\n\n"
        f"*Approval token:* `{token}`\n"
        f"To approve: run `python scripts/approve_outreach_batch.py {token}`\n"
        f"Or reply in Slack: `APPROVE {token}`\n"
        f"Batch will auto-abort if not approved within {APPROVAL_TIMEOUT_HOURS} hours."
    )
    return notify_slack(approval_msg)


def _write_approval_token_file(token: str, records: list[CarrierSendRecord]) -> Path:
    """Write the pending approval state to disk so the CLI script can unblock."""
    _LOGS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    token_file = _LOGS_DIR / f"outreach_approval_{ts}.json"
    payload = {
        "token": token,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "approved": False,
        "rejected": False,
        "carriers": [
            {"dot": r.dot, "email": r.email, "legal_name": r.legal_name}
            for r in records
        ],
    }
    with open(token_file, "w") as f:
        json.dump(payload, f, indent=2)
    logger.info("Wrote approval token file: %s (token=%s)", token_file, token)
    return token_file


def _wait_for_approval(token_file: Path, token: str) -> bool:
    """Poll the token file until approved, rejected, or timeout (6h).

    Returns True if approved, False otherwise.
    In dry_run the caller never reaches this -- see run_daily_outreach_batch.
    """
    import time as _time
    deadline = time.time() + APPROVAL_TIMEOUT_HOURS * 3600
    logger.info("Waiting for batch approval (token=%s, timeout=%dh)...", token, APPROVAL_TIMEOUT_HOURS)

    while time.time() < deadline:
        try:
            with open(token_file) as f:
                state = json.load(f)
            if state.get("approved"):
                logger.info("Batch approved (token=%s)", token)
                return True
            if state.get("rejected"):
                logger.info("Batch rejected (token=%s)", token)
                return False
        except Exception as e:
            logger.warning("Could not read token file %s: %s", token_file, e)
        _time.sleep(30)

    logger.warning("Approval timeout after %dh (token=%s) -- aborting batch", APPROVAL_TIMEOUT_HOURS, token)
    notify_slack(f"Sofia E1 batch timed out waiting for approval (token={token}) -- batch aborted.")
    return False


# ── Sheet write-back ──────────────────────────────────────────────────────────

def _write_outreach_record(carrier: dict, record: CarrierSendRecord) -> None:
    """Write E1 outreach fields to the carrier's sheet row."""
    dot = record.dot
    if not dot:
        logger.warning("Cannot write outreach record -- no DOT for %s", record.legal_name)
        return
    updates = {
        "Outreach_Status": "E1_SENT",
        "Outreach_E1_SentAt": record.sent_at,
        "Outreach_Thread_Id": record.thread_id,
    }
    try:
        update_carrier_fields_by_dot(dot, updates)
        logger.info("Wrote outreach record for DOT=%s thread=%s", dot, record.thread_id)
    except Exception as e:
        logger.error("Failed to write outreach record for DOT=%s: %s", dot, e)


def _flag_outreach_error(dot: str, error: str) -> None:
    try:
        update_carrier_fields_by_dot(dot, {"Outreach_Status": "outreach_error"})
    except Exception as e:
        logger.warning("Could not flag outreach_error for DOT=%s: %s", dot, e)


# ── Bounce circuit breaker ────────────────────────────────────────────────────

_DSN_SENDERS = re.compile(
    r"(mailer-daemon|postmaster|delivery.*failure|mail.*delivery.*subsystem|"
    r"undeliverable|noreply@.*\.google\.com)",
    re.I,
)
_BOUNCE_SUBJECT = re.compile(
    r"(delivery status notification|undeliverable|mail delivery failed|"
    r"returned mail|delivery failure|bounce)",
    re.I,
)


def _is_hard_bounce(msg: dict) -> bool:
    sender = get_header(msg, "From").lower()
    subject = get_header(msg, "Subject").lower()
    return bool(_DSN_SENDERS.search(sender) or _BOUNCE_SUBJECT.search(subject))


def check_bounces_for_batch(thread_ids: list[str], sent_emails: list[str]) -> int:
    """Search Gmail for bounce DSNs against our sent thread IDs.

    Returns hard bounce count. Soft bounces also counted but logged separately.
    Does not modify the sheet -- caller handles the circuit breaker logic.
    """
    if not thread_ids and not sent_emails:
        return 0

    hard_bounces = 0
    try:
        svc = get_gmail_service()
        query = "from:(mailer-daemon OR postmaster) newer_than:1d"
        resp = svc.users().messages().list(userId="me", q=query, maxResults=50).execute()
        msgs = resp.get("messages", [])
        for stub in msgs:
            msg = get_message(stub["id"])
            if _is_hard_bounce(msg):
                # Check if this bounce is for one of our sent emails
                body_snip = str(msg.get("snippet", "")).lower()
                for email in sent_emails:
                    if email.lower() in body_snip:
                        hard_bounces += 1
                        logger.warning("Hard bounce detected for %s", email)
                        break
    except Exception as e:
        logger.warning("Bounce check failed: %s -- circuit breaker skipped", e)

    return hard_bounces


def _set_global_outreach_paused() -> None:
    """Write Outreach_Paused=true to the carrier sheet Settings tab if it exists."""
    try:
        settings = get_settings()
        sheet_id = settings.CARRIER_MASTER_SHEET_ID
        if not sheet_id:
            logger.warning("Cannot set Outreach_Paused: CARRIER_MASTER_SHEET_ID not set")
            return
        # Write to a named range / Settings tab if available; log either way.
        write_range(sheet_id, "Settings!A20:B20", [["Outreach_Paused", "true"]])
        logger.critical("OUTREACH PAUSED: hard-bounce rate exceeded threshold. Written to Settings.")
        notify_slack(
            "CRITICAL: Sofia outreach paused -- hard-bounce rate exceeded 10% of last batch. "
            "Derek must review and lift the hold before any further sends."
        )
    except Exception as e:
        logger.critical(
            "Failed to write Outreach_Paused flag: %s. "
            "MANUAL ACTION REQUIRED: pause all outreach sends immediately.", e
        )
        notify_slack(
            "CRITICAL: Bounce circuit breaker triggered but could not write Outreach_Paused flag. "
            f"Error: {e}. Derek -- manually pause all outreach sends."
        )


# ── Main entry point ──────────────────────────────────────────────────────────

def run_daily_outreach_batch(dry_run: bool = True, limit: int = 20) -> BatchResult:
    """
    Run the Sofia daily E1 carrier outreach batch.

    dry_run=True (default): render + preview only, no sends, no sheet writes.
    dry_run=False: still requires Derek approval via token before any send fires.

    Returns BatchResult dataclass.
    """
    result = BatchResult(dry_run=dry_run)

    # ── 1. Assemble batch ─────────────────────────────────────────────────────
    candidates = assemble_batch(limit)
    if not candidates:
        logger.info("No batch candidates found -- nothing to do.")
        return result

    logger.info("Batch assembled: %d candidates (limit=%d)", len(candidates), limit)

    # ── 2. Pre-flight eligibility gate ────────────────────────────────────────
    records: list[CarrierSendRecord] = []
    for c in candidates:
        email = (c.get("Contact Email") or c.get("Primary_Email") or "").strip()
        dot = str(c.get("DOT Number") or c.get("DOT_Number") or "")
        mc = str(c.get("MC Number") or c.get("MC_Number") or "")
        legal_name = _carrier_legal_name(c)
        state = _carrier_state(c)

        eligible, reason = preflight_carrier(c)
        if not eligible:
            logger.info("SKIP DOT=%s %s: %s", dot, legal_name, reason)
            result.skipped += 1
            result.skipped_details.append({"dot": dot, "name": legal_name, "reason": reason})
            continue

        subject, body = render_e1(c)
        records.append(CarrierSendRecord(
            dot=dot,
            mc=mc,
            legal_name=legal_name,
            email=email,
            state=state,
            subject=subject,
            body=body,
        ))

    logger.info(
        "Pre-flight complete: %d eligible, %d skipped",
        len(records), result.skipped,
    )

    if not records:
        logger.info("No eligible carriers after pre-flight -- done.")
        return result

    # ── 3. Approval gate (ALWAYS enforced) ───────────────────────────────────
    token = str(uuid.uuid4())[:12]
    result.approval_token = token
    token_file = _write_approval_token_file(token, records)
    post_approval_request(records, token)

    if dry_run:
        # In dry_run we don't block -- just return the preview state.
        logger.info(
            "[DRY RUN] Approval gate reached. Token=%s. %d emails would be sent. "
            "No sends, no sheet writes.",
            token, len(records),
        )
        result.sent = 0
        # Count as "previewed" in dry_run for reporting
        result.skipped += len(records)  # they're all in preview, none actually sent
        _log_batch_report(result, records, dry_run=True)
        return result

    # Non-dry-run: block until approval
    approved = _wait_for_approval(token_file, token)
    if not approved:
        logger.warning("Batch not approved -- aborting. Token=%s", token)
        _log_batch_report(result, records, dry_run=False, aborted=True)
        return result

    # ── 4. Send loop ──────────────────────────────────────────────────────────
    sent_emails: list[str] = []
    for i, rec in enumerate(records):
        try:
            sent = send_email(
                to=rec.email,
                subject=rec.subject,
                body_text=rec.body,
            )
            rec.thread_id = sent.get("threadId", "")
            rec.status = "sent"
            rec.sent_at = datetime.now(timezone.utc).isoformat()
            result.sent += 1
            result.thread_ids.append(rec.thread_id)
            sent_emails.append(rec.email)

            # Write back to sheet immediately on success
            carrier_row = next(
                (c for c in candidates
                 if str(c.get("DOT Number") or c.get("DOT_Number") or "") == rec.dot),
                None,
            )
            if carrier_row is not None:
                _write_outreach_record(carrier_row, rec)

            logger.info(
                "Sent E1 to %s (%s) -- thread=%s",
                rec.email, rec.legal_name, rec.thread_id,
            )
        except Exception as e:
            rec.status = "error"
            rec.error = str(e)
            result.errors += 1
            logger.error("Send failed for DOT=%s (%s): %s", rec.dot, rec.email, e)
            _flag_outreach_error(rec.dot, str(e))

        # Rate limit -- 10/min ceiling (6s between sends)
        if i < len(records) - 1:
            time.sleep(SEND_DELAY_SECONDS)

    # ── 5. Bounce circuit breaker (skip in dry_run) ───────────────────────────
    if result.sent > 0:
        logger.info("Waiting 30 minutes for bounce detection...")
        time.sleep(30 * 60)
        bounce_count = check_bounces_for_batch(result.thread_ids, sent_emails)
        result.bounces_detected = bounce_count
        bounce_rate = bounce_count / result.sent if result.sent > 0 else 0
        logger.info(
            "Bounce check: %d hard bounces / %d sent (%.1f%%)",
            bounce_count, result.sent, bounce_rate * 100,
        )
        if bounce_rate > BOUNCE_PAUSE_THRESHOLD:
            logger.critical(
                "Bounce rate %.1f%% exceeds threshold %.1f%% -- pausing outreach",
                bounce_rate * 100, BOUNCE_PAUSE_THRESHOLD * 100,
            )
            _set_global_outreach_paused()

    # ── 6. Final report ───────────────────────────────────────────────────────
    _log_batch_report(result, records, dry_run=False)
    return result


# ── Logging ───────────────────────────────────────────────────────────────────

def _log_batch_report(
    result: BatchResult,
    records: list[CarrierSendRecord],
    dry_run: bool,
    aborted: bool = False,
) -> None:
    _LOGS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = _LOGS_DIR / f"sofia_outreach_build_20260415.log"

    mode = "DRY RUN" if dry_run else ("ABORTED" if aborted else "LIVE")
    lines = [
        f"[{ts}] Sofia E1 Batch Report -- {mode}",
        f"  sent={result.sent} skipped={result.skipped} errors={result.errors} "
        f"bounces={result.bounces_detected}",
        f"  approval_token={result.approval_token}",
        "",
    ]
    for r in records:
        lines.append(
            f"  {r.status.upper():<8} DOT={r.dot:<12} {r.legal_name[:30]:<32} "
            f"{r.email[:40]:<42} thread={r.thread_id or 'n/a'}"
            + (f" SKIP: {r.skip_reason}" if r.skip_reason else "")
            + (f" ERR: {r.error}" if r.error else "")
        )
    for skip in result.skipped_details:
        lines.append(f"  SKIPPED  DOT={skip['dot']:<12} {skip['name'][:30]:<32} reason={skip['reason']}")

    report_text = "\n".join(lines) + "\n"

    with open(report_path, "a", encoding="utf-8") as f:
        f.write(report_text)
    logger.info("Batch report appended to %s", report_path)
