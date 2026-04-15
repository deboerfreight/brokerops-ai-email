"""
Workflow: MDL Vendor Outreach Dispatcher

Approval-gated first-touch email workflow for MDL vendor bidding contacts.
Derek warm-calls a vendor contact, enters the new row in the MDL Vendor
Outreach sheet, and flips col K (Start Outreach) to TRUE. This dispatcher
picks up checked+unsent rows, renders a rigid first-touch template, sends
via Gmail as sales@deboerfreight.com, and stamps the row with thread ID
and timestamp.

NOT an LLM flow. The template renderer is a plain str.format — no Gemini,
no variation, no per-vendor composition. Idempotency is enforced via col H
(Initial Email Sent At): once stamped, the row is done forever.

# DO NOT read column F (Derek's Notes) — it is a private scratchpad
# walled off from agent context by design. See Bolt brief 2026-04-14.
# All sheet reads MUST use the two explicit ranges A:E and G:K. Never A:K,
# never A:F, never F:anything. A pytest assertion enforces this.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from app.config import get_settings
from app.gmail import send_email
from app.google_auth import get_sheets_service

logger = logging.getLogger("brokerops.workflows.mdl_vendor_outreach")

# ── Tanker exclusion (2026-04-15) ──────────────────────────────────────────
# MDL vendor data model has NO equipment column — vendors are building supply
# distributors/manufacturers, not freight carriers. "TANKER" equipment bucket
# exclusion does not apply here.
#
# However, a name-based check is applied as a safety net in case a petroleum
# transport or chemical hauler was inadvertently added to the vendor sheet.
# Uses the same patterns as BrokerOps EXCLUDED_SERVICE_TYPE_PATTERNS (tanker
# subset only). Fuel/propane companies are NOT excluded here — they can be
# valid building supply partners (see feedback_industrial_ag_focus.md).
_MDL_TANKER_NAME_RE = re.compile(
    r"\btanker\b|\btank\s+lines?\b|\bbulk\s+liquid\b"
    r"|\bpetroleum\s+transport\b|\bchemical\s+transport\b",
    re.IGNORECASE,
)


# ── Slack notifications ────────────────────────────────────────────────────
# Wired to real webhook 2026-04-14. notify_slack() in app/notifications.py
# degrades to logger-only when SLACK_WEBHOOK_URL is blank.
from app.notifications import notify_slack as _notify_slack  # noqa: E402


# ── Constants ─────────────────────────────────────────────────────────────

TAB_NAME = "Vendors"

# Two disjoint read ranges — column F is never requested. Load-bearing.
READ_RANGE_AE = f"{TAB_NAME}!A2:E"
READ_RANGE_GK = f"{TAB_NAME}!G2:K"

STATUS_PENDING = "pending"
STATUS_AWAITING_REPLY = "awaiting_reply"
STATUS_SEND_FAILED = "send_failed"

# RFC-5322-ish email regex, deliberately conservative. Pipeline fails loudly
# on anything that doesn't match, letting Derek fix the sheet cell directly.
_EMAIL_RE = re.compile(
    r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
)

_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "mdl_vendor_first_touch.txt"


# ── Template rendering ────────────────────────────────────────────────────

def _load_template() -> tuple[str, str]:
    """Return (subject, body) from the first-touch template file.

    File format: a SUBJECT: line, then a '---' delimiter, then the body.
    """
    raw = _TEMPLATE_PATH.read_text(encoding="utf-8")
    if "---" not in raw:
        raise RuntimeError(f"Template {_TEMPLATE_PATH} missing '---' delimiter")
    head, body = raw.split("---", 1)
    subject = ""
    for line in head.splitlines():
        line = line.strip()
        if line.upper().startswith("SUBJECT:"):
            subject = line.split(":", 1)[1].strip()
            break
    if not subject:
        raise RuntimeError(f"Template {_TEMPLATE_PATH} missing SUBJECT line")
    return subject, body.lstrip("\n")


def _render_first_touch(
    first_name: str, referring_contact_name: str
) -> tuple[str, str]:
    """Return (subject, body) for the rendered first-touch email.

    Rules (locked by brief, per feedback_avoid_ai_tells.md):
      - First name blank -> greeting is literally 'Hello,' (NOT 'Hello team,'
        or 'Hi there,' — those are AI tells)
      - First name present -> 'Hi {first_name},'
      - Referring contact blank -> DROP the "after he spoke with {name}"
        phrase entirely. Sentence becomes "after his call" with no
        substitute like 'your team' or 'your office' (those are AI tells).
      - No LLM, no template-fill defaults.

    Fails loudly if any '{variable}' placeholder is unresolved after
    rendering.
    """
    subject, body = _load_template()

    fn = (first_name or "").strip()
    if fn:
        greeting = f"Hi {fn},"
    else:
        greeting = "Hello,"

    referring = (referring_contact_name or "").strip()
    if referring:
        # Substitute the name into the templated phrase.
        rendered = body.format(
            greeting=greeting,
            referring_contact_name=referring,
        )
    else:
        # Drop the "after he spoke with {name}" phrase entirely.
        # "after he spoke with {referring_contact_name}" -> "after his call"
        body_no_ref = body.replace(
            "after he spoke with {referring_contact_name}",
            "after his call",
        )
        rendered = body_no_ref.format(greeting=greeting)

    # Fail-loud guard against unresolved placeholders
    leftover = re.findall(r"\{[A-Za-z_][A-Za-z0-9_]*\}", rendered)
    if leftover:
        raise ValueError(
            f"Unresolved template placeholders in rendered body: {leftover}"
        )

    return subject, rendered


# ── Sheet helpers ─────────────────────────────────────────────────────────

def _svc():
    return get_sheets_service().spreadsheets()


def _read_sheet(sheet_id: str) -> list[dict]:
    """Read the sheet using TWO explicit disjoint ranges (A:E + G:K).

    Column F (Derek's Notes) is never requested. Returns a list of dicts,
    one per row, with:
      row_number (1-indexed sheet row), vendor_company, first_name,
      last_name, email, referring_contact, date_added, sent_at, status,
      thread_id, start_outreach
    """
    resp = _svc().values().batchGet(
        spreadsheetId=sheet_id,
        ranges=[READ_RANGE_AE, READ_RANGE_GK],
        majorDimension="ROWS",
    ).execute()

    value_ranges = resp.get("valueRanges", [])
    if len(value_ranges) != 2:
        raise RuntimeError(
            f"Expected 2 value ranges, got {len(value_ranges)}"
        )

    ae_rows = value_ranges[0].get("values", [])
    gk_rows = value_ranges[1].get("values", [])

    n = max(len(ae_rows), len(gk_rows))
    rows: list[dict] = []
    for i in range(n):
        ae = ae_rows[i] if i < len(ae_rows) else []
        gk = gk_rows[i] if i < len(gk_rows) else []

        def _cell(arr: list, idx: int) -> str:
            return (arr[idx] if idx < len(arr) else "") or ""

        row = {
            "row_number": i + 2,  # header is row 1
            "vendor_company": _cell(ae, 0).strip(),
            "first_name": _cell(ae, 1).strip(),
            "last_name": _cell(ae, 2).strip(),
            "email": _cell(ae, 3).strip(),
            "referring_contact": _cell(ae, 4).strip(),
            "date_added": _cell(gk, 0).strip(),
            "sent_at": _cell(gk, 1).strip(),
            "status": _cell(gk, 2).strip(),
            "thread_id": _cell(gk, 3).strip(),
            "start_outreach_raw": _cell(gk, 4).strip(),
        }
        row["start_outreach"] = row["start_outreach_raw"].upper() == "TRUE"
        rows.append(row)
    return rows


def _is_valid_email(email: str) -> bool:
    return bool(_EMAIL_RE.match(email or ""))


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Core dispatch loop ────────────────────────────────────────────────────

def run(dry_run: bool = False) -> dict:
    """Execute one dispatcher cycle.

    Returns stats dict: sent, skipped_unchecked, skipped_already_sent,
    validation_failed, send_failed, rows_scanned.
    """
    settings = get_settings()
    sheet_id = settings.MDL_VENDOR_SHEET_ID
    stats = {
        "rows_scanned": 0,
        "sent": 0,
        "skipped_unchecked": 0,
        "skipped_already_sent": 0,
        "validation_failed": 0,
        "send_failed": 0,
    }

    if not sheet_id:
        msg = "MDL_VENDOR_SHEET_ID is not configured — aborting dispatcher"
        logger.error(msg)
        _notify_slack(f"Nina MDL vendor dispatcher failed: {msg}")
        return stats

    try:
        rows = _read_sheet(sheet_id)
    except Exception as e:
        logger.exception("Failed to read MDL vendor sheet")
        _notify_slack(f"Nina MDL vendor dispatcher failed: sheet read error: {e}")
        return stats

    stats["rows_scanned"] = len(rows)
    logger.info("MDL vendor dispatcher: scanned %d row(s)", len(rows))

    batch_data: list[dict] = []

    for row in rows:
        row_num = row["row_number"]
        vendor = row["vendor_company"]
        email = row["email"]

        # Completely blank row — ignore entirely (sheet grid is pre-allocated).
        if not any([vendor, row["first_name"], row["last_name"], email,
                    row["referring_contact"], row["sent_at"],
                    row["start_outreach_raw"]]):
            continue

        # Idempotency: col H (sent_at) is the authoritative flag.
        if row["sent_at"]:
            stats["skipped_already_sent"] += 1
            continue

        if not row["start_outreach"]:
            stats["skipped_unchecked"] += 1
            continue

        if not vendor:
            logger.warning("Row %d has no vendor_company — skipping", row_num)
            stats["validation_failed"] += 1
            batch_data.append({
                "range": f"{TAB_NAME}!H{row_num}:I{row_num}",
                "values": [[f"ERROR: missing vendor_company", STATUS_SEND_FAILED]],
            })
            continue

        # Tanker name exclusion (2026-04-15): MDL sheet has no equipment column,
        # but catch any petroleum-transport or chemical-hauler company that was
        # mistakenly entered as a building supply vendor.
        if _MDL_TANKER_NAME_RE.search(vendor):
            logger.info(
                "Row %d (%s): skipped — tanker/petroleum-transport name pattern "
                "(tanker exclusion rule 2026-04-15)",
                row_num, vendor,
            )
            stats["skipped_unchecked"] += 1
            continue

        if not _is_valid_email(email):
            logger.warning("Row %d (%s) has invalid email %r — marking send_failed",
                           row_num, vendor, email)
            stats["validation_failed"] += 1
            batch_data.append({
                "range": f"{TAB_NAME}!H{row_num}:I{row_num}",
                "values": [[f"ERROR: invalid email {email!r}", STATUS_SEND_FAILED]],
            })
            continue

        # Render template (fail-loud on unresolved placeholders)
        try:
            subject, body = _render_first_touch(
                first_name=row["first_name"],
                referring_contact_name=row["referring_contact"],
            )
        except Exception as e:
            logger.exception("Row %d (%s): template render failed", row_num, vendor)
            stats["validation_failed"] += 1
            batch_data.append({
                "range": f"{TAB_NAME}!H{row_num}:I{row_num}",
                "values": [[f"ERROR: render failed: {e}", STATUS_SEND_FAILED]],
            })
            continue

        if dry_run:
            logger.info("[DRY RUN] Would send to %s (%s) — row %d", email, vendor, row_num)
            stats["sent"] += 1
            continue

        # Send via Gmail (same pattern as app/gmail.py:240)
        try:
            result = send_email(to=email, subject=subject, body_text=body)
            thread_id = result.get("threadId", "")
            sent_at = _now_utc_iso()
            # Stamp H (sent_at), I (status), J (thread_id) in a single write.
            # Column K is left alone; Derek's checkbox stays checked.
            batch_data.append({
                "range": f"{TAB_NAME}!H{row_num}:J{row_num}",
                "values": [[sent_at, STATUS_AWAITING_REPLY, thread_id]],
            })
            stats["sent"] += 1
            logger.info(
                "Sent MDL first-touch to %s (%s) — row %d, thread %s",
                email, vendor, row_num, thread_id,
            )
            _notify_slack(
                f"Nina sent MDL vendor first-touch -> {vendor} ({email}) "
                f"— thread {thread_id}"
            )
        except Exception as e:
            logger.exception("Row %d (%s): send failed", row_num, vendor)
            stats["send_failed"] += 1
            batch_data.append({
                "range": f"{TAB_NAME}!I{row_num}",
                "values": [[STATUS_SEND_FAILED]],
            })

    # Flush all sheet writes in a single batchUpdate
    if batch_data and not dry_run:
        try:
            _svc().values().batchUpdate(
                spreadsheetId=sheet_id,
                body={
                    "valueInputOption": "USER_ENTERED",
                    "data": batch_data,
                },
            ).execute()
            logger.info("Applied %d batched sheet update(s)", len(batch_data))
        except Exception as e:
            logger.exception("batchUpdate failed")
            _notify_slack(f"Nina MDL vendor dispatcher failed: batchUpdate error: {e}")

    logger.info(
        "MDL vendor dispatcher cycle complete: %d sent, %d skipped "
        "(already sent), %d skipped (unchecked), %d validation failed, "
        "%d send failed",
        stats["sent"], stats["skipped_already_sent"], stats["skipped_unchecked"],
        stats["validation_failed"], stats["send_failed"],
    )
    return stats
