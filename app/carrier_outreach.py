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
# exclude_by_equipment was previously imported from scripts.prospect_carriers,
# but scripts/ is not in the Cloud Run image. Inlined here so app/ is fully
# self-contained. Keep in sync with scripts/prospect_carriers.py.
def exclude_by_equipment(carrier: dict) -> bool:
    """Return True if carrier should be excluded due to TANKER equipment bucket."""
    eq_raw = (
        carrier.get("Equipment_Type")
        or carrier.get("Equipment Types")
        or carrier.get("equipment_types")
        or carrier.get("Equipment_Types")
        or ""
    )
    buckets = [b.strip().upper() for b in str(eq_raw).split(",") if b.strip()]
    return "TANKER" in buckets

# Mobile approval flow imports — loaded lazily to avoid import errors
# when running outside Cloud Run (e.g. local scripts).
def _get_signed_url_helpers():
    from app.signed_urls import sign_token
    from app.pending_batch_store import store_pending_batch, mark_batch_used
    return sign_token, store_pending_batch, mark_batch_used

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
    # Cloud Run mobile approval flow fields
    status: str = "complete"        # "pending_approval" | "complete" | "dry_run" | "aborted"
    batch_id: str = ""              # UUID4 batch identifier (set when status=pending_approval)
    gcs_uri: str = ""               # gs:// URI of the pending batch JSON
    # Populated in dry_run mode — rendered carrier records for caller display
    records: list[dict] = field(default_factory=list)


# ── Template rendering ────────────────────────────────────────────────────────

def _equipment_bucket(carrier: dict) -> str:
    """Human-readable equipment string, reefer excluded from display."""
    eq = (carrier.get("Equipment_Type") or carrier.get("Equipment Types") or "").strip()
    if not eq:
        return "freight"
    parts = [p.replace("_", " ").strip().lower() for p in eq.split(",")]
    parts = [p for p in parts if p and not _EQUIPMENT_REEFER.search(p)]
    return ", ".join(parts) if parts else "freight"


def _equipment_bucket_parts(carrier: dict) -> list[str]:
    """Return equipment parts list (reefer excluded). Used by _format_equipment_list."""
    eq = (carrier.get("Equipment_Type") or carrier.get("Equipment Types") or "").strip()
    if not eq:
        return []
    parts = [p.replace("_", " ").strip().lower() for p in eq.split(",")]
    return [p for p in parts if p and not _EQUIPMENT_REEFER.search(p)]


def _carrier_state(carrier: dict) -> str:
    return (carrier.get("State") or carrier.get("state") or "").strip()


_ROLE_ACCOUNTS = frozenset({
    "info", "ops", "dispatch", "contact", "sales", "support", "admin",
    "noreply", "no-reply", "help", "office", "team", "customer", "service",
})


def _extract_name_from_email(email: str) -> str:
    """Extract a human first name from the local part of an email address.

    Rules (Fix 4, 2026-04-15):
      1. Strip digits and trailing dots from the local-part.
      2. If the result is 3+ alphabetic characters AND not a role-account keyword,
         title-case it and return as the name.
      3. Otherwise return "" (falls back to "Hi," in the greeting).

    Examples:
      mike@hotshotdriving.com      -> "Mike"
      tere@pgttransport.com        -> "Tere"
      lennox@soundmedia1.com       -> "Lennox"
      ops@acme.com                 -> ""  (role account)
      jorgeltrindade92@gmail.com   -> ""  (digits dominate)
      supernicetransport@gmail.com -> ""  (not a name)
    """
    if not email or "@" not in email:
        return ""
    local = email.split("@", 1)[0]
    # Strip digits and trailing dots
    stripped = re.sub(r"[^a-zA-Z]", "", local)
    if len(stripped) < 3:
        return ""
    if stripped.lower() in _ROLE_ACCOUNTS:
        return ""
    # Must be pure alpha after stripping — if the original had so many digits
    # that < 3 alpha chars remain, bail (handled above via len check)
    return stripped.title()


def _carrier_contact_name(carrier: dict) -> str:
    """Return the best available contact first name for use in email greetings.

    Prefers the sheet Contact Name column; falls back to email local-part extraction.
    Returns empty string when no clean name can be found (caller uses "Hi,").
    """
    cn = (carrier.get("Contact Name") or carrier.get("Contact_Name") or "").strip()
    if cn and len(cn) > 1:
        return cn  # preserve original casing from sheet

    # Fallback: try to extract from email local-part
    email = (carrier.get("Contact Email") or carrier.get("Primary_Email") or "").strip()
    return _extract_name_from_email(email)


STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
}


def _spell_state(abbr: str) -> str:
    """Return full state name for a two-letter abbreviation, or the original string."""
    return STATE_NAMES.get(abbr.upper(), abbr)


def _format_equipment_list(buckets: list[str]) -> str:
    """Return a natural-language equipment list with pluralization.

    Examples:
      ["dry van"]              -> "dry vans"
      ["dry van", "flatbed"]   -> "dry vans and flatbeds"
      ["dry van", "flatbed", "box truck"] -> "dry vans, flatbeds, and box trucks"
    """
    _plural = {
        "dry van": "dry vans",
        "flatbed": "flatbeds",
        "box truck": "box trucks",
        "reefer": "reefers",
    }
    pluralized = [_plural.get(b.lower().strip(), b.lower().strip() + "s") for b in buckets]
    if len(pluralized) == 0:
        return ""
    if len(pluralized) == 1:
        return pluralized[0]
    if len(pluralized) == 2:
        return f"{pluralized[0]} and {pluralized[1]}"
    # Oxford comma for 3+
    return ", ".join(pluralized[:-1]) + f", and {pluralized[-1]}"


def _carrier_legal_name(carrier: dict) -> str:
    dba = (carrier.get("DBA_Name") or "").strip()
    legal = (carrier.get("Legal_Name") or carrier.get("Company Name") or "").strip()
    name = dba or legal
    # Do NOT title-case — preserve original casing from sheet
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
    state_full = _spell_state(state) if state else ""
    equip_parts = _equipment_bucket_parts(carrier)
    contact_name = _carrier_contact_name(carrier)
    dot = str(carrier.get("DOT Number") or carrier.get("DOT_Number") or "0")
    variant = _subject_variant(dot)

    # Subject — preserve legal name casing exactly; use colon separator
    if variant == 1 and legal_name:
        subject = f"{legal_name}: capacity question"
    elif variant == 2:
        subject = "Quick intro from deBoer Freight"
    else:
        subject = "Introduction -- deBoer Freight"

    # Greeting — comma, not dash
    if contact_name:
        greeting = f"Hi {contact_name},"
    else:
        greeting = "Hi,"

    # Opening — Derek self-intro, specialty focus
    origin_line = (
        "My name is Derek deBoer, owner/operator of deBoer Freight, "
        "a licensed freight brokerage based in Key West, Florida. "
        "We specialize in moving commodities and building materials in and out of Florida, "
        "and we're expanding our carrier network."
    )

    # Ask — equipment types, lanes, and rate expectations
    ask_line = (
        "I'd like to learn more about your operation. Could you share:"
    )

    body = f"""{greeting}

{origin_line}

{ask_line}

  1. The equipment types you run (dry van, flatbed, reefer, box truck, hot shot, etc.)
  2. The primary lanes you cover
  3. Rate expectations on those lanes

If you've got capacity, we'd like to get you set up in our network.

Thanks,
Derek deBoer
www.deboerfreight.com
(305) 767-3480
MC 1712065"""

    return subject, body


def render_e2(carrier: dict) -> str:
    """Render E2 body (day-3 bump, in-thread)."""
    contact_name = _carrier_contact_name(carrier)
    state = _carrier_state(carrier)
    state_full = _spell_state(state) if state else ""

    greeting = f"Hi {contact_name}," if contact_name else "Hi,"
    state_clause = f" in {state_full}" if state_full else ""

    return f"""{greeting}

Bumping this up -- wanted to make sure my last note didn't get buried.

We have Florida-origin loads moving regularly and we're actively looking to add carriers{state_clause}. If you have capacity, just a quick reply with your lanes and rough rates is enough to get started.

Thanks,
Derek deBoer
www.deboerfreight.com
(305) 767-3480
MC 1712065"""


def render_e3(carrier: dict) -> str:
    """Render E3 body (day-7 final nudge, in-thread)."""
    contact_name = _carrier_contact_name(carrier)
    greeting = f"Hi {contact_name}," if contact_name else "Hi,"

    return f"""{greeting}

No worries if this isn't a fit right now. Wanted to close the loop.

If anything changes on your end, you're always welcome to reply here and we'll pick it up.

Thanks,
Derek deBoer
www.deboerfreight.com
(305) 767-3480
MC 1712065"""


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
    # Fix 3: Outreach_Exclude check — runs before every other gate.
    exclude_reason = (carrier.get("Outreach_Exclude") or "").strip()
    if exclude_reason:
        return False, f"excluded: {exclude_reason}"

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


_PERSONAL_DOMAINS = frozenset({
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
    "comcast.net", "icloud.com", "me.com", "live.com", "msn.com", "protonmail.com",
})

_GEO_PRIORITY = {
    "FL": 0,
    "GA": 1, "AL": 1, "SC": 1, "NC": 1,
    "TN": 2, "MS": 2, "KY": 2,
}


def _is_personal_domain(email: str) -> bool:
    """Return True if the email's domain is a known personal/consumer provider."""
    if not email or "@" not in email:
        return False
    domain = email.split("@", 1)[1].lower()
    return domain in _PERSONAL_DOMAINS


def _carrier_power_units(carrier: dict) -> int:
    raw = carrier.get("Power_Units") or carrier.get("Fleet Size") or "0"
    try:
        return int(float(str(raw)))
    except (ValueError, TypeError):
        return 0


def _carrier_equipment_bucket_count(carrier: dict) -> int:
    eq = (carrier.get("Equipment_Type") or carrier.get("Equipment Types") or "").strip()
    if not eq:
        return 0
    return len([p for p in eq.split(",") if p.strip()])


def _carrier_sort_key(carrier: dict) -> tuple:
    """Lower tuple value = ranked higher (sorted ascending).

    Tiebreaker order:
      1. Business-domain email (0) vs personal-domain (1)
      2. Power units 3-20 sweet spot (0) vs outside (1)
      3. Multi-equipment (0) vs single (1)
      4. DOT number ascending (stable, deterministic)
    """
    email = (carrier.get("Contact Email") or carrier.get("Primary_Email") or "").strip()
    personal = 1 if _is_personal_domain(email) else 0
    units = _carrier_power_units(carrier)
    sweet = 0 if 3 <= units <= 20 else 1
    multi = 0 if _carrier_equipment_bucket_count(carrier) >= 2 else 1
    dot_raw = carrier.get("DOT Number") or carrier.get("DOT_Number") or "999999999"
    try:
        dot_int = int(dot_raw)
    except (ValueError, TypeError):
        dot_int = 999999999
    return (personal, sweet, multi, dot_int)


def _pull_sheet_candidates(limit: int) -> list[dict]:
    """Pull from live sheet with Manley filters and ranked sort.

    Ranking order (Fix 1, 2026-04-15):
      Hard gates (preflight_carrier):
        - Outreach_Exclude empty
        - Outreach_Status blank/none
        - pass_basic vetting
        - Service Type == General
        - valid email (not blank/PHONE_ONLY/_INVALID)
        - not reefer-only, not tanker

      Geo filter: FL, GA, AL, SC, NC, TN, MS, KY only.

      Sort tiebreakers (ascending):
        1. Business-domain email before personal-domain (personal-domain NOT excluded)
        2. Power units 3-20 sweet spot
        3. Multi-equipment carriers
        4. DOT number ascending (deterministic)
    """
    all_carriers = get_all_carriers()
    eligible = []
    for c in all_carriers:
        ok, _ = preflight_carrier(c)
        if not ok:
            continue
        state = _carrier_state(c).upper()
        if state and state not in ("FL", "GA", "AL", "SC", "NC", "TN", "MS", "KY"):
            continue
        eligible.append(c)

    # Sort by tiebreakers (personal-domain last, sweet-spot units first, etc.)
    eligible.sort(key=_carrier_sort_key)

    candidates = eligible[:limit]
    logger.info(
        "Sheet pull yielded %d eligible carriers; returning top %d after ranking",
        len(eligible), len(candidates),
    )
    return candidates


def assemble_batch(limit: int, from_file: bool = False) -> list[dict]:
    """Assemble the outreach batch.

    Default (Cloud Run scheduled path): pull fresh from live sheet.
    Pass from_file=True to use the local preview JSON for manual review runs.
    """
    if from_file:
        preview = _load_preview_json(_PREVIEW_JSON)
        if preview:
            logger.info("assemble_batch: using preview JSON (%d rows, capped at %d)", len(preview), limit)
            return preview[:limit]
        logger.warning("assemble_batch: from_file=True but preview JSON not found -- falling back to sheet")
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


# ── Mobile approval: post Slack DM with signed tap-links ─────────────────────

def post_mobile_approval_request(
    records: list[CarrierSendRecord],
    approve_url: str,
    batch_id: str,
    ttl_hours: int = 6,
) -> bool:
    """Post a phone-readable Slack DM with carrier preview + single tap-to-review link.

    The approve_url points to GET /approve which is the preview page — Derek
    reviews the batch there and taps Confirm or Cancel. No separate cancel URL.
    """
    lines = [f"\U0001f4ec *Manley batch ready \u2014 {len(records)} carriers*\n"]
    for i, r in enumerate(records, 1):
        name_short = r.legal_name[:36] if len(r.legal_name) > 36 else r.legal_name
        lines.append(f"{i}. {name_short} (DOT {r.dot}) \u2014 {r.state}")
    lines.append("")
    lines.append(f"\U0001f517 <{approve_url}|Tap to review and action>")
    lines.append("")
    lines.append(f"Expires in {ttl_hours} hours.")
    msg = "\n".join(lines)
    return notify_slack(msg)


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


# ── Post-approval send loop (Cloud Run mobile approval flow) ──────────────────

def run_approved_batch_send(batch: dict) -> dict:
    """
    Execute the send loop for an already-approved batch.

    Called by the /approve route AFTER mark_batch_used() has fired.
    This is the post-approval send loop — it does NOT assemble, gate, or
    write approval state. It only iterates carriers, sends, and returns stats.

    Args:
        batch: Deserialized pending batch dict from GCS. Must contain:
               - "records": list of carrier record dicts
               - "candidates": list of original carrier sheet rows (for sheet write-back)

    Returns:
        dict with keys: sent (int), bounced (int), errored (int)
    """
    records_raw = batch.get("records", [])
    candidates = batch.get("candidates", [])
    sent_count = 0
    bounce_count = 0
    error_count = 0
    sent_emails: list[str] = []
    thread_ids: list[str] = []

    records = [
        CarrierSendRecord(
            dot=r.get("dot", ""),
            mc=r.get("mc", ""),
            legal_name=r.get("legal_name", ""),
            email=r.get("email", ""),
            state=r.get("state", ""),
            subject=r.get("subject", ""),
            body=r.get("body", ""),
        )
        for r in records_raw
    ]

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
            sent_count += 1
            thread_ids.append(rec.thread_id)
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
                "Sent E1 to %s (%s) — thread=%s",
                rec.email, rec.legal_name, rec.thread_id,
            )
        except Exception as e:
            rec.status = "error"
            rec.error = str(e)
            error_count += 1
            logger.error("Send failed for DOT=%s (%s): %s", rec.dot, rec.email, e)
            _flag_outreach_error(rec.dot, str(e))

        # Rate limit: 6s between sends (10/min ceiling)
        if i < len(records) - 1:
            time.sleep(SEND_DELAY_SECONDS)

    # Bounce circuit breaker — check after sends
    if sent_count > 0:
        detected = check_bounces_for_batch(thread_ids, sent_emails)
        bounce_count = detected
        bounce_rate = detected / sent_count if sent_count > 0 else 0
        logger.info(
            "Bounce check: %d hard bounces / %d sent (%.1f%%)",
            detected, sent_count, bounce_rate * 100,
        )
        if bounce_rate > BOUNCE_PAUSE_THRESHOLD:
            logger.critical(
                "Bounce rate %.1f%% exceeds threshold — pausing outreach",
                bounce_rate * 100,
            )
            _set_global_outreach_paused()

    logger.info(
        "run_approved_batch_send complete: sent=%d bounced=%d errored=%d",
        sent_count, bounce_count, error_count,
    )
    return {"sent": sent_count, "bounced": bounce_count, "errored": error_count}


# ── Main entry point ──────────────────────────────────────────────────────────

def run_daily_outreach_batch(dry_run: bool = True, limit: int = 20) -> BatchResult:
    """
    Sofia daily E1 carrier outreach batch — Cloud Run non-blocking version.

    Flow:
      1. Assemble batch from live sheet (Manley filters).
      2. Pre-flight eligibility gate per carrier.
      3. Generate batch_id (UUID4) and store batch JSON in GCS.
      4. Generate signed approve + cancel URLs (HMAC-SHA256, 6h TTL).
      5. Post Slack DM with 20-carrier preview + tap-links.
      6. Return immediately with status="pending_approval".

    The actual send loop fires in /approve (app/task_routes.py) when
    Derek taps the approve link on his phone.

    dry_run=True: render + preview only; no GCS write, no Slack, no sends.
    dry_run=False: full Cloud Run flow (assemble → GCS → Slack → return 200).

    Returns BatchResult dataclass.
    """
    result = BatchResult(dry_run=dry_run)

    # ── 1. Assemble batch ─────────────────────────────────────────────────────
    candidates = assemble_batch(limit)
    if not candidates:
        logger.info("No batch candidates found -- nothing to do.")
        result.status = "complete"
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
        result.status = "complete"
        return result

    # ── 3. Dry-run short-circuit ──────────────────────────────────────────────
    if dry_run:
        token = str(uuid.uuid4())[:12]
        result.approval_token = token
        result.sent = 0
        result.skipped += len(records)
        result.status = "dry_run"
        # Expose rendered records so callers can display them without side effects
        result.records = [
            {
                "dot": r.dot,
                "legal_name": r.legal_name,
                "email": r.email,
                "subject": r.subject,
                "body": r.body,
            }
            for r in records
        ]
        logger.info(
            "[DRY RUN] %d emails would be assembled. Token=%s. "
            "No GCS write, no Slack, no sends.",
            len(records), token,
        )
        _log_batch_report(result, records, dry_run=True)
        return result

    # ── 4. Generate batch_id and store in GCS ────────────────────────────────
    batch_id = str(uuid.uuid4())
    result.approval_token = batch_id
    result.batch_id = batch_id

    sign_token_fn, store_pending_batch, _ = _get_signed_url_helpers()

    # Build serializable batch payload (candidates included for sheet write-back)
    batch_payload = {
        "batch_id": batch_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "used": False,
        "records": [
            {
                "dot": r.dot,
                "mc": r.mc,
                "legal_name": r.legal_name,
                "email": r.email,
                "state": r.state,
                "subject": r.subject,
                "body": r.body,
            }
            for r in records
        ],
        "candidates": candidates,
    }

    try:
        gcs_uri = store_pending_batch(batch_id, batch_payload)
        result.gcs_uri = gcs_uri
        logger.info("Batch stored in GCS: batch_id=%s uri=%s", batch_id, gcs_uri)
    except Exception as e:
        logger.exception("Failed to store pending batch in GCS: %s", e)
        notify_slack(
            f":rotating_light: Sofia batch assembly failed — could not write to GCS.\n"
            f"Error: {e}\nbatch_id={batch_id}"
        )
        result.status = "aborted"
        return result

    # ── 5. Generate signed approve + cancel URLs ──────────────────────────────
    signing_secret = os.environ.get("APPROVAL_SIGNING_SECRET", "")
    if not signing_secret:
        logger.error("APPROVAL_SIGNING_SECRET not set — cannot generate signed URLs")
        notify_slack(
            ":rotating_light: Sofia batch assembly failed — APPROVAL_SIGNING_SECRET not configured."
        )
        result.status = "aborted"
        return result

    signed = sign_token_fn({"batch_id": batch_id}, signing_secret, ttl_seconds=APPROVAL_TIMEOUT_HOURS * 3600)
    token_str = signed["token"]
    sig = signed["sig"]
    exp = signed["exp"]

    service_url = os.environ.get("SERVICE_URL", "https://brokerops-ai-service-url")
    approve_url = f"{service_url}/approve?token={token_str}&sig={sig}&exp={exp}"

    # ── 6. Post Slack DM with preview + single tap-link ───────────────────────
    try:
        post_mobile_approval_request(
            records,
            approve_url=approve_url,
            batch_id=batch_id,
            ttl_hours=APPROVAL_TIMEOUT_HOURS,
        )
        logger.info("Slack approval DM posted for batch_id=%s", batch_id)
    except Exception as e:
        logger.exception("Failed to post Slack approval DM: %s", e)
        # Non-fatal — batch is in GCS, Derek can retrieve manually

    # ── 7. Return immediately (Cloud Run does not block) ──────────────────────
    result.status = "pending_approval"
    _log_batch_report(result, records, dry_run=False)
    logger.info(
        "run_daily_outreach_batch complete: status=pending_approval batch_id=%s "
        "%d carriers pending Derek approval",
        batch_id, len(records),
    )
    return result


# ── Logging ───────────────────────────────────────────────────────────────────

def _log_batch_report(
    result: BatchResult,
    records: list[CarrierSendRecord],
    dry_run: bool,
    aborted: bool = False,
) -> None:
    """Emit the batch report to structured logs.

    Cloud Run-safe: writes only via the stdlib logger (which Cloud Logging
    captures). No local file writes — scripts/ does not exist inside the
    container image and is not writable by the non-root app user anyway.
    """
    mode = "DRY RUN" if dry_run else ("ABORTED" if aborted else "LIVE")
    logger.info(
        "Sofia E1 Batch Report [%s]: sent=%d skipped=%d errors=%d bounces=%d token=%s",
        mode, result.sent, result.skipped, result.errors,
        result.bounces_detected, result.approval_token,
    )
    for r in records:
        logger.info(
            "  %s DOT=%s %s %s thread=%s%s%s",
            r.status.upper(),
            r.dot,
            r.legal_name[:40],
            r.email[:60],
            r.thread_id or "n/a",
            f" SKIP: {r.skip_reason}" if r.skip_reason else "",
            f" ERR: {r.error}" if r.error else "",
        )
    for skip in result.skipped_details:
        logger.info(
            "  SKIPPED DOT=%s %s reason=%s",
            skip.get("dot"), skip.get("name", "")[:40], skip.get("reason"),
        )
