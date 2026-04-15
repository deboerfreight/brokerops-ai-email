"""
Workflow: Carrier Outreach Sender

Sends proactive outreach emails to prospective carriers on behalf of
Sofia Reyes (Carrier Relations, deBoer Freight).

Sequence:
  1. NEW carriers with a valid email -> send initial outreach, set OUTREACH_SENT
  2. OUTREACH_SENT carriers aged 3+ days  -> send follow-up #1 in-thread, set FOLLOW_UP_1
  3. FOLLOW_UP_1 carriers aged 4+ days    -> send follow-up #2 in-thread, set FOLLOW_UP_2
  4. FOLLOW_UP_2 carriers aged 7+ days    -> mark UNRESPONSIVE

PHONE_ONLY carriers are logged and skipped (no email attempt).
Rate-limited: configurable delay between sends to avoid Gmail throttling.
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Optional

from app.config import get_settings
from app.gmail import send_email, reply_to_thread, search_messages
from app.sheets import (
    get_all_carriers,
    get_broker_settings,
    is_carrier_vetted,
    update_carrier_fields_by_key,
)

logger = logging.getLogger("brokerops.workflows.carrier_outreach")

# ── Configuration ──────────────────────────────────────────────────────────

SEND_DELAY_SECONDS = 10          # pause between individual emails
FOLLOW_UP_1_DELAY_DAYS = 3      # days after initial outreach
FOLLOW_UP_2_DELAY_DAYS = 4      # days after follow-up #1
UNRESPONSIVE_DELAY_DAYS = 7     # days after follow-up #2
BATCH_LIMIT = 20                 # max emails per run (safety cap)

DEBOER_MC = "1712065"
SOFIA_PHONE = "305-767-3480"


# ── Email body builders ───────────────────────────────────────────────────

def _get_broker_info() -> dict:
    """Load broker settings, with safe defaults."""
    try:
        broker = get_broker_settings()
    except Exception:
        broker = {}
    return {
        "company": broker.get("Broker_Company_Name", "deBoer Freight"),
        "phone": broker.get("Broker_Company_Phone", SOFIA_PHONE),
    }


_ROLE_ACCOUNTS = frozenset({
    "info", "ops", "dispatch", "contact", "sales", "support", "admin",
    "hello", "help", "mail", "office", "team", "billing", "accounting",
    "service", "services", "freight", "loads", "bookings",
})


def _carrier_contact_name(carrier: dict) -> Optional[str]:
    """Extract a first name from the carrier's contact email local-part.

    Rules:
      - Extract local-part (before @).
      - If it's a role account (info, ops, dispatch, etc.) → return None.
      - If length < 3 → return None.
      - Otherwise title-case and return.
    """
    email = (carrier.get("Contact Email") or carrier.get("Primary_Email") or "").strip().lower()
    if not email or "@" not in email:
        return None
    local = email.split("@")[0]
    # Strip common separators — take first segment (e.g. "mike.smith" → "mike")
    for sep in (".", "_", "+", "-"):
        if sep in local:
            local = local.split(sep)[0]
    if not local or len(local) < 3:
        return None
    if local in _ROLE_ACCOUNTS:
        return None
    return local.title()


def _carrier_display_name(carrier: dict) -> Optional[str]:
    """Return the best name to use when greeting a carrier, or None if unknown.

    DBA_Name is typically proper case and used as-is. Legal_Name from FMCSA
    comes back ALL CAPS, which produces shouted greetings — title-case it.
    Returns None when no usable name exists so the template can degrade to
    a plain "Hello," rather than "Hello Carrier,".
    """
    dba = (carrier.get("DBA_Name") or "").strip()
    if dba:
        return dba
    legal = (carrier.get("Legal_Name") or "").strip()
    if legal:
        return legal.title()
    return None


def _greeting(name: Optional[str]) -> str:
    """Render the greeting line. Uses contact first name when available."""
    if name:
        return f"Hi {name},"
    return "Hi,"


def _normalize_legal_name_acronym(name: str) -> str:
    """Uppercase the first word of a legal name if it looks like an acronym.

    Rule: first word is 2-4 characters AND contains no vowels (a/e/i/o/u/y,
    case-insensitive) → uppercase it. Otherwise leave as-is.

    Examples:
      Pgt Transport INC  → PGT Transport INC
      Rdh Trucking INC   → RDH Trucking INC
      Cts Logistics LLC  → CTS Logistics LLC
      Apex Trucking LLC  → Apex Trucking LLC  (has vowel 'e')
      The Hilton Group   → The Hilton Group   (has vowel 'e')
    """
    if not name:
        return name
    words = name.split()
    if not words:
        return name
    first = words[0]
    if 2 <= len(first) <= 4 and not re.search(r"[aeiouy]", first, re.IGNORECASE):
        words[0] = first.upper()
    return " ".join(words)


def _carrier_region(carrier: dict) -> str:
    """Best-effort region description from preferred lanes or notes.

    Returns an empty string when unknown — callers must omit the region
    phrase entirely rather than fabricate one.
    """
    lanes = (carrier.get("Preferred_Lanes") or "").strip()
    if lanes:
        return lanes
    return ""


def _carrier_equipment(carrier: dict) -> str:
    """Return equipment string, sanitized for natural reading.

    Replaces underscores with spaces and lowercases. If the source is empty,
    returns plain "freight" rather than fabricating a type list.
    """
    eq = (carrier.get("Equipment_Type") or "").strip()
    if not eq:
        return "freight"
    parts = [t.replace("_", " ").strip().lower() for t in eq.split(",")]
    parts = [p for p in parts if p]
    if not parts:
        return "freight"
    return ", ".join(parts)


def build_initial_outreach(carrier: dict) -> tuple[str, str]:
    """Return (subject, body) for the initial outreach email."""
    info = _get_broker_info()
    contact_name = _carrier_contact_name(carrier)
    name = _carrier_display_name(carrier)
    region = _carrier_region(carrier)
    equipment = _carrier_equipment(carrier)
    greeting = _greeting(contact_name)
    _ = region  # region unused in the opener; follow-ups handle region-specific phrasing

    subject = f"Freight opportunities - deBoer Freight (MC#{DEBOER_MC})"

    opener = (
        f"I'm Sofia at deBoer Freight (MC#{DEBOER_MC}). We move {equipment} "
        f"out of South FL and are adding carriers."
    )

    body = f"""{greeting}

{opener}

What we offer:
  - Consistent freight, not just one-off loads
  - Quick pay options available
  - Straightforward booking with no runaround

If you are interested, please reply with your preferred lanes and equipment types, and we will start matching you with available loads right away.

You are welcome to verify our authority at https://safer.fmcsa.dot.gov using MC#{DEBOER_MC}.

Thank you,
Sofia Reyes
Carrier Relations
{info['company']}
{info['phone']}
"""
    return subject, body


def build_followup_1(carrier: dict) -> str:
    """Return body text for follow-up #1 (sent in-thread)."""
    info = _get_broker_info()
    contact_name = _carrier_contact_name(carrier)
    region = _carrier_region(carrier)
    greeting = _greeting(contact_name)

    if region:
        lede = (
            f"I wanted to follow up on my earlier message. We have freight moving "
            f"through the {region} area regularly and would like to make sure you "
            f"are on our carrier list."
        )
    else:
        lede = (
            "I wanted to follow up on my earlier message. We have freight moving "
            "regularly and would like to make sure you are on our carrier list."
        )

    return f"""{greeting}

{lede}

If you could reply with your preferred lanes and equipment types, we can start getting loads in front of you.

Thank you,
Sofia Reyes
Carrier Relations
{info['company']}
{info['phone']}
"""


def build_followup_2(carrier: dict) -> str:
    """Return body text for the final follow-up (sent in-thread)."""
    info = _get_broker_info()
    contact_name = _carrier_contact_name(carrier)
    region = _carrier_region(carrier)
    greeting = _greeting(contact_name)

    if region:
        closer = (
            f"This is my last note on this thread. If you are ever looking for "
            f"freight in the {region} area, we are here and ready to work together. "
            f"Simply reply to this email any time and we will get you set up."
        )
    else:
        closer = (
            "This is my last note on this thread. If you are ever looking for "
            "freight, we are here and ready to work together. Simply reply to "
            "this email any time and we will get you set up."
        )

    return f"""{greeting}

{closer}

Thank you,
Sofia Reyes
Carrier Relations
{info['company']}
{info['phone']}
"""


# ── Helpers ────────────────────────────────────────────────────────────────

def _parse_date(date_str: str) -> Optional[date]:
    """Parse an ISO date string, returning None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _days_since(date_str: str) -> Optional[int]:
    """Return number of days since date_str, or None if unparseable."""
    d = _parse_date(date_str)
    if d is None:
        return None
    return (date.today() - d).days


def _has_valid_email(carrier: dict) -> bool:
    """Check that the carrier has a real email address (not PHONE_ONLY or blank)."""
    email = (carrier.get("Primary_Email") or "").strip()
    if not email:
        return False
    if email.upper() == "PHONE_ONLY":
        return False
    if "@" not in email:
        return False
    return True


def _is_phone_only(carrier: dict) -> bool:
    """Check if carrier is flagged as phone-only."""
    email = (carrier.get("Primary_Email") or "").strip().upper()
    method = (carrier.get("Outreach_Method") or "").strip().upper()
    return email == "PHONE_ONLY" or method == "PHONE"


def _find_outreach_thread(carrier_email: str) -> Optional[str]:
    """Search Gmail for an existing outreach thread to this carrier.

    Looks for sent messages with our outreach subject to their address.
    Returns the thread ID if found, None otherwise.
    """
    try:
        query = f"to:{carrier_email} subject:\"Freight opportunities\" from:me"
        results = search_messages("SENT", query=query)
        if not results:
            # Try without label — search all mail
            from app.google_auth import get_gmail_service
            svc = get_gmail_service()
            resp = svc.users().messages().list(
                userId="me",
                q=f"to:{carrier_email} subject:\"Freight opportunities\" from:me",
                maxResults=1,
            ).execute()
            msgs = resp.get("messages", [])
            if msgs:
                return msgs[0].get("threadId")
            return None
        return results[0].get("threadId")
    except Exception as e:
        logger.warning("Could not find outreach thread for %s: %s", carrier_email, e)
        return None


def _has_any_prior_gmail_thread(carrier_email: str) -> bool:
    """Dedup hardening: check whether ANY Gmail message has been exchanged
    with this address (sent OR received, any label, any subject).

    This catches the gap where a carrier sits in sheet status=NEW but we
    already talked to them manually, via a prior import, or via a previous
    outreach whose sheet update failed. Runs before every initial send.

    Returns True iff Gmail has at least one message involving this address.
    Any API failure returns False (fail-open to avoid silently blocking sends
    on transient errors — the sheet-status check remains the primary gate).
    """
    if not carrier_email or "@" not in carrier_email:
        return False
    try:
        from app.google_auth import get_gmail_service
        svc = get_gmail_service()
        query = f"(to:{carrier_email} OR from:{carrier_email})"
        resp = svc.users().messages().list(
            userId="me", q=query, maxResults=1,
        ).execute()
        return bool(resp.get("messages", []))
    except Exception as e:
        logger.warning("Gmail dedup thread check failed for %s: %s — allowing send",
                       carrier_email, e)
        return False


def _verify_gmail_ready() -> bool:
    """Safeguard: verify the Gmail integration is functional before sending."""
    try:
        from app.google_auth import get_gmail_service
        svc = get_gmail_service()
        profile = svc.users().getProfile(userId="me").execute()
        email = profile.get("emailAddress", "")
        if email:
            logger.info("Gmail integration verified: sending as %s", email)
            return True
        logger.error("Gmail profile returned no email address")
        return False
    except Exception as e:
        logger.error("Gmail integration check failed: %s", e)
        return False


# ── Core outreach logic ───────────────────────────────────────────────────

def _send_initial_outreach(carrier: dict) -> bool:
    """Send initial outreach email to a NEW carrier. Returns True on success."""
    mc = carrier.get("MC_Number", "")
    dot = carrier.get("DOT_Number", "")
    email = carrier.get("Primary_Email", "").strip()
    carrier_key = mc or dot

    subject, body = build_initial_outreach(carrier)

    try:
        result = send_email(to=email, subject=subject, body_text=body)
        thread_id = result.get("threadId", "")

        updates = {
            "Onboarding_Status": "OUTREACH_SENT",
            "Last_Load_Date": date.today().isoformat(),  # reuse as Last_Contacted
        }
        if thread_id:
            # Store thread ID in Internal_Notes for follow-up threading
            existing_notes = carrier.get("Internal_Notes", "")
            thread_note = f"[Outreach {date.today().isoformat()}] threadId={thread_id}"
            if existing_notes:
                updates["Internal_Notes"] = f"{existing_notes} | {thread_note}"
            else:
                updates["Internal_Notes"] = thread_note

        update_carrier_fields_by_key(mc, dot, updates)
        logger.info("Initial outreach sent to %s (%s) — thread %s",
                     carrier_key, email, thread_id)
        return True

    except Exception as e:
        logger.error("Failed to send initial outreach to %s (%s): %s",
                      carrier_key, email, e)
        return False


def _send_followup(carrier: dict, followup_num: int) -> bool:
    """Send a follow-up email in-thread. followup_num is 1 or 2."""
    mc = carrier.get("MC_Number", "")
    dot = carrier.get("DOT_Number", "")
    email = carrier.get("Primary_Email", "").strip()
    carrier_key = mc or dot

    # Find the original outreach thread
    thread_id = _extract_thread_id_from_notes(carrier)
    if not thread_id:
        # Try searching Gmail for the thread
        thread_id = _find_outreach_thread(email)

    if not thread_id:
        logger.warning("No outreach thread found for %s — cannot send follow-up #%d",
                        carrier_key, followup_num)
        return False

    subject = f"Freight opportunities - deBoer Freight (MC#{DEBOER_MC})"

    if followup_num == 1:
        body = build_followup_1(carrier)
        next_status = "FOLLOW_UP_1"
    else:
        body = build_followup_2(carrier)
        next_status = "FOLLOW_UP_2"

    try:
        reply_to_thread(
            thread_id=thread_id,
            to=email,
            subject=subject,
            body_text=body,
        )
        update_carrier_fields_by_key(mc, dot, {
            "Onboarding_Status": next_status,
            "Last_Load_Date": date.today().isoformat(),
        })
        logger.info("Follow-up #%d sent to %s (%s) in thread %s",
                     followup_num, carrier_key, email, thread_id)
        return True

    except Exception as e:
        logger.error("Failed to send follow-up #%d to %s (%s): %s",
                      followup_num, carrier_key, email, e)
        return False


def _extract_thread_id_from_notes(carrier: dict) -> Optional[str]:
    """Extract a stored threadId from Internal_Notes."""
    notes = carrier.get("Internal_Notes", "")
    if "threadId=" not in notes:
        return None
    # Find the last threadId mention
    import re
    matches = re.findall(r"threadId=(\S+)", notes)
    return matches[-1] if matches else None


# ── Main run function ──────────────────────────────────────────────────────

def run(
    dry_run: bool = False,
    batch_limit: int = BATCH_LIMIT,
    send_delay: float = SEND_DELAY_SECONDS,
) -> dict:
    """
    Execute a full outreach cycle.

    Steps:
      1. NEW carriers with valid email -> initial outreach
      2. OUTREACH_SENT aged 3+ days    -> follow-up #1
      3. FOLLOW_UP_1 aged 4+ days      -> follow-up #2
      4. FOLLOW_UP_2 aged 7+ days      -> mark UNRESPONSIVE

    Args:
        dry_run: If True, log what would happen but don't send or update.
        batch_limit: Max total emails to send per run.
        send_delay: Seconds to wait between sends.

    Returns:
        Dict with counts: initial_sent, followup_1_sent, followup_2_sent,
        marked_unresponsive, phone_only_skipped, errors.
    """
    stats = {
        "initial_sent": 0,
        "followup_1_sent": 0,
        "followup_2_sent": 0,
        "marked_unresponsive": 0,
        "phone_only_skipped": 0,
        "errors": 0,
    }
    total_sent = 0

    # Verify Gmail is ready before doing anything
    if not dry_run:
        if not _verify_gmail_ready():
            logger.error("Gmail integration not ready — aborting outreach cycle")
            return stats

    all_carriers = get_all_carriers()
    logger.info("Loaded %d carriers from Carrier_Master", len(all_carriers))

    # Categorize carriers by status
    new_carriers = []
    outreach_sent = []
    followup_1 = []
    followup_2 = []
    phone_only = []

    vetting_skipped = 0
    for c in all_carriers:
        status = (c.get("Onboarding_Status") or "").strip().upper()

        # Vetting gate: only carriers whose sheet-level Vetting Status is
        # 'pass_basic' are eligible for any outreach action. This enforces
        # the 3 hard-reject rules (fleet>=3, liability>=$1M, cargo>=$100K).
        if not is_carrier_vetted(c):
            vetting_skipped += 1
            continue

        if _is_phone_only(c):
            if status == "NEW":
                phone_only.append(c)
            continue

        if status == "NEW":
            if _has_valid_email(c):
                new_carriers.append(c)
            else:
                phone_only.append(c)
        elif status == "OUTREACH_SENT":
            outreach_sent.append(c)
        elif status == "FOLLOW_UP_1":
            followup_1.append(c)
        elif status == "FOLLOW_UP_2":
            followup_2.append(c)

    if vetting_skipped:
        logger.info(
            "Vetting gate filtered %d carriers (Vetting Status != pass_basic)",
            vetting_skipped,
        )
    stats["vetting_skipped"] = vetting_skipped

    # Log PHONE_ONLY carriers
    for c in phone_only:
        carrier_key = c.get("MC_Number") or c.get("DOT_Number", "?")
        logger.info("PHONE_ONLY: %s (%s) — skipping email outreach",
                     carrier_key, c.get("Legal_Name", ""))
        stats["phone_only_skipped"] += 1

    logger.info(
        "Outreach candidates: %d new, %d awaiting follow-up #1, "
        "%d awaiting follow-up #2, %d awaiting unresponsive mark, %d phone-only",
        len(new_carriers), len(outreach_sent), len(followup_1),
        len(followup_2), len(phone_only),
    )

    # ── Step 1: Initial outreach to NEW carriers ──
    for carrier in new_carriers:
        if total_sent >= batch_limit:
            logger.info("Batch limit (%d) reached — stopping", batch_limit)
            break

        carrier_key = carrier.get("MC_Number") or carrier.get("DOT_Number", "?")
        email = carrier.get("Primary_Email", "")

        # Gate 3 dedup hardening: if the sheet says NEW but Gmail already has
        # a thread with this address (manual correspondence, prior import, or
        # a failed-sheet-update send from an earlier run), skip. This is the
        # second line of defense behind the status-based categorization.
        if _has_any_prior_gmail_thread(email):
            logger.info("SKIP initial outreach for %s (%s) — existing Gmail thread found",
                         carrier_key, email)
            stats.setdefault("skipped_existing_thread", 0)
            stats["skipped_existing_thread"] += 1
            continue

        if dry_run:
            logger.info("[DRY RUN] Would send initial outreach to %s (%s)",
                         carrier_key, email)
            stats["initial_sent"] += 1
            total_sent += 1
            continue

        if _send_initial_outreach(carrier):
            stats["initial_sent"] += 1
            total_sent += 1
            if total_sent < batch_limit:
                time.sleep(send_delay)
        else:
            stats["errors"] += 1

    # ── Step 2: Follow-up #1 for OUTREACH_SENT (3+ days) ──
    for carrier in outreach_sent:
        if total_sent >= batch_limit:
            break

        days = _days_since(carrier.get("Last_Load_Date", ""))
        if days is None or days < FOLLOW_UP_1_DELAY_DAYS:
            continue

        carrier_key = carrier.get("MC_Number") or carrier.get("DOT_Number", "?")
        email = carrier.get("Primary_Email", "")

        if not _has_valid_email(carrier):
            continue

        if dry_run:
            logger.info("[DRY RUN] Would send follow-up #1 to %s (%s) — %d days since outreach",
                         carrier_key, email, days)
            stats["followup_1_sent"] += 1
            total_sent += 1
            continue

        if _send_followup(carrier, followup_num=1):
            stats["followup_1_sent"] += 1
            total_sent += 1
            if total_sent < batch_limit:
                time.sleep(send_delay)
        else:
            stats["errors"] += 1

    # ── Step 3: Follow-up #2 for FOLLOW_UP_1 (4+ days) ──
    for carrier in followup_1:
        if total_sent >= batch_limit:
            break

        days = _days_since(carrier.get("Last_Load_Date", ""))
        if days is None or days < FOLLOW_UP_2_DELAY_DAYS:
            continue

        carrier_key = carrier.get("MC_Number") or carrier.get("DOT_Number", "?")
        email = carrier.get("Primary_Email", "")

        if not _has_valid_email(carrier):
            continue

        if dry_run:
            logger.info("[DRY RUN] Would send follow-up #2 to %s (%s) — %d days since follow-up #1",
                         carrier_key, email, days)
            stats["followup_2_sent"] += 1
            total_sent += 1
            continue

        if _send_followup(carrier, followup_num=2):
            stats["followup_2_sent"] += 1
            total_sent += 1
            if total_sent < batch_limit:
                time.sleep(send_delay)
        else:
            stats["errors"] += 1

    # ── Step 4: Mark FOLLOW_UP_2 carriers as UNRESPONSIVE (7+ days) ──
    for carrier in followup_2:
        days = _days_since(carrier.get("Last_Load_Date", ""))
        if days is None or days < UNRESPONSIVE_DELAY_DAYS:
            continue

        mc = carrier.get("MC_Number", "")
        dot = carrier.get("DOT_Number", "")
        carrier_key = mc or dot

        if dry_run:
            logger.info("[DRY RUN] Would mark %s (%s) as UNRESPONSIVE — %d days since follow-up #2",
                         carrier_key, carrier.get("Legal_Name", ""), days)
            stats["marked_unresponsive"] += 1
            continue

        try:
            update_carrier_fields_by_key(mc, dot, {
                "Onboarding_Status": "UNRESPONSIVE",
            })
            logger.info("Marked %s (%s) as UNRESPONSIVE after %d days",
                         carrier_key, carrier.get("Legal_Name", ""), days)
            stats["marked_unresponsive"] += 1
        except Exception as e:
            logger.error("Failed to mark %s as UNRESPONSIVE: %s", carrier_key, e)
            stats["errors"] += 1

    logger.info(
        "Outreach cycle complete: %d initial, %d follow-up #1, %d follow-up #2, "
        "%d marked unresponsive, %d phone-only skipped, %d errors",
        stats["initial_sent"], stats["followup_1_sent"], stats["followup_2_sent"],
        stats["marked_unresponsive"], stats["phone_only_skipped"], stats["errors"],
    )
    return stats
