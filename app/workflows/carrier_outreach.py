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
SOFIA_PHONE = "305-395-9401"


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


def _carrier_display_name(carrier: dict) -> str:
    """Return the best name to use when greeting a carrier."""
    return carrier.get("DBA_Name") or carrier.get("Legal_Name", "Carrier")


def _carrier_region(carrier: dict) -> str:
    """Best-effort region description from preferred lanes or notes."""
    lanes = carrier.get("Preferred_Lanes", "").strip()
    if lanes:
        return lanes
    return "South Florida"


def _carrier_equipment(carrier: dict) -> str:
    """Return equipment string, defaulting to a generic term."""
    eq = carrier.get("Equipment_Type", "").strip()
    return eq if eq else "dry van and refrigerated"


def build_initial_outreach(carrier: dict) -> tuple[str, str]:
    """Return (subject, body) for the initial outreach email."""
    info = _get_broker_info()
    name = _carrier_display_name(carrier)
    region = _carrier_region(carrier)
    equipment = _carrier_equipment(carrier)

    subject = f"Freight opportunities \u2014 deBoer Freight (MC#{DEBOER_MC})"

    body = f"""Hello {name},

My name is Sofia Reyes and I work with deBoer Freight (MC#{DEBOER_MC}). We move {equipment} freight in the {region} area and we are looking for reliable carriers to partner with.

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
    name = _carrier_display_name(carrier)
    region = _carrier_region(carrier)

    return f"""Hello {name},

I wanted to follow up on my earlier message. We have freight moving through the {region} area regularly and would like to make sure you are on our carrier list.

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
    name = _carrier_display_name(carrier)
    region = _carrier_region(carrier)

    return f"""Hello {name},

This is my last note on this thread. If you are ever looking for freight in the {region} area, we are here and ready to work together. Simply reply to this email any time and we will get you set up.

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

    subject = f"Freight opportunities \u2014 deBoer Freight (MC#{DEBOER_MC})"

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

    for c in all_carriers:
        status = (c.get("Onboarding_Status") or "").strip().upper()

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
