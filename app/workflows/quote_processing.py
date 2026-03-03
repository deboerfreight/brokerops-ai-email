"""
Workflow: Quote Parsing & Carrier Selection

Watches for replies to RFQ emails:
  1. Search Gmail for threads labelled OPS/RFQ_SENT with new replies.
  2. Parse carrier quotes using "first dollar amount" rule.
  3. Select lowest valid rate (tie-break: On_Time_Score, Last_Load_Date).
  4. Set Assigned_Carrier_MC; advance Load_Status.
"""
from __future__ import annotations

import logging
import re
from datetime import date
from typing import Optional

from app.config import get_settings
from app.gmail import (
    search_messages, get_message, get_thread, get_body_text,
    get_header, add_label, remove_label,
)
from app.sheets import (
    get_loads_by_status, get_carrier, update_load_fields,
    is_message_processed, mark_message_processed,
    is_carrier_dispatch_eligible,
)
from app.parsers import parse_quote_reply

logger = logging.getLogger("brokerops.workflows.quote_processing")


def _extract_load_id_from_subject(subject: str) -> Optional[str]:
    """Extract Load_ID from RFQ subject like 'RFQ | 2026-0001 | ...'"""
    m = re.search(r"(\d{4}-\d{4})", subject)
    return m.group(1) if m else None


def _extract_sender_email(from_header: str) -> str:
    """Extract email from 'Name <email@example.com>' format."""
    m = re.search(r"<(.+?)>", from_header)
    return m.group(1).lower() if m else from_header.strip().lower()


def _find_carrier_by_email(email: str, carriers_cache: dict) -> Optional[dict]:
    """Look up carrier by email."""
    return carriers_cache.get(email.lower())


def run() -> list[str]:
    """
    Process RFQ replies and select carriers for loads in RFQ_SENT status.
    Returns list of load_ids that had carrier selection made.
    """
    from app.sheets import get_all_carriers

    selected: list[str] = []

    # Build email → carrier lookup
    all_carriers = get_all_carriers()
    email_to_carrier: dict[str, dict] = {}
    for c in all_carriers:
        email = c.get("Primary_Email", "").strip().lower()
        if email:
            email_to_carrier[email] = c

    # Get loads in RFQ_SENT or QUOTES_RECEIVED status
    rfq_loads = get_loads_by_status("RFQ_SENT") + get_loads_by_status("QUOTES_RECEIVED")
    load_id_set = {l["Load_ID"] for l in rfq_loads}

    if not load_id_set:
        logger.info("No loads in RFQ_SENT/QUOTES_RECEIVED status.")
        return []

    # Search for messages in RFQ threads
    messages = search_messages("OPS/RFQ_SENT")
    logger.info("Found %d message(s) in OPS/RFQ_SENT threads.", len(messages))

    # Collect quotes per load_id
    quotes_by_load: dict[str, list[dict]] = {}

    for stub in messages:
        msg_id = stub["id"]

        if is_message_processed(msg_id):
            continue

        try:
            msg = get_message(msg_id)
            subject = get_header(msg, "Subject")
            from_addr = get_header(msg, "From")
            body = get_body_text(msg)
            sender_email = _extract_sender_email(from_addr)

            # Check if this is from a carrier (not from us)
            settings = get_settings()
            if sender_email == settings.BROKER_EMAIL.lower():
                mark_message_processed(msg_id, "our_own_rfq")
                continue

            load_id = _extract_load_id_from_subject(subject)
            if not load_id or load_id not in load_id_set:
                mark_message_processed(msg_id, f"no_matching_load:{load_id}")
                continue

            # Parse the quote
            quote_data = parse_quote_reply(body)
            if quote_data["rate"] is None:
                logger.info("No rate found in reply from %s for load %s", sender_email, load_id)
                mark_message_processed(msg_id, f"no_rate:{load_id}")
                continue

            # Find carrier
            carrier = _find_carrier_by_email(sender_email, email_to_carrier)
            if not carrier:
                logger.warning("No carrier found for email %s", sender_email)
                mark_message_processed(msg_id, f"unknown_carrier:{sender_email}")
                continue

            quote_data["carrier"] = carrier
            quote_data["msg_id"] = msg_id
            quote_data["sender"] = sender_email

            if load_id not in quotes_by_load:
                quotes_by_load[load_id] = []
            quotes_by_load[load_id].append(quote_data)

            mark_message_processed(msg_id, f"quote:{load_id}:{quote_data['rate']}")
            logger.info(
                "Parsed quote from %s for load %s: $%.2f",
                sender_email, load_id, quote_data["rate"],
            )

        except Exception:
            logger.exception("Failed to process quote message %s", msg_id)

    # Select best carrier for each load
    for load_id, quotes in quotes_by_load.items():
        # Update status to QUOTES_RECEIVED
        update_load_fields(load_id, {
            "Load_Status": "QUOTES_RECEIVED",
            "Last_Updated": date.today().isoformat(),
        })

        # Filter to valid (dispatch-eligible) carriers
        valid_quotes = [
            q for q in quotes
            if is_carrier_dispatch_eligible(q["carrier"])
        ]

        # Also include onboarding-required carriers (W9 missing)
        onboarding_quotes = [
            q for q in quotes
            if not is_carrier_dispatch_eligible(q["carrier"])
            and q["carrier"].get("Authority_Status") == "ACTIVE"
            and q["carrier"].get("Compliance_Status") == "CLEAR"
        ]

        all_valid = valid_quotes + onboarding_quotes

        if not all_valid:
            logger.info("No valid quotes yet for load %s.", load_id)
            continue

        # Sort: lowest rate → highest On_Time_Score → most recent Last_Load_Date
        def sort_key(q):
            ot = int(q["carrier"].get("On_Time_Score", "0") or "0")
            lld = q["carrier"].get("Last_Load_Date", "1970-01-01") or "1970-01-01"
            return (q["rate"], -ot, lld)

        all_valid.sort(key=sort_key)
        best = all_valid[0]
        carrier = best["carrier"]
        mc = carrier.get("MC_Number", "")

        # Determine next status
        w9 = carrier.get("W9_On_File", "").upper() in ("TRUE", "YES", "1")
        if is_carrier_dispatch_eligible(carrier):
            next_status = "CARRIER_SELECTED"
        else:
            next_status = "ONBOARDING_REQUIRED"

        update_load_fields(load_id, {
            "Assigned_Carrier_MC": mc,
            "Load_Status": next_status,
            "Target_Buy_Rate": str(best["rate"]),
            "Last_Updated": date.today().isoformat(),
            "Internal_Notes": f"Selected carrier MC={mc} at ${best['rate']:.2f}",
        })

        # Update labels
        try:
            add_label(best["msg_id"], "OPS/QUOTES_RECEIVED")
        except Exception:
            pass

        selected.append(load_id)
        logger.info(
            "Load %s: selected carrier %s (MC=%s) at $%.2f, status=%s",
            load_id, carrier.get("Legal_Name"), mc, best["rate"], next_status,
        )

    return selected
