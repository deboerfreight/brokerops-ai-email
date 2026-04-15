"""
Workflow: Carrier Outreach Reply Processing

Handles replies from carriers to general outreach emails (not load-specific RFQs).
When a carrier responds to outreach:
  1. Classify the reply via reply_classifier.classify_reply().
  2. Route it (sheet updates, E4 scheduling, Slack) via reply_classifier.route_classified_reply().
  3. Label and mark processed.

Also handles MDL vendor replies (extension added 2026-04-14 per Bolt brief):
see run_mdl_vendor_replies() at the bottom of this file. The MDL vendor
reply handler MUST NOT read column F (Derek's Notes) on the MDL Vendor
Outreach sheet — it only touches col I (status).
"""
from __future__ import annotations

import logging
import re
from datetime import date
from typing import Optional

from app.config import get_settings
from app.gmail import (
    search_messages, get_message, get_body_text, get_header,
    add_label, get_thread,
)
from app.google_auth import get_gmail_service, get_sheets_service
from app.sheets import (
    get_all_carriers,
    is_message_processed,
    mark_message_processed,
)

logger = logging.getLogger("brokerops.workflows.outreach_reply")


# ── Slack notifications ────────────────────────────────────────────────────
# Wired to real webhook 2026-04-14. notify_slack() in app/notifications.py
# degrades to logger-only when SLACK_WEBHOOK_URL is blank, so this import is
# safe even without the env var set.
from app.notifications import notify_slack as _notify_slack  # noqa: E402


def _extract_sender_email(from_header: str) -> str:
    """Extract email from 'Name <email@example.com>' format."""
    m = re.search(r"<(.+?)>", from_header)
    return m.group(1).lower() if m else from_header.strip().lower()


def _find_carrier_by_email(email: str, carriers: list[dict]) -> Optional[dict]:
    """Look up a carrier by their email address."""
    email_lower = email.lower()
    for c in carriers:
        carrier_email = c.get("Primary_Email", "").strip().lower()
        if carrier_email and carrier_email == email_lower:
            return c
    return None


def run() -> list[str]:
    """
    Process carrier replies to outreach emails (labeled OPS/OUTREACH_REPLY).

    Classification and routing are delegated entirely to reply_classifier —
    the canonical path. Sofia handlers have been removed.

    Returns list of message IDs that were processed.
    """
    from app.reply_classifier import classify_reply, route_classified_reply

    settings = get_settings()
    processed: list[str] = []

    messages = search_messages("OPS/OUTREACH_REPLY")
    if not messages:
        logger.info("No messages in OPS/OUTREACH_REPLY.")
        return []

    logger.info("Found %d message(s) in OPS/OUTREACH_REPLY.", len(messages))

    # Build carrier email lookup
    all_carriers = get_all_carriers()

    for stub in messages:
        msg_id = stub["id"]

        if is_message_processed(msg_id):
            continue

        try:
            msg = get_message(msg_id)
            from_addr = get_header(msg, "From")
            subject = get_header(msg, "Subject")
            body = get_body_text(msg)
            sender_email = _extract_sender_email(from_addr)

            # Skip our own messages
            if sender_email == settings.BROKER_EMAIL.lower():
                mark_message_processed(msg_id, "our_own_outreach")
                continue

            # Find the carrier
            carrier = _find_carrier_by_email(sender_email, all_carriers)
            if not carrier:
                logger.warning("No carrier found for outreach reply from %s", sender_email)
                mark_message_processed(msg_id, f"unknown_carrier_outreach:{sender_email}")
                continue

            dot = carrier.get("DOT_Number", "")
            mc = carrier.get("MC_Number", "")
            logger.info(
                "Processing outreach reply from %s (DOT=%s MC=%s): '%s'",
                sender_email, dot, mc, subject,
            )

            # Classify and route via the canonical reply_classifier path
            classified = classify_reply(subject=subject, body=body, sender=from_addr)
            logger.info(
                "Classified reply DOT=%s as category=%s confidence=%s",
                dot, classified.category, classified.confidence,
            )
            # Fix 6: pass reply_body so draft flow can reference the original reply
            route_classified_reply(classified, carrier_dot=dot, reply_body=body)

            mark_message_processed(
                msg_id,
                f"outreach_reply:{dot}:category={classified.category}",
            )
            processed.append(msg_id)

        except Exception:
            logger.exception("Failed to process outreach reply %s", msg_id)

    logger.info("Processed %d outreach reply(s).", len(processed))
    return processed


# ── MDL Vendor reply handler ─────────────────────────────────────────────
# Added 2026-04-14 per Bolt brief. Inbound replies on MDL vendor threads
# are routed here, classified, and the MDL sheet's col I (Status) is
# updated. Column F (Derek's Notes) is NEVER touched. All reads use the
# disjoint ranges A:E and G:K — same privacy guarantee as the dispatcher.

_MDL_TAB = "Vendors"
_MDL_READ_AE = f"{_MDL_TAB}!A2:E"
_MDL_READ_GK = f"{_MDL_TAB}!G2:K"

_RFQ_KEYWORDS = re.compile(
    # Keyword list is explicit per Bolt brief 2026-04-14. Do NOT add
    # generic words like "load" or "shipment" — a casual reply like
    # "thanks, will send a test load tomorrow" should classify as
    # `replied`, not `rfq_received`. RFQ classification requires a
    # quoting/routing term or an attached quote document.
    r"\b(quote|rate|lane|pickup|delivery|weight|rfq|dims?|origin|destination)\b",
    re.IGNORECASE,
)
_OOO_KEYWORDS = re.compile(
    r"\b(out of office|automatic reply|auto[- ]?reply|vacation|on leave|"
    r"away from (my )?(desk|office))\b",
    re.IGNORECASE,
)
_UNSUB_KEYWORDS = re.compile(
    r"\b(unsubscribe|remove me|stop (emailing|contacting)|do not (email|contact))\b",
    re.IGNORECASE,
)


def _read_mdl_vendor_rows(sheet_id: str) -> list[dict]:
    """Read MDL vendor rows via disjoint A:E + G:K ranges. Col F excluded.

    Returns list of dicts keyed by row_number, vendor_company, email,
    status, thread_id. No F is ever requested from the Sheets API.
    """
    svc = get_sheets_service().spreadsheets()
    resp = svc.values().batchGet(
        spreadsheetId=sheet_id,
        ranges=[_MDL_READ_AE, _MDL_READ_GK],
        majorDimension="ROWS",
    ).execute()
    vrs = resp.get("valueRanges", [])
    if len(vrs) != 2:
        raise RuntimeError(f"Expected 2 ranges, got {len(vrs)}")
    ae = vrs[0].get("values", [])
    gk = vrs[1].get("values", [])
    n = max(len(ae), len(gk))

    def _cell(arr: list, idx: int) -> str:
        return (arr[idx] if idx < len(arr) else "") or ""

    rows = []
    for i in range(n):
        a = ae[i] if i < len(ae) else []
        g = gk[i] if i < len(gk) else []
        rows.append({
            "row_number": i + 2,
            "vendor_company": _cell(a, 0).strip(),
            "email": _cell(a, 3).strip(),
            "status": _cell(g, 2).strip(),
            "thread_id": _cell(g, 3).strip(),
        })
    return rows


def _update_mdl_status(sheet_id: str, row_number: int, status: str) -> None:
    """Write a single status cell (col I). Never touches col F."""
    svc = get_sheets_service().spreadsheets()
    svc.values().update(
        spreadsheetId=sheet_id,
        range=f"{_MDL_TAB}!I{row_number}",
        valueInputOption="USER_ENTERED",
        body={"values": [[status]]},
    ).execute()
    logger.info("MDL vendor row %d -> status=%s", row_number, status)


def _classify_mdl_reply(body: str, subject: str, has_attachment: bool) -> str:
    """Return one of: rfq_received / replied / awaiting_reply / stalled.

    - auto-reply / OOO          -> awaiting_reply (unchanged)
    - unsubscribe / remove      -> stalled
    - RFQ keywords OR attachment-> rfq_received
    - otherwise                 -> replied
    """
    text = f"{subject}\n{body}"
    if _OOO_KEYWORDS.search(text):
        return "awaiting_reply"
    if _UNSUB_KEYWORDS.search(text):
        return "stalled"
    if has_attachment or _RFQ_KEYWORDS.search(text):
        return "rfq_received"
    return "replied"


def _message_has_interesting_attachment(msg: dict) -> bool:
    """True if the message has an .xls/.xlsx/.pdf/.csv attachment."""
    payload = msg.get("payload", {})
    parts = payload.get("parts", []) or []
    for part in parts:
        fn = (part.get("filename") or "").lower()
        if not fn:
            continue
        if fn.endswith((".xls", ".xlsx", ".pdf", ".csv")):
            return True
    return False


def run_mdl_vendor_replies() -> dict:
    """Scan inbound sales@ mail for replies to MDL vendor first-touch threads.

    For each inbound message (label INBOX, newer than 7 days, not from us),
    look up its threadId in col J of the MDL Vendor Outreach sheet. On match,
    classify the reply, update col I, and Slack the outcome.

    Returns stats dict: scanned, matched, rfq_received, replied, stalled,
    awaiting_reply, errors.
    """
    stats = {
        "scanned": 0,
        "matched": 0,
        "rfq_received": 0,
        "replied": 0,
        "stalled": 0,
        "awaiting_reply": 0,
        "errors": 0,
    }

    settings = get_settings()
    sheet_id = settings.MDL_VENDOR_SHEET_ID
    if not sheet_id:
        logger.warning("MDL_VENDOR_SHEET_ID not set — skipping MDL reply sweep")
        return stats

    try:
        rows = _read_mdl_vendor_rows(sheet_id)
    except Exception as e:
        logger.exception("Failed to read MDL vendor sheet for reply sweep")
        _notify_slack(f"Nina MDL vendor reply sweep failed: sheet read error: {e}")
        stats["errors"] += 1
        return stats

    # Build thread_id -> row lookup for rows that have been sent and are
    # awaiting a reply (or already replied — Derek may get multiple emails
    # on the same thread).
    thread_lookup: dict[str, dict] = {}
    for r in rows:
        tid = r["thread_id"].strip()
        if tid:
            thread_lookup[tid] = r

    if not thread_lookup:
        logger.info("MDL reply sweep: no vendor rows with thread IDs yet")
        return stats

    # Fetch recent inbound messages (last 7 days, in inbox, not from us)
    try:
        svc = get_gmail_service()
        query = f"newer_than:7d in:inbox -from:{settings.BROKER_EMAIL}"
        resp = svc.users().messages().list(
            userId="me", q=query, maxResults=50,
        ).execute()
        msgs = resp.get("messages", [])
    except Exception as e:
        logger.exception("Failed to list inbound messages for MDL reply sweep")
        _notify_slack(f"Nina MDL vendor reply sweep failed: Gmail list error: {e}")
        stats["errors"] += 1
        return stats

    stats["scanned"] = len(msgs)
    logger.info("MDL reply sweep: scanning %d recent inbound message(s)", len(msgs))

    for stub in msgs:
        msg_id = stub["id"]
        try:
            msg = get_message(msg_id)
            thread_id = msg.get("threadId", "")
            if thread_id not in thread_lookup:
                continue

            row = thread_lookup[thread_id]
            if is_message_processed(msg_id):
                continue

            stats["matched"] += 1

            from_addr = get_header(msg, "From")
            subject = get_header(msg, "Subject")
            body = get_body_text(msg)
            has_att = _message_has_interesting_attachment(msg)

            new_status = _classify_mdl_reply(body, subject, has_att)

            # Only update if the classification actually moves the row forward.
            # awaiting_reply stays silent (OOO bounceback, etc).
            if new_status == "awaiting_reply":
                logger.info(
                    "MDL vendor reply (row %d, %s): looks like auto-reply/OOO — no update",
                    row["row_number"], row["vendor_company"],
                )
                stats["awaiting_reply"] += 1
                mark_message_processed(msg_id, f"mdl_vendor_ooo:{row['row_number']}")
                continue

            _update_mdl_status(sheet_id, row["row_number"], new_status)
            stats[new_status] = stats.get(new_status, 0) + 1

            _notify_slack(
                f"MDL vendor reply -> {row['vendor_company']} ({row['email']}) "
                f"classified as {new_status} (from {from_addr})"
            )

            if new_status == "stalled":
                _notify_slack(
                    f"ALERT: Unsubscribe/remove request from "
                    f"{row['vendor_company']} ({row['email']}) — Derek should review"
                )

            if new_status == "rfq_received":
                # Handoff hook: downstream agent-01 RFQ extraction is a
                # follow-up wiring task. For now we just stamp the status
                # and alert. The RFQ extractor can poll for rows where
                # status=rfq_received on its own schedule.
                logger.info(
                    "MDL vendor row %d classified rfq_received — agent-01 handoff "
                    "pending separate wiring",
                    row["row_number"],
                )

            mark_message_processed(msg_id, f"mdl_vendor_reply:{row['row_number']}:{new_status}")

        except Exception as e:
            logger.exception("Failed to process MDL vendor reply %s", msg_id)
            stats["errors"] += 1

    logger.info(
        "MDL reply sweep complete: scanned=%d matched=%d rfq=%d replied=%d "
        "stalled=%d ooo=%d errors=%d",
        stats["scanned"], stats["matched"], stats["rfq_received"],
        stats["replied"], stats["stalled"], stats["awaiting_reply"],
        stats["errors"],
    )
    return stats
