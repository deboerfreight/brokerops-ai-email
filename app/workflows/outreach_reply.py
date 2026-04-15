"""
Workflow: Carrier Outreach Reply Processing

Handles replies from carriers to general outreach emails (not load-specific RFQs).
When a carrier responds to Sofia's outreach:
  1. Parse interest signals, lanes, equipment, rates from the reply.
  2. Update Carrier_Master with extracted info.
  3. Send Sofia's follow-up requesting onboarding docs (W-9, COI, Authority Letter, ACH)
     and lane pricing.
  4. Label and mark processed.

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
    reply_to_thread, add_label, get_thread,
)
from app.google_auth import get_gmail_service, get_sheets_service
from app.sheets import (
    get_all_carriers, get_carrier, update_carrier_fields,
    update_carrier_fields_by_key, is_message_processed,
    mark_message_processed, get_broker_settings,
)

logger = logging.getLogger("brokerops.workflows.outreach_reply")


# ── Slack notifications ────────────────────────────────────────────────────
# Wired to real webhook 2026-04-14. notify_slack() in app/notifications.py
# degrades to logger-only when SLACK_WEBHOOK_URL is blank, so this import is
# safe even without the env var set.
from app.notifications import notify_slack as _notify_slack  # noqa: E402


# ── Gemini-based reply analysis ─────────────────────────────────────────────

_OUTREACH_REPLY_PROMPT = """You are a freight brokerage assistant for De Boer Freight.
A motor carrier has replied to our outreach email. Analyze their reply and extract:

Return ONLY a JSON object with these fields:
- "interested": true/false — are they interested in working with us?
- "lanes": list of strings — any lanes/routes they mention (e.g. "Miami to Orlando", "South FL")
- "equipment_types": list of strings — equipment they have (e.g. "box truck", "reefer", "flatbed")
- "rate_info": string — any rate/pricing info they mention, or empty string
- "contact_name": string — contact person name if mentioned, or empty string
- "contact_phone": string — phone number if mentioned, or empty string
- "contact_email": string — alternate email if mentioned, or empty string
- "fleet_size": string — number of trucks/units if mentioned, or empty string
- "commodities": string — what they typically haul, or empty string
- "service_areas": string — geographic areas they serve, or empty string
- "decline_reason": string — if not interested, why, or empty string
- "notes": string — any other useful details

No markdown, no backticks, just JSON.
"""


def _analyze_reply(body: str, subject: str, from_addr: str) -> dict:
    """Use Gemini to analyze a carrier's outreach reply."""
    from app.ai_parser import _call_gemini, _extract_json

    full_text = (
        f"From: {from_addr}\n"
        f"Subject: {subject}\n\n"
        f"{body}"
    )
    prompt = f"{_OUTREACH_REPLY_PROMPT}\n\nCarrier Reply:\n---\n{full_text}\n---\n\nJSON:"

    try:
        text = _call_gemini(prompt, max_tokens=512)
        result = _extract_json(text)
        logger.info("Outreach reply analysis: interested=%s, lanes=%s",
                     result.get("interested"), result.get("lanes"))
        return result
    except Exception as e:
        logger.error("Outreach reply analysis failed: %s", e)
        return {"interested": True, "lanes": [], "equipment_types": [],
                "rate_info": "", "notes": "Analysis failed — treat as interested"}


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


def _greeting_from_carrier(carrier: dict) -> str:
    """Render a safe greeting line from a carrier row.

    Mirrors the pattern in carrier_outreach.py: DBA_Name preferred, Legal_Name
    title-cased as fallback (to avoid shouted ALL CAPS from FMCSA data), and
    a plain 'Hello,' when no usable name exists. A single-word fragment is
    still title-cased so ``TAMPA`` becomes ``Tampa``.
    """
    dba = (carrier.get("DBA_Name") or "").strip()
    if dba:
        # Title-case multi-word shouted names (e.g. "TAMPA TRANSPORT LLC");
        # preserve normal mixed-case brands by not forcing .title() if it
        # already contains lowercase letters.
        if dba.isupper():
            name = dba.title()
        else:
            name = dba
        return f"Hello {name},"
    legal = (carrier.get("Legal_Name") or "").strip()
    if legal:
        name = legal.title() if legal.isupper() else legal
        return f"Hello {name},"
    return "Hello,"


def _build_sofia_followup(carrier: dict, analysis: dict) -> str:
    """Build Sofia's follow-up email requesting docs and lane pricing."""
    try:
        broker = get_broker_settings()
    except Exception:
        broker = {}

    contact_name = (analysis.get("contact_name") or "").strip()
    if contact_name:
        greeting = f"Hello {contact_name},"
    else:
        # Fall back to carrier display name (title-cased to avoid shouted
        # ALL CAPS DBA/Legal names — same pattern as carrier_outreach.py).
        greeting = _greeting_from_carrier(carrier)

    broker_company = broker.get("Broker_Company_Name", "deBoer Freight")
    broker_phone = broker.get("Broker_Company_Phone", "305-767-3480")

    body = f"""{greeting}

Thanks for the reply.

To get you set up in our system, we will need the following documents:

1. W-9 Form (completed and signed)
2. Certificate of Insurance (COI) showing:
   - Auto Liability coverage of at least $1,000,000
   - Cargo coverage of at least $100,000
3. Copy of your Operating Authority letter
4. ACH/direct deposit information for payment

Please reply to this email with the documents attached as PDF files.

We would also like to get your pricing for any lanes you run regularly. If you could share your rates for the routes and equipment types you cover, that will help us start matching you with available freight right away.

Thanks,
Sofia Reyes
Carrier Relations
{broker_company}
{broker_phone}
"""
    return body


def _build_sofia_decline_followup(carrier: dict, analysis: dict) -> str:
    """Build Sofia's polite follow-up when a carrier declines."""
    try:
        broker = get_broker_settings()
    except Exception:
        broker = {}

    contact_name = (analysis.get("contact_name") or "").strip()
    if contact_name:
        greeting = f"Hello {contact_name},"
    else:
        greeting = _greeting_from_carrier(carrier)

    broker_company = broker.get("Broker_Company_Name", "deBoer Freight")
    broker_phone = broker.get("Broker_Company_Phone", "305-767-3480")

    body = f"""{greeting}

Thank you for letting us know. We completely understand.

If things change, shoot us a note.

Could you share which lanes and areas you typically run? That way we can reach out when we have freight that fits your operation.

Appreciate it,
Sofia Reyes
Carrier Relations
{broker_company}
{broker_phone}
"""
    return body


def _update_carrier_from_analysis(carrier: dict, analysis: dict) -> None:
    """Update Carrier_Master fields based on parsed reply data."""
    mc = carrier.get("MC_Number", "")
    dot = carrier.get("DOT_Number", "")
    updates: dict[str, str] = {}

    # Build notes from analysis
    notes_parts = []
    if analysis.get("lanes"):
        notes_parts.append(f"Lanes: {', '.join(analysis['lanes'])}")
    if analysis.get("commodities"):
        notes_parts.append(f"Commodities: {analysis['commodities']}")
    if analysis.get("service_areas"):
        notes_parts.append(f"Service areas: {analysis['service_areas']}")
    if analysis.get("fleet_size"):
        notes_parts.append(f"Fleet: {analysis['fleet_size']}")
    if analysis.get("rate_info"):
        notes_parts.append(f"Rates: {analysis['rate_info']}")
    if analysis.get("decline_reason"):
        notes_parts.append(f"Declined: {analysis['decline_reason']}")
    if analysis.get("notes"):
        notes_parts.append(analysis["notes"])

    if notes_parts:
        existing_notes = carrier.get("Internal_Notes", "")
        new_notes = f"[Outreach reply {date.today().isoformat()}] {'; '.join(notes_parts)}"
        if existing_notes:
            updates["Internal_Notes"] = f"{existing_notes} | {new_notes}"
        else:
            updates["Internal_Notes"] = new_notes

    # Update contact info if we got better data
    if analysis.get("contact_phone") and not carrier.get("Primary_Phone"):
        updates["Primary_Phone"] = analysis["contact_phone"]

    # Update equipment types if carrier self-reported
    if analysis.get("equipment_types"):
        equip_str = ", ".join(analysis["equipment_types"]).upper()
        updates["Equipment_Type"] = equip_str

    # Update preferred lanes
    if analysis.get("lanes"):
        existing_lanes = carrier.get("Preferred_Lanes", "")
        new_lanes = ", ".join(analysis["lanes"])
        if existing_lanes:
            updates["Preferred_Lanes"] = f"{existing_lanes}, {new_lanes}"
        else:
            updates["Preferred_Lanes"] = new_lanes

    # Mark onboarding status
    if analysis.get("interested"):
        updates["Onboarding_Status"] = "OUTREACH_INTERESTED"
    else:
        updates["Onboarding_Status"] = "OUTREACH_DECLINED"

    if updates:
        update_carrier_fields_by_key(mc, dot, updates)
        logger.info("Updated carrier MC=%s DOT=%s with outreach reply data: %s",
                     mc, dot, list(updates.keys()))


def run() -> list[str]:
    """
    Process carrier replies to outreach emails (labeled OPS/OUTREACH_REPLY).
    Returns list of message IDs that were processed.
    """
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
            thread_id = msg.get("threadId", "")

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

            mc = carrier.get("MC_Number", "")
            logger.info("Processing outreach reply from %s (MC=%s): '%s'",
                         sender_email, mc, subject)

            # Analyze the reply with Gemini
            analysis = _analyze_reply(body, subject, from_addr)

            # Update carrier record
            _update_carrier_from_analysis(carrier, analysis)

            # Send Sofia's follow-up — gated by OUTREACH_AUTO_REPLY_ENABLED
            auto_reply_enabled = get_settings().OUTREACH_AUTO_REPLY_ENABLED
            if analysis.get("interested", True):
                if auto_reply_enabled:
                    reply_body = _build_sofia_followup(carrier, analysis)
                    reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}"
                    reply_to_thread(
                        thread_id=thread_id,
                        to=sender_email,
                        subject=reply_subject,
                        body_text=reply_body,
                    )
                    add_label(msg_id, "OPS/ONBOARDING")
                    logger.info("Sofia sent onboarding follow-up to %s (MC=%s)", sender_email, mc)
                else:
                    logger.info("AUTO-REPLY DISABLED: would have sent Sofia onboarding follow-up to %s (MC=%s)", sender_email, mc)
            else:
                if auto_reply_enabled:
                    reply_body = _build_sofia_decline_followup(carrier, analysis)
                    reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}"
                    reply_to_thread(
                        thread_id=thread_id,
                        to=sender_email,
                        subject=reply_subject,
                        body_text=reply_body,
                    )
                    logger.info("Sofia sent decline follow-up to %s (MC=%s)", sender_email, mc)
                else:
                    logger.info("AUTO-REPLY DISABLED: would have sent Sofia decline follow-up to %s (MC=%s)", sender_email, mc)

            mark_message_processed(msg_id, f"outreach_reply:{mc}:interested={analysis.get('interested')}")
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
