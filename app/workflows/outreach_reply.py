"""
Workflow: Carrier Outreach Reply Processing

Handles replies from carriers to general outreach emails (not load-specific RFQs).
When a carrier responds to Sofia's outreach:
  1. Parse interest signals, lanes, equipment, rates from the reply.
  2. Update Carrier_Master with extracted info.
  3. Send Sofia's follow-up requesting onboarding docs (W-9, COI, Authority Letter, ACH)
     and lane pricing.
  4. Label and mark processed.
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
from app.sheets import (
    get_all_carriers, get_carrier, update_carrier_fields,
    update_carrier_fields_by_key, is_message_processed,
    mark_message_processed, get_broker_settings,
)

logger = logging.getLogger("brokerops.workflows.outreach_reply")


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


def _build_sofia_followup(carrier: dict, analysis: dict) -> str:
    """Build Sofia's follow-up email requesting docs and lane pricing."""
    try:
        broker = get_broker_settings()
    except Exception:
        broker = {}

    name = carrier.get("DBA_Name") or carrier.get("Legal_Name", "").split(" ")[0]
    contact_name = analysis.get("contact_name", "")
    greeting_name = contact_name if contact_name else name

    broker_company = broker.get("Broker_Company_Name", "deBoer Freight")
    broker_phone = broker.get("Broker_Company_Phone", "305-767-3480")

    body = f"""Hello {greeting_name},

Thank you for getting back to us. We appreciate your interest in working together.

To get you set up in our system, we will need the following documents:

1. W-9 Form (completed and signed)
2. Certificate of Insurance (COI) showing:
   - Auto Liability coverage of at least $1,000,000
   - Cargo coverage of at least $100,000
3. Copy of your Operating Authority letter
4. ACH/direct deposit information for payment

Please reply to this email with the documents attached as PDF files.

We would also like to get your pricing for any lanes you run regularly. If you could share your rates for the routes and equipment types you cover, that will help us start matching you with available freight right away.

If you have any questions, feel free to call us at {broker_phone}.

Thank you,
Sofia Reyes
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

    name = carrier.get("DBA_Name") or carrier.get("Legal_Name", "").split(" ")[0]
    contact_name = analysis.get("contact_name", "")
    greeting_name = contact_name if contact_name else name

    broker_company = broker.get("Broker_Company_Name", "deBoer Freight")
    broker_phone = broker.get("Broker_Company_Phone", "305-767-3480")

    body = f"""Hello {greeting_name},

Thank you for letting us know. We completely understand.

If your availability changes or you have capacity in the future, we would be glad to hear from you. We are always looking for reliable carriers to partner with.

Could you share which lanes and areas you typically run? That way we can reach out when we have freight that fits your operation.

Thank you,
Sofia Reyes
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

            # Send Sofia's follow-up
            if analysis.get("interested", True):
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
                reply_body = _build_sofia_decline_followup(carrier, analysis)
                reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}"
                reply_to_thread(
                    thread_id=thread_id,
                    to=sender_email,
                    subject=reply_subject,
                    body_text=reply_body,
                )
                logger.info("Sofia sent decline follow-up to %s (MC=%s)", sender_email, mc)

            mark_message_processed(msg_id, f"outreach_reply:{mc}:interested={analysis.get('interested')}")
            processed.append(msg_id)

        except Exception:
            logger.exception("Failed to process outreach reply %s", msg_id)

    logger.info("Processed %d outreach reply(s).", len(processed))
    return processed
