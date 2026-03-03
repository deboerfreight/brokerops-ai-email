"""
Workflow: Approval Gate

1. For loads at READY_FOR_APPROVAL (or CARRIER_SELECTED with eligible carrier):
   send an approval-packet email to Broker_Operations_Email.
2. Watch for APPROVE / REJECT replies.
3. Advance load to APPROVED → DISPATCHED or REJECTED.
"""
from __future__ import annotations

import logging
from datetime import date

from app.config import get_settings
from app.gmail import (
    send_email, search_messages, get_message, get_thread,
    get_body_text, get_header, add_label, remove_label,
)
from app.sheets import (
    get_loads_by_status, get_load, get_carrier,
    update_load_fields, is_carrier_dispatch_eligible,
    is_message_processed, mark_message_processed,
    get_broker_settings,
)
from app.parsers import parse_approval_reply

logger = logging.getLogger("brokerops.workflows.approval")


def _send_approval_packet(load: dict, carrier: dict) -> None:
    """Send the approval email to the broker ops email."""
    try:
        broker = get_broker_settings()
    except Exception:
        broker = {}

    ops_email = broker.get("Broker_Operations_Email", get_settings().BROKER_EMAIL)
    load_id = load["Load_ID"]

    subject = (
        f"APPROVAL REQUIRED | {load_id} | "
        f"{load['Origin_City']},{load['Origin_State']} → "
        f"{load['Destination_City']},{load['Destination_State']}"
    )

    body = f"""APPROVAL REQUIRED FOR LOAD {load_id}
{'='*50}

LOAD DETAILS
  Load ID:       {load_id}
  Customer:      {load.get('Customer_Name', '')}
  Origin:        {load.get('Origin_City', '')}, {load.get('Origin_State', '')} {load.get('Origin_Zip', '')}
  Destination:   {load.get('Destination_City', '')}, {load.get('Destination_State', '')} {load.get('Destination_Zip', '')}
  Pickup:        {load.get('Pickup_Date', '')} ({load.get('Pickup_Time_Window', 'OPEN')})
  Delivery:      {load.get('Delivery_Date', '')} ({load.get('Delivery_Time_Window', 'OPEN')})
  Equipment:     {load.get('Equipment_Type', '')}
  Commodity:     {load.get('Commodity', '')}
  Weight:        {load.get('Weight_Lbs', '')} lbs

SELECTED CARRIER
  Legal Name:    {carrier.get('Legal_Name', '')}
  DBA:           {carrier.get('DBA_Name', '')}
  MC Number:     {carrier.get('MC_Number', '')}
  DOT Number:    {carrier.get('DOT_Number', '')}
  Phone:         {carrier.get('Primary_Phone', '')}
  Email:         {carrier.get('Primary_Email', '')}

RATE & COMPLIANCE
  Agreed Rate:   ${load.get('Target_Buy_Rate', '0')}
  Authority:     {carrier.get('Authority_Status', '')} (verified {carrier.get('Authority_Verified_Date', 'N/A')})
  Insurance Exp: {carrier.get('Insurance_Expiration', '')}
  Auto Liability:{carrier.get('Auto_Liability_Coverage', '')}
  Cargo Coverage:{carrier.get('Cargo_Coverage', '')}
  Compliance:    {carrier.get('Compliance_Status', '')}
  W9 On File:    {carrier.get('W9_On_File', '')}
  On-Time Score: {carrier.get('On_Time_Score', '')}
  Claims Count:  {carrier.get('Claims_Count', '')}

{'='*50}
TO APPROVE, reply with exactly:
APPROVE {load_id}

TO REJECT, reply with exactly:
REJECT {load_id}
{'='*50}
"""

    result = send_email(to=ops_email, subject=subject, body_text=body)
    add_label(result["id"], "OPS/READY_FOR_APPROVAL")
    logger.info("Sent approval packet for load %s to %s", load_id, ops_email)


def run_send_packets() -> list[str]:
    """Send approval packets for loads ready for approval."""
    sent: list[str] = []

    # CARRIER_SELECTED loads with eligible carrier → READY_FOR_APPROVAL first
    cs_loads = get_loads_by_status("CARRIER_SELECTED")
    for load in cs_loads:
        mc = load.get("Assigned_Carrier_MC", "")
        if not mc:
            continue
        carrier = get_carrier(mc)
        if carrier and is_carrier_dispatch_eligible(carrier):
            update_load_fields(load["Load_ID"], {
                "Load_Status": "READY_FOR_APPROVAL",
                "Last_Updated": date.today().isoformat(),
            })

    # Now process all READY_FOR_APPROVAL
    loads = get_loads_by_status("READY_FOR_APPROVAL")

    for load in loads:
        load_id = load["Load_ID"]
        mc = load.get("Assigned_Carrier_MC", "")
        if not mc:
            continue

        carrier = get_carrier(mc)
        if not carrier:
            logger.warning("Carrier MC=%s not found for load %s", mc, load_id)
            continue

        try:
            _send_approval_packet(load, carrier)
            sent.append(load_id)
        except Exception:
            logger.exception("Failed to send approval packet for load %s", load_id)

    return sent


def run_check_replies() -> list[str]:
    """
    Check for APPROVE/REJECT replies in OPS/READY_FOR_APPROVAL threads.
    Only accepts replies from the broker's own email.
    """
    settings = get_settings()
    processed: list[str] = []

    messages = search_messages("OPS/READY_FOR_APPROVAL")
    logger.info("Found %d message(s) in OPS/READY_FOR_APPROVAL.", len(messages))

    try:
        broker = get_broker_settings()
    except Exception:
        broker = {}
    ops_email = broker.get("Broker_Operations_Email", settings.BROKER_EMAIL).lower()

    for stub in messages:
        msg_id = stub["id"]

        if is_message_processed(msg_id):
            continue

        try:
            msg = get_message(msg_id)
            from_addr = get_header(msg, "From").lower()
            body = get_body_text(msg)

            # Only accept from broker's own email
            if ops_email not in from_addr and settings.BROKER_EMAIL.lower() not in from_addr:
                mark_message_processed(msg_id, "not_from_broker")
                continue

            result = parse_approval_reply(body)
            action = result.get("action")
            load_id = result.get("load_id")

            if not action or not load_id:
                mark_message_processed(msg_id, "no_approval_action")
                continue

            load = get_load(load_id)
            if not load:
                mark_message_processed(msg_id, f"load_not_found:{load_id}")
                continue

            if action == "APPROVE":
                update_load_fields(load_id, {
                    "Approval_Status": "APPROVED",
                    "Load_Status": "DISPATCHED",
                    "Last_Updated": date.today().isoformat(),
                })
                # Swap labels
                try:
                    remove_label(msg_id, "OPS/READY_FOR_APPROVAL")
                    add_label(msg_id, "OPS/DISPATCHED")
                except Exception:
                    pass
                logger.info("Load %s APPROVED and DISPATCHED.", load_id)

            elif action == "REJECT":
                update_load_fields(load_id, {
                    "Approval_Status": "REJECTED",
                    "Load_Status": "BLOCKED",
                    "Last_Updated": date.today().isoformat(),
                    "Internal_Notes": f"Rejected by broker via email.",
                })
                try:
                    remove_label(msg_id, "OPS/READY_FOR_APPROVAL")
                    add_label(msg_id, "OPS/BLOCKED")
                except Exception:
                    pass
                logger.info("Load %s REJECTED.", load_id)

            mark_message_processed(msg_id, f"approval:{action}:{load_id}")
            processed.append(load_id)

        except Exception:
            logger.exception("Failed to process approval message %s", msg_id)

    return processed
