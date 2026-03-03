"""
Workflow: Carrier Onboarding

For loads with Load_Status=ONBOARDING_REQUIRED:
  1. Send onboarding request email to carrier asking for W9 + COI PDF.
  2. Watch for document submissions via email.
  3. Store docs in BrokerOps/Carriers/{MC_Number}/.
  4. Update carrier fields and advance load status.
"""
from __future__ import annotations

import logging
from datetime import date

from app.config import get_settings
from app.gmail import (
    send_email, search_messages, get_message, get_body_text,
    get_header, get_attachments, add_label,
)
from app.sheets import (
    get_loads_by_status, get_carrier, update_carrier_fields,
    update_load_fields, is_message_processed, mark_message_processed,
    is_carrier_dispatch_eligible, get_broker_settings,
)
from app.drive import ensure_folder, upload_file

logger = logging.getLogger("brokerops.workflows.onboarding")


def _send_onboarding_request(carrier: dict, load_id: str) -> None:
    """Send onboarding request email to carrier."""
    email = carrier.get("Primary_Email", "")
    mc = carrier.get("MC_Number", "")
    name = carrier.get("Legal_Name") or carrier.get("DBA_Name", "Carrier")

    try:
        broker = get_broker_settings()
    except Exception:
        broker = {}

    broker_name = broker.get("Broker_Company_Name", "BrokerOps")
    broker_phone = broker.get("Broker_Company_Phone", "")

    subject = f"Onboarding Required | Load {load_id} | MC# {mc}"
    body = f"""Hello {name},

Thank you for your interest in Load {load_id}. Before we can dispatch this load,
we need the following documents on file:

1. W-9 Form (completed and signed)
2. Certificate of Insurance (COI) showing:
   - Auto Liability coverage of at least $1,000,000
   - Cargo coverage of at least $100,000

Please reply to this email with the documents attached as PDF files.

If you have any questions, please don't hesitate to reach out.

Thank you,
{broker_name}
{broker_phone}
"""

    result = send_email(to=email, subject=subject, body_text=body)
    add_label(result["id"], "OPS/ONBOARDING")
    logger.info("Sent onboarding request to %s (MC=%s) for load %s", email, mc, load_id)


def run_send_requests() -> list[str]:
    """Send onboarding requests for all ONBOARDING_REQUIRED loads."""
    sent: list[str] = []
    loads = get_loads_by_status("ONBOARDING_REQUIRED")

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
            _send_onboarding_request(carrier, load_id)
            update_load_fields(load_id, {
                "Load_Status": "DOCS_PENDING",
                "Last_Updated": date.today().isoformat(),
            })
            sent.append(load_id)
        except Exception:
            logger.exception("Failed to send onboarding request for load %s", load_id)

    return sent


def run_check_documents() -> list[str]:
    """
    Check for onboarding document submissions in OPS/ONBOARDING threads.
    Process attachments and update carrier records.
    """
    settings = get_settings()
    updated: list[str] = []

    messages = search_messages("OPS/ONBOARDING")
    logger.info("Found %d message(s) in OPS/ONBOARDING.", len(messages))

    for stub in messages:
        msg_id = stub["id"]

        if is_message_processed(msg_id):
            continue

        try:
            msg = get_message(msg_id)
            from_addr = get_header(msg, "From")
            subject = get_header(msg, "Subject")

            # Skip our own messages
            if settings.BROKER_EMAIL.lower() in from_addr.lower():
                mark_message_processed(msg_id, "our_onboarding_request")
                continue

            # Get attachments
            attachments = get_attachments(msg_id, msg)
            if not attachments:
                mark_message_processed(msg_id, "no_attachments")
                continue

            # Extract MC number from subject
            import re
            mc_match = re.search(r"MC#?\s*(\d+)", subject)
            if not mc_match:
                mark_message_processed(msg_id, "no_mc_in_subject")
                continue

            mc = mc_match.group(1)
            carrier = get_carrier(mc)
            if not carrier:
                mark_message_processed(msg_id, f"carrier_not_found:{mc}")
                continue

            # Create carrier folder and upload docs
            carrier_folder_id = ensure_folder(mc, settings.CARRIERS_FOLDER_ID)

            updates: dict[str, str] = {}
            for att in attachments:
                filename = att["filename"].lower()
                upload_file(att["filename"], att["data"], att["mime_type"], carrier_folder_id)
                logger.info("Stored document '%s' for carrier MC=%s", att["filename"], mc)

                # Detect document type
                if "w9" in filename or "w-9" in filename:
                    updates["W9_On_File"] = "TRUE"
                if "coi" in filename or "insurance" in filename or "certificate" in filename:
                    # We'll need to manually verify the COI, but mark as received
                    updates["Onboarding_Status"] = "COI_RECEIVED"

            if updates:
                updates["Last_Load_Date"] = date.today().isoformat()
                update_carrier_fields(mc, updates)
                updated.append(mc)

                # Check if carrier is now dispatch-eligible
                refreshed = get_carrier(mc)
                if refreshed and is_carrier_dispatch_eligible(refreshed):
                    # Find loads assigned to this carrier in DOCS_PENDING
                    from app.sheets import get_loads_by_status
                    pending_loads = get_loads_by_status("DOCS_PENDING")
                    for load in pending_loads:
                        if load.get("Assigned_Carrier_MC") == mc:
                            update_load_fields(load["Load_ID"], {
                                "Load_Status": "READY_FOR_APPROVAL",
                                "Last_Updated": date.today().isoformat(),
                            })
                            logger.info(
                                "Load %s advanced to READY_FOR_APPROVAL (carrier MC=%s now eligible)",
                                load["Load_ID"], mc,
                            )

            mark_message_processed(msg_id, f"onboarding_docs:{mc}")

        except Exception:
            logger.exception("Failed to process onboarding message %s", msg_id)

    return updated
