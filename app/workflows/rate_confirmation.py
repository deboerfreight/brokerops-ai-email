"""
Workflow: Rate Confirmation (Google Doc → PDF)

For loads with Load_Status=DISPATCHED and Approval_Status=APPROVED:
  1. Copy the Rate_Confirmation_Template Google Doc.
  2. Replace placeholders with load + carrier data.
  3. Export to PDF.
  4. Email PDF to carrier.
  5. Store PDF in BrokerOps/Loads/{Load_ID}/.
  6. Update Load_Status to COMPLETED (or keep DISPATCHED and let manual process finalize).
"""
from __future__ import annotations

import logging
from datetime import date

from app.config import get_settings
from app.gmail import send_email
from app.sheets import (
    get_loads_by_status, get_carrier, update_load_fields,
    get_broker_settings,
)
from app.drive import (
    copy_template, replace_placeholders, export_as_pdf,
    upload_file, ensure_folder,
)

logger = logging.getLogger("brokerops.workflows.rate_confirmation")


def _build_replacements(load: dict, carrier: dict, broker: dict) -> dict[str, str]:
    """Build a placeholder → value mapping for the rate confirmation template."""
    return {
        "{Load_ID}": load.get("Load_ID", ""),
        "{Dispatch_Date}": date.today().strftime("%m/%d/%Y"),
        "{Your_Company_Name}": broker.get("Broker_Company_Name", ""),
        "{Your_Company_Address}": broker.get("Broker_Company_Address", ""),
        "{Your_Company_Phone}": broker.get("Broker_Company_Phone", ""),
        "{Your_Company_Email}": broker.get("Broker_Company_Email", ""),
        "{Carrier_Legal_Name}": carrier.get("Legal_Name", ""),
        "{Carrier_DBA}": carrier.get("DBA_Name", ""),
        "{Carrier_MC}": carrier.get("MC_Number", ""),
        "{Carrier_DOT}": carrier.get("DOT_Number", ""),
        "{Carrier_Phone}": carrier.get("Primary_Phone", ""),
        "{Carrier_Email}": carrier.get("Primary_Email", ""),
        "{Origin_City}": load.get("Origin_City", ""),
        "{Origin_State}": load.get("Origin_State", ""),
        "{Origin_Zip}": load.get("Origin_Zip", ""),
        "{Destination_City}": load.get("Destination_City", ""),
        "{Destination_State}": load.get("Destination_State", ""),
        "{Destination_Zip}": load.get("Destination_Zip", ""),
        "{Pickup_Date}": load.get("Pickup_Date", ""),
        "{Pickup_Time_Window}": load.get("Pickup_Time_Window", "OPEN"),
        "{Delivery_Date}": load.get("Delivery_Date", ""),
        "{Delivery_Time_Window}": load.get("Delivery_Time_Window", "OPEN"),
        "{Equipment_Type}": load.get("Equipment_Type", ""),
        "{Commodity}": load.get("Commodity", ""),
        "{Weight_Lbs}": load.get("Weight_Lbs", ""),
        "{Agreed_Rate}": load.get("Target_Buy_Rate", ""),
        "{Temp_Control}": load.get("Temp_Control_Required", "FALSE"),
        "{Hazmat}": load.get("Hazmat", "FALSE"),
    }


def run() -> list[str]:
    """Generate and send rate confirmations for all DISPATCHED+APPROVED loads."""
    settings = get_settings()
    completed: list[str] = []

    loads = get_loads_by_status("DISPATCHED")

    try:
        broker = get_broker_settings()
    except Exception:
        broker = {}

    for load in loads:
        load_id = load["Load_ID"]

        if load.get("Approval_Status") != "APPROVED":
            continue

        # Skip if rate conf already sent (check Internal_Notes)
        if "rate_conf_sent" in (load.get("Internal_Notes", "") or "").lower():
            continue

        mc = load.get("Assigned_Carrier_MC", "")
        if not mc:
            continue

        carrier = get_carrier(mc)
        if not carrier:
            logger.warning("Carrier MC=%s not found for load %s", mc, load_id)
            continue

        try:
            # 1. Copy template
            doc_name = f"Rate_Confirmation_{load_id}"
            load_folder_id = ensure_folder(load_id, settings.LOADS_FOLDER_ID)
            new_doc_id = copy_template(
                settings.RATE_CONFIRMATION_TEMPLATE_ID,
                doc_name,
                load_folder_id,
            )

            # 2. Replace placeholders
            replacements = _build_replacements(load, carrier, broker)
            replace_placeholders(new_doc_id, replacements)

            # 3. Export to PDF
            pdf_bytes = export_as_pdf(new_doc_id)

            # 4. Store PDF in load folder
            pdf_filename = f"Rate_Confirmation_{load_id}.pdf"
            upload_file(pdf_filename, pdf_bytes, "application/pdf", load_folder_id)

            # 5. Email to carrier
            carrier_email = carrier.get("Primary_Email", "")
            if carrier_email:
                send_email(
                    to=carrier_email,
                    subject=f"Rate Confirmation | {load_id} | "
                            f"{load['Origin_City']},{load['Origin_State']} → "
                            f"{load['Destination_City']},{load['Destination_State']}",
                    body_text=f"Please find attached the rate confirmation for Load {load_id}.\n\n"
                              f"Please review, sign, and return at your earliest convenience.\n\n"
                              f"Thank you,\n{broker.get('Broker_Company_Name', 'BrokerOps')}",
                    attachments=[{
                        "filename": pdf_filename,
                        "data": pdf_bytes,
                    }],
                )

            # 6. Update load
            update_load_fields(load_id, {
                "Last_Updated": date.today().isoformat(),
                "Internal_Notes": f"Rate conf sent to {carrier_email}; rate_conf_sent",
            })

            completed.append(load_id)
            logger.info("Rate confirmation generated and sent for load %s to %s", load_id, carrier_email)

        except Exception:
            logger.exception("Failed to generate rate confirmation for load %s", load_id)

    return completed
