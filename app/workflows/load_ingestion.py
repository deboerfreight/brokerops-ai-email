"""
Workflow: Load Ingestion

Trigger: Gmail thread labelled OPS/NEW_LOAD
Steps:
  1. Fetch unprocessed messages with that label.
  2. Parse email to extract load fields.
  3. Generate Load_ID from Settings counter.
  4. Insert row into Load_Master.
  5. Create Drive folder BrokerOps/Loads/{Load_ID}/ and store email + attachments.
  6. Mark message processed; swap label to appropriate next state.
"""
from __future__ import annotations

import logging
from datetime import date

from app.config import get_settings
from app.gmail import (
    search_messages, get_message, get_body_text, get_header,
    get_attachments, add_label, remove_label,
)
from app.sheets import (
    get_next_load_id, insert_load,
    is_message_processed, mark_message_processed,
)
from app.drive import ensure_folder, upload_file, upload_text
from app.parsers import parse_load_email

logger = logging.getLogger("brokerops.workflows.load_ingestion")


def run() -> list[str]:
    """Process all OPS/NEW_LOAD messages. Return list of created Load_IDs."""
    settings = get_settings()
    created: list[str] = []

    messages = search_messages("OPS/NEW_LOAD")
    logger.info("Found %d message(s) with OPS/NEW_LOAD label.", len(messages))

    for stub in messages:
        msg_id = stub["id"]
        thread_id = stub.get("threadId", msg_id)

        if is_message_processed(msg_id):
            logger.debug("Skipping already-processed message %s", msg_id)
            continue

        try:
            msg = get_message(msg_id)
            subject = get_header(msg, "Subject")
            body = get_body_text(msg)
            from_addr = get_header(msg, "From")

            logger.info("[%s] Processing new load email from %s: '%s'", msg_id, from_addr, subject)

            # Parse
            fields = parse_load_email(body, subject)
            load_id = get_next_load_id()

            # Fill in system fields
            today = date.today().isoformat()
            fields.update({
                "Load_ID": load_id,
                "Customer_Rate": "",
                "Assigned_Carrier_MC": "",
                "Load_Status": "NEW",
                "Approval_Status": "PENDING",
                "RFQ_Count": "0",
                "Created_Date": today,
                "Last_Updated": today,
                "Internal_Notes": f"Ingested from Gmail msg {msg_id}",
            })

            # Insert into Load_Master
            insert_load(fields)

            # Create Drive folder
            load_folder_id = ensure_folder(load_id, settings.LOADS_FOLDER_ID)

            # Store email body
            upload_text(f"{load_id}_email.txt", f"From: {from_addr}\nSubject: {subject}\n\n{body}", load_folder_id)

            # Store attachments
            attachments = get_attachments(msg_id, msg)
            for att in attachments:
                upload_file(att["filename"], att["data"], att["mime_type"], load_folder_id)

            # Swap labels
            remove_label(msg_id, "OPS/NEW_LOAD")
            # The next step (carrier sourcing) will be triggered by the polling job

            mark_message_processed(msg_id, f"load_ingestion:{load_id}")
            created.append(load_id)
            logger.info("[%s] Load %s created successfully.", msg_id, load_id)

        except Exception:
            logger.exception("Failed to process message %s", msg_id)

    return created
