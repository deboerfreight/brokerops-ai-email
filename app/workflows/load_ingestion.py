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
    get_attachments, add_label, remove_label, send_email,
    reply_to_thread,
)
from app.sheets import (
    get_next_load_id, insert_load,
    is_message_processed, mark_message_processed,
)
from app.drive import ensure_folder, upload_file, upload_text
from app.parsers import parse_load_email
from app.ai_parser import (
    classify_email, parse_with_gemini,
    check_completeness, build_missing_fields_reply,
)
from app.equipment import recommend_equipment

logger = logging.getLogger("brokerops.workflows.load_ingestion")


def run() -> list[str]:
    """Process all OPS/NEW_LOAD messages. Return list of created Load_IDs."""
    settings = get_settings()
    created: list[str] = []

    try:
        messages = search_messages("OPS/NEW_LOAD")
    except Exception as e:
        logger.error("search_messages failed: %s", e)
        messages = []
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

            logger.info("[%s] Processing new load email from %s: '%s' (body: %d chars)",
                        msg_id, from_addr, subject, len(body))
            logger.info("[%s] Body preview: %.300s", msg_id, body)

            # ── Step 1: Classify the email ──
            classification = classify_email(body, subject, from_addr)
            category = classification.get("category", "NEW_LOAD")
            confidence = classification.get("confidence", 0.0)

            if category == "CARRIER_QUOTE":
                # This is a carrier replying to an RFQ – route to quotes pipeline
                logger.info("[%s] Classified as CARRIER_QUOTE (%.0f%%) – re-labeling",
                            msg_id, confidence * 100)
                add_label(msg_id, "OPS/QUOTES_RECEIVED")
                remove_label(msg_id, "OPS/NEW_LOAD")
                mark_message_processed(msg_id, "reclassified:CARRIER_QUOTE")
                continue

            if category == "OTHER" and confidence >= 0.95:
                # High confidence it's not load-related – block it
                logger.info("[%s] Classified as OTHER (%.0f%%) – blocking",
                            msg_id, confidence * 100)
                add_label(msg_id, "OPS/BLOCKED")
                remove_label(msg_id, "OPS/NEW_LOAD")
                mark_message_processed(msg_id, "reclassified:OTHER")
                continue

            if category == "LOAD_UPDATE":
                # Follow-up on an existing load – for now, treat as new load
                # but add a note so ops knows to check for duplicates
                logger.info("[%s] Classified as LOAD_UPDATE (%.0f%%) – ingesting with note",
                            msg_id, confidence * 100)

            # Categories NEW_LOAD and LOAD_UPDATE proceed to ingestion

            # ── Step 2: Parse with both regex and Gemini AI, merge results ──
            # Regex is fast and handles well-structured emails
            regex_fields = parse_load_email(body, subject)
            key_fields = ["Origin_City", "Destination_City", "Pickup_Date",
                          "Equipment_Type", "Commodity", "Weight_Lbs"]
            regex_filled = sum(1 for f in key_fields if regex_fields.get(f))
            logger.info("[%s] Regex parser filled %d/%d key fields", msg_id, regex_filled, len(key_fields))

            # Always run Gemini for best results — it handles casual language
            logger.info("[%s] Running Gemini AI parser", msg_id)
            ai_fields = parse_with_gemini(body, subject)
            ai_filled = sum(1 for f in key_fields if ai_fields.get(f))
            logger.info("[%s] Gemini parser filled %d/%d key fields", msg_id, ai_filled, len(key_fields))

            # Merge: use whichever parser got each field, prefer Gemini for
            # fields it found since it understands context better
            fields = {}
            all_field_names = set(list(regex_fields.keys()) + list(ai_fields.keys()))
            for k in all_field_names:
                ai_val = ai_fields.get(k, "")
                regex_val = regex_fields.get(k, "")
                # Prefer Gemini value if it has one, fall back to regex
                fields[k] = ai_val if ai_val else regex_val

            final_filled = sum(1 for f in key_fields if fields.get(f))
            logger.info("[%s] Merged result: %d/%d key fields filled", msg_id, final_filled, len(key_fields))

            # ── Step 3: Equipment intelligence ──
            equip_rec = recommend_equipment(fields)
            logger.info("[%s] Equipment recommendation: %s (tier: %s, verify: %s)",
                        msg_id, equip_rec["recommended"], equip_rec["cost_tier"],
                        equip_rec["requires_verification"])

            # Apply equipment recommendation if Gemini didn't set it
            if not fields.get("Equipment_Type") and equip_rec.get("recommended_type"):
                fields["Equipment_Type"] = equip_rec["recommended_type"]
                logger.info("[%s] Applied inferred equipment: %s", msg_id, equip_rec["recommended_type"])

            # Append inferred special requirements
            existing_special = fields.get("Special_Requirements", "")
            inferred = equip_rec.get("special_requirements_inferred", [])
            if inferred:
                combined = [existing_special] if existing_special else []
                combined.extend(inferred)
                fields["Special_Requirements"] = ", ".join(combined)

            load_id = get_next_load_id()

            # Fill in system fields
            today = date.today().isoformat()
            # Extract email address from From header
            import re as _re
            email_match = _re.search(r'[\w.+-]+@[\w-]+\.[\w.]+', from_addr)
            customer_email = email_match.group(0) if email_match else from_addr
            # Always set Customer_Email from From header (Gemini often returns "" for this)
            if not fields.get("Customer_Email"):
                fields["Customer_Email"] = customer_email

            # Build internal notes with equipment intelligence
            equip_notes = []
            if equip_rec.get("warnings"):
                equip_notes.append("WARNINGS: " + "; ".join(equip_rec["warnings"]))
            if equip_rec.get("alternatives"):
                equip_notes.append("Alt trailers: " + ", ".join(equip_rec["alternatives"]))
            if equip_rec.get("notes"):
                equip_notes.append(equip_rec["notes"])

            internal_note = f"Ingested from Gmail msg {msg_id}"
            if equip_notes:
                internal_note += " | EQUIP: " + " | ".join(equip_notes)

            fields.update({
                "Load_ID": load_id,
                "Customer_Rate": "",
                "Assigned_Carrier_MC": "",
                "Load_Status": "NEW",
                "Approval_Status": "PENDING",
                "RFQ_Count": "0",
                "Created_Date": today,
                "Last_Updated": today,
                "Internal_Notes": internal_note,
            })

            # ── Step 3b: Request packing slip if verification needed ──
            if equip_rec["requires_verification"]:
                fields["Load_Status"] = "VERIFY"
                verification_note = "NEEDS VERIFICATION: " + "; ".join(equip_rec["verification_reasons"])
                fields["Internal_Notes"] = verification_note + " | " + internal_note
                logger.info("[%s] Load needs verification: %s", msg_id, equip_rec["verification_reasons"])

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

            # ── Check completeness and auto-reply if needed ──
            completeness = check_completeness(fields)
            missing_req = completeness["missing_required"]
            missing_pref = completeness["missing_preferred"]

            if missing_req:
                # Critical fields missing – create load but mark as INCOMPLETE
                fields["Load_Status"] = "INCOMPLETE"
                fields["Internal_Notes"] = (
                    f"Ingested from Gmail msg {msg_id}. "
                    f"MISSING REQUIRED: {', '.join(missing_req)}"
                )
                logger.warning("[%s] Load %s missing required fields: %s",
                               msg_id, load_id, missing_req)

                # Auto-reply disabled for now
                # reply_body = build_missing_fields_reply(missing_req, missing_pref, load_id)
                # try:
                #     reply_to_thread(
                #         thread_id=thread_id,
                #         to=from_addr,
                #         subject=subject,
                #         body_text=reply_body,
                #     )
                #     logger.info("[%s] Sent auto-reply requesting missing fields", msg_id)
                # except Exception as reply_err:
                #     logger.error("[%s] Failed to send auto-reply: %s", msg_id, reply_err)

                # Label as BLOCKED until info arrives
                add_label(msg_id, "OPS/BLOCKED")
            elif missing_pref:
                logger.info("[%s] Load %s missing preferred fields: %s (proceeding anyway)",
                            msg_id, load_id, missing_pref)

            # Swap labels
            remove_label(msg_id, "OPS/NEW_LOAD")
            # The next step (carrier sourcing) will be triggered by the polling job

            mark_message_processed(msg_id, f"load_ingestion:{load_id}")
            created.append(load_id)
            logger.info("[%s] Load %s created successfully.", msg_id, load_id)

        except Exception as exc:
            logger.exception("Failed to process message %s", msg_id)
            # Store error for diagnostics (accessible via ingest-test endpoint)
            if not hasattr(run, "_last_errors"):
                run._last_errors = []
            run._last_errors.append({"msg_id": msg_id, "error": str(exc)})

    return created
