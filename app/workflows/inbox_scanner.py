"""
Workflow: Inbox Scanner

Scans the Gmail inbox for new emails that haven't been labeled yet
and applies OPS/NEW_LOAD so the ingestion pipeline picks them up.

This replaces the need for Gmail filters, giving us full control
over which emails enter the pipeline.
"""
from __future__ import annotations

import logging

from app.config import get_settings
from app.gmail import get_gmail_service, _get_label_id, add_label
from app.sheets import is_message_processed

logger = logging.getLogger("brokerops.workflows.inbox_scanner")


def run() -> list[str]:
    """
    Scan the inbox for unlabeled emails and apply OPS/NEW_LOAD.
    Returns list of message IDs that were labeled.
    """
    svc = get_gmail_service()
    settings = get_settings()
    labeled: list[str] = []

    # Get all OPS label IDs so we can skip already-labeled emails
    ops_label_ids = set()
    all_labels = svc.users().labels().list(userId="me").execute().get("labels", [])
    for lbl in all_labels:
        if lbl["name"].startswith("OPS/"):
            ops_label_ids.add(lbl["id"])

    new_load_label_id = _get_label_id("OPS/NEW_LOAD")
    if not new_load_label_id:
        logger.error("OPS/NEW_LOAD label not found")
        return []

    # Search for recent inbox emails (last 3 days, unread or read)
    # Exclude emails sent BY us (from:me) to avoid labeling our own outbound,
    # but include emails where we are also the sender (e.g. test emails to self)
    # by checking if the message is in INBOX (sent-to-self lands in both SENT and INBOX)
    broker_email = settings.BROKER_EMAIL.lower()
    query = "in:inbox newer_than:3d"
    page_token = None
    candidates = []

    while True:
        resp = svc.users().messages().list(
            userId="me", q=query, pageToken=page_token, maxResults=50
        ).execute()
        candidates.extend(resp.get("messages", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    logger.info("Inbox scanner found %d candidate message(s)", len(candidates))

    for stub in candidates:
        msg_id = stub["id"]

        # Skip if already processed
        if is_message_processed(msg_id):
            continue

        # Fetch message metadata (lightweight – just labels and headers)
        msg = svc.users().messages().get(
            userId="me", id=msg_id, format="metadata",
            metadataHeaders=["Subject", "From", "To"]
        ).execute()

        # Skip if already has any OPS/ label
        msg_labels = set(msg.get("labelIds", []))
        if msg_labels & ops_label_ids:
            continue

        # This email has no OPS label and hasn't been processed – label it
        subject = ""
        from_addr = ""
        to_addr = ""
        for h in msg.get("payload", {}).get("headers", []):
            if h["name"] == "Subject":
                subject = h["value"]
            elif h["name"] == "From":
                from_addr = h["value"]
            elif h["name"] == "To":
                to_addr = h["value"]

        # Skip outbound emails we sent TO others (but allow send-to-self)
        if broker_email and broker_email in from_addr.lower():
            if broker_email not in to_addr.lower():
                logger.debug("Skipping outbound email %s to %s", msg_id, to_addr)
                continue

        # Skip known non-freight senders and subjects
        from_lower = from_addr.lower()
        subject_lower = subject.lower()

        # Skip automated/system emails
        skip_senders = [
            "noreply@", "no-reply@", "notifications@", "mailer-daemon@",
            "postmaster@", "github.com", "google.com", "googlemail.com",
            "calendar-notification", "drive-shares-noreply",
            "ads-noreply@", "marketing@", "newsletter@", "support@google",
        ]
        if any(skip in from_lower for skip in skip_senders):
            logger.debug("Skipping system/automated email %s from %s", msg_id, from_addr)
            continue

        # Skip bounce-back / delivery failure emails
        skip_subjects = [
            "delivery status notification", "undeliverable",
            "mail delivery failed", "returned mail",
            "out of office", "automatic reply", "auto-reply",
        ]
        if any(skip in subject_lower for skip in skip_subjects):
            logger.debug("Skipping bounce/auto-reply %s: '%s'", msg_id, subject)
            continue

        logger.info("Auto-labeling message %s from %s: '%s'", msg_id, from_addr, subject)
        add_label(msg_id, "OPS/NEW_LOAD")
        labeled.append(msg_id)

    logger.info("Inbox scanner labeled %d new message(s)", len(labeled))
    return labeled
