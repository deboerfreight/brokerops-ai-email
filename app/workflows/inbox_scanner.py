"""
Workflow: Inbox Scanner

Scans the Gmail inbox for new emails that haven't been labeled yet.
Classifies each email and applies the correct OPS/ label:
  - OPS/OUTREACH_REPLY — carrier replying to our outreach (matched by sender email in Carrier_Master)
  - OPS/NEW_LOAD — everything else that looks like freight business

This replaces the need for Gmail filters, giving us full control
over which emails enter the pipeline.
"""
from __future__ import annotations

import logging
import re

from app.config import get_settings
from app.gmail import get_gmail_service, _get_label_id, add_label
from app.sheets import is_message_processed, get_all_carriers

logger = logging.getLogger("brokerops.workflows.inbox_scanner")


def _extract_sender_email(from_header: str) -> str:
    """Extract email from 'Name <email@example.com>' format."""
    m = re.search(r"<(.+?)>", from_header)
    return m.group(1).lower() if m else from_header.strip().lower()


def _build_carrier_email_set() -> set[str]:
    """Build a set of all carrier emails from Carrier_Master for fast lookup."""
    emails = set()
    for c in get_all_carriers():
        email = c.get("Primary_Email", "").strip().lower()
        if email:
            emails.add(email)
    return emails


def _ensure_label(label_name: str) -> str | None:
    """Get label ID, creating it if it doesn't exist."""
    label_id = _get_label_id(label_name)
    if label_id:
        return label_id

    # Create the label
    svc = get_gmail_service()
    try:
        result = svc.users().labels().create(
            userId="me",
            body={"name": label_name, "labelListVisibility": "labelShow",
                  "messageListVisibility": "show"}
        ).execute()
        from app.gmail import _label_cache
        _label_cache[label_name] = result["id"]
        logger.info("Created Gmail label '%s' (id=%s)", label_name, result["id"])
        return result["id"]
    except Exception as e:
        logger.error("Failed to create label '%s': %s", label_name, e)
        return None


def run() -> list[str]:
    """
    Scan the inbox for unlabeled emails, classify, and apply the right OPS/ label.
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

    # Ensure OPS/OUTREACH_REPLY label exists
    outreach_reply_label_id = _ensure_label("OPS/OUTREACH_REPLY")

    # Build carrier email lookup for outreach reply detection
    carrier_emails = _build_carrier_email_set()
    logger.info("Loaded %d carrier email(s) for outreach reply detection.", len(carrier_emails))

    # Search for recent inbox emails (last 3 days, unread or read)
    # Exclude emails sent BY us (from:me) to avoid labeling our own outbound
    query = "in:inbox newer_than:3d -from:me"
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
            metadataHeaders=["Subject", "From"]
        ).execute()

        # Skip if already has any OPS/ label
        msg_labels = set(msg.get("labelIds", []))
        if msg_labels & ops_label_ids:
            continue

        # This email has no OPS label and hasn't been processed – classify it
        subject = ""
        from_addr = ""
        for h in msg.get("payload", {}).get("headers", []):
            if h["name"] == "Subject":
                subject = h["value"]
            elif h["name"] == "From":
                from_addr = h["value"]

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

        # ── Classification: carrier outreach reply vs new load ──────────
        sender_email = _extract_sender_email(from_addr)

        # Check if sender is a known carrier AND this looks like a reply
        # (not a load-specific RFQ — those have "RFQ |" in subject and get
        # labeled OPS/RFQ_SENT by carrier_sourcing)
        is_carrier = sender_email in carrier_emails
        is_rfq_thread = "rfq |" in subject_lower or "rfq|" in subject_lower
        is_reply = subject_lower.startswith("re:") or subject_lower.startswith("re :")

        if is_carrier and not is_rfq_thread and outreach_reply_label_id:
            logger.info("Carrier outreach reply detected: %s from %s: '%s'",
                         msg_id, from_addr, subject)
            add_label(msg_id, "OPS/OUTREACH_REPLY")
            labeled.append(msg_id)
        else:
            logger.info("Auto-labeling as NEW_LOAD: %s from %s: '%s'",
                         msg_id, from_addr, subject)
            add_label(msg_id, "OPS/NEW_LOAD")
            labeled.append(msg_id)

    logger.info("Inbox scanner labeled %d new message(s)", len(labeled))
    return labeled
