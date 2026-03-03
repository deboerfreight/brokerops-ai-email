"""
BrokerOps AI – Gmail helpers: read, send, label, search.
"""
from __future__ import annotations

import base64
import logging
import re
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import Any, Optional

from app.config import get_settings
from app.google_auth import get_gmail_service

logger = logging.getLogger("brokerops.gmail")


# ── Label management ─────────────────────────────────────────────────────────

_label_cache: dict[str, str] = {}


def _get_label_id(label_name: str) -> Optional[str]:
    """Resolve a label name like 'OPS/NEW_LOAD' to its Gmail label ID."""
    if label_name in _label_cache:
        return _label_cache[label_name]
    svc = get_gmail_service()
    resp = svc.users().labels().list(userId="me").execute()
    for lbl in resp.get("labels", []):
        _label_cache[lbl["name"]] = lbl["id"]
    return _label_cache.get(label_name)


def add_label(message_id: str, label_name: str) -> None:
    label_id = _get_label_id(label_name)
    if not label_id:
        logger.warning("Label '%s' not found.", label_name)
        return
    get_gmail_service().users().messages().modify(
        userId="me", id=message_id,
        body={"addLabelIds": [label_id]}
    ).execute()
    logger.info("Added label '%s' to message %s", label_name, message_id)


def remove_label(message_id: str, label_name: str) -> None:
    label_id = _get_label_id(label_name)
    if not label_id:
        return
    get_gmail_service().users().messages().modify(
        userId="me", id=message_id,
        body={"removeLabelIds": [label_id]}
    ).execute()
    logger.info("Removed label '%s' from message %s", label_name, message_id)


# ── Searching / fetching ────────────────────────────────────────────────────

def search_messages(label_name: str, query: str = "") -> list[dict]:
    """Return list of message stubs matching a Gmail label + optional query."""
    logger.info("search_messages called: label_name='%s', query='%s'", label_name, query)
    label_id = _get_label_id(label_name)
    if not label_id:
        logger.warning("Label '%s' not found – returning empty list. Cache keys: %s",
                        label_name, list(_label_cache.keys()))
        return []
    logger.info("Resolved label '%s' → id '%s'", label_name, label_id)
    svc = get_gmail_service()
    results: list[dict] = []
    page_token = None
    while True:
        resp = svc.users().messages().list(
            userId="me", labelIds=[label_id], q=query, pageToken=page_token
        ).execute()
        batch = resp.get("messages", [])
        logger.info("messages.list returned %d message(s) (resultSizeEstimate=%s)",
                     len(batch), resp.get("resultSizeEstimate"))
        results.extend(batch)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    logger.info("search_messages total: %d message(s) for label '%s'", len(results), label_name)
    return results


def get_message(message_id: str) -> dict:
    """Fetch full message payload."""
    return get_gmail_service().users().messages().get(
        userId="me", id=message_id, format="full"
    ).execute()


def get_thread(thread_id: str) -> dict:
    """Fetch an entire thread."""
    return get_gmail_service().users().threads().get(
        userId="me", id=thread_id, format="full"
    ).execute()


def get_header(msg: dict, name: str) -> str:
    """Extract a header value from a message payload."""
    for h in msg.get("payload", {}).get("headers", []):
        if h["name"].lower() == name.lower():
            return h["value"]
    return ""


def get_body_text(msg: dict) -> str:
    """Best-effort extraction of the plain-text body."""
    payload = msg.get("payload", {})
    # Simple single-part
    if payload.get("mimeType") == "text/plain" and payload.get("body", {}).get("data"):
        return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="replace")
    # Multipart
    for part in payload.get("parts", []):
        if part.get("mimeType") == "text/plain" and part.get("body", {}).get("data"):
            return base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
        # Nested multipart
        for sub in part.get("parts", []):
            if sub.get("mimeType") == "text/plain" and sub.get("body", {}).get("data"):
                return base64.urlsafe_b64decode(sub["body"]["data"]).decode("utf-8", errors="replace")
    return ""


def get_attachments(message_id: str, msg: dict) -> list[dict]:
    """Return list of {filename, data_bytes} for each attachment."""
    results = []
    payload = msg.get("payload", {})
    parts = payload.get("parts", [])
    for part in parts:
        filename = part.get("filename")
        if filename and part.get("body", {}).get("attachmentId"):
            att = get_gmail_service().users().messages().attachments().get(
                userId="me", messageId=message_id, id=part["body"]["attachmentId"]
            ).execute()
            data = base64.urlsafe_b64decode(att["data"])
            results.append({"filename": filename, "data": data, "mime_type": part.get("mimeType", "application/octet-stream")})
    return results


# ── Sending ──────────────────────────────────────────────────────────────────

def send_email(
    to: str,
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
    thread_id: Optional[str] = None,
    attachments: Optional[list[dict]] = None,
) -> dict:
    """Send an email (plain + optional HTML). Returns sent message metadata."""
    if body_html or attachments:
        msg = MIMEMultipart("mixed")
        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(body_text, "plain"))
        if body_html:
            alt.attach(MIMEText(body_html, "html"))
        msg.attach(alt)
        for att in (attachments or []):
            part = MIMEBase("application", "octet-stream")
            part.set_payload(att["data"])
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{att["filename"]}"')
            msg.attach(part)
    else:
        msg = MIMEText(body_text, "plain")

    settings = get_settings()
    msg["to"] = to
    msg["from"] = settings.BROKER_EMAIL
    msg["subject"] = subject

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("ascii")
    body: dict[str, Any] = {"raw": raw}
    if thread_id:
        body["threadId"] = thread_id

    sent = get_gmail_service().users().messages().send(userId="me", body=body).execute()
    logger.info("Sent email to %s, subject='%s', id=%s", to, subject, sent.get("id"))
    return sent


def reply_to_thread(thread_id: str, to: str, subject: str, body_text: str) -> dict:
    """Send a reply within an existing thread."""
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"
    return send_email(to=to, subject=subject, body_text=body_text, thread_id=thread_id)
