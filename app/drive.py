"""
BrokerOps AI – Google Drive helpers: folders, file upload, template copy, PDF export.
"""
from __future__ import annotations

import io
import logging
from typing import Optional

from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

from app.config import get_settings
from app.google_auth import get_drive_service

logger = logging.getLogger("brokerops.drive")


def _svc():
    return get_drive_service()


# ── Folder management ────────────────────────────────────────────────────────

def create_folder(name: str, parent_id: str) -> str:
    """Create a folder in Drive and return its ID."""
    meta = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = _svc().files().create(body=meta, fields="id").execute()
    logger.info("Created Drive folder '%s' (id=%s) under %s", name, folder["id"], parent_id)
    return folder["id"]


def find_folder(name: str, parent_id: str) -> Optional[str]:
    """Find a folder by name under a parent; return its ID or None."""
    q = (
        f"name='{name}' and '{parent_id}' in parents "
        "and mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    resp = _svc().files().list(q=q, fields="files(id)", spaces="drive").execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def ensure_folder(name: str, parent_id: str) -> str:
    """Find or create a folder under parent."""
    existing = find_folder(name, parent_id)
    if existing:
        return existing
    return create_folder(name, parent_id)


# ── File upload ──────────────────────────────────────────────────────────────

def upload_file(
    name: str,
    data: bytes,
    mime_type: str,
    parent_id: str,
) -> str:
    """Upload a file to Drive and return its ID."""
    meta = {"name": name, "parents": [parent_id]}
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type, resumable=True)
    f = _svc().files().create(body=meta, media_body=media, fields="id").execute()
    logger.info("Uploaded '%s' to Drive folder %s (id=%s)", name, parent_id, f["id"])
    return f["id"]


def upload_text(name: str, text: str, parent_id: str) -> str:
    """Upload a plain text file."""
    return upload_file(name, text.encode("utf-8"), "text/plain", parent_id)


# ── Template copy & placeholder replacement ──────────────────────────────────

def copy_template(template_id: str, new_name: str, destination_folder_id: str) -> str:
    """Copy a Google Doc template into a destination folder. Return new doc ID."""
    body = {"name": new_name, "parents": [destination_folder_id]}
    copy = _svc().files().copy(fileId=template_id, body=body, fields="id").execute()
    logger.info("Copied template %s → '%s' (id=%s)", template_id, new_name, copy["id"])
    return copy["id"]


def replace_placeholders(doc_id: str, replacements: dict[str, str]) -> None:
    """
    Replace {{placeholder}} tokens in a Google Doc using the Docs API.
    We import the Docs service lazily since it's only needed here.
    """
    from app.google_auth import get_credentials
    from googleapiclient.discovery import build

    docs_svc = build("docs", "v1", credentials=get_credentials())
    requests = []
    for placeholder, value in replacements.items():
        requests.append({
            "replaceAllText": {
                "containsText": {"text": placeholder, "matchCase": True},
                "replaceText": value,
            }
        })
    if requests:
        docs_svc.documents().batchUpdate(
            documentId=doc_id, body={"requests": requests}
        ).execute()
        logger.info("Replaced %d placeholders in doc %s", len(requests), doc_id)


# ── PDF export ───────────────────────────────────────────────────────────────

def export_as_pdf(doc_id: str) -> bytes:
    """Export a Google Doc as PDF bytes."""
    request = _svc().files().export_media(fileId=doc_id, mimeType="application/pdf")
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    logger.info("Exported doc %s as PDF (%d bytes)", doc_id, buf.tell())
    return buf.getvalue()
