"""
BrokerOps AI — Cloud Storage reply draft store.

Stores carrier reply draft JSON in GCS so the reply approval flow
can read and update draft state across Cloud Run requests.

Bucket:  gs://wide-decoder-489023-p1-brokerops
Prefix:  reply_drafts/
Objects: {draft_id}.json

Authentication via Cloud Run workload identity (brokerops-gmail SA).
"""
from __future__ import annotations

import json
import logging
import time
from typing import Optional

from google.cloud import storage

logger = logging.getLogger("brokerops.reply_draft_store")

BUCKET = "wide-decoder-489023-p1-brokerops"
PREFIX = "reply_drafts"


def store_reply_draft(draft_id: str, draft_data: dict) -> str:
    """Write reply draft JSON to GCS. Returns gs:// URI."""
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(f"{PREFIX}/{draft_id}.json")
    blob.upload_from_string(json.dumps(draft_data), content_type="application/json")
    uri = f"gs://{BUCKET}/{PREFIX}/{draft_id}.json"
    logger.info("Stored reply draft draft_id=%s at %s", draft_id, uri)
    return uri


def read_reply_draft(draft_id: str) -> Optional[dict]:
    """Read reply draft JSON from GCS. Returns None if not found."""
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(f"{PREFIX}/{draft_id}.json")
    if not blob.exists():
        logger.warning("Reply draft not found in GCS: draft_id=%s", draft_id)
        return None
    return json.loads(blob.download_as_string())


def mark_draft_used(draft_id: str, sent: bool = False) -> None:
    """Mark a draft as used (sent or discarded) to prevent double-sends."""
    data = read_reply_draft(draft_id)
    if not data:
        logger.warning("mark_draft_used: draft_id=%s not found in GCS", draft_id)
        return
    data["used"] = True
    data["used_at"] = time.time()
    data["sent"] = sent
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(f"{PREFIX}/{draft_id}.json")
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    logger.info("Marked reply draft used: draft_id=%s sent=%s", draft_id, sent)
