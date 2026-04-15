"""
BrokerOps AI — Cloud Storage pending batch store.

Stores carrier outreach batch JSON in GCS so the mobile approval flow
can read and update batch state across Cloud Run requests (which have
no persistent local filesystem).

Bucket:  gs://wide-decoder-489023-p1-brokerops
Prefix:  pending_batches/
Objects: {batch_id}.json

Authentication is automatic via Cloud Run runtime identity
(brokerops-gmail SA). SA must have roles/storage.objectAdmin on the bucket.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Optional

from google.cloud import storage

logger = logging.getLogger("brokerops.pending_batch_store")

BUCKET = "wide-decoder-489023-p1-brokerops"
PREFIX = "pending_batches"


def store_pending_batch(batch_id: str, batch_data: dict) -> str:
    """
    Write batch JSON to GCS.

    Args:
        batch_id:   UUID4 string identifying the batch (also used as filename).
        batch_data: Serializable dict containing carrier list + metadata.

    Returns:
        gs:// URI of the stored object.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(f"{PREFIX}/{batch_id}.json")
    blob.upload_from_string(json.dumps(batch_data), content_type="application/json")
    uri = f"gs://{BUCKET}/{PREFIX}/{batch_id}.json"
    logger.info("Stored pending batch batch_id=%s at %s", batch_id, uri)
    return uri


def read_pending_batch(batch_id: str) -> Optional[dict]:
    """
    Read batch JSON from GCS.

    Returns:
        Parsed dict, or None if the object does not exist.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(f"{PREFIX}/{batch_id}.json")
    if not blob.exists():
        logger.warning("Pending batch not found in GCS: batch_id=%s", batch_id)
        return None
    data = json.loads(blob.download_as_string())
    return data


def mark_batch_used(batch_id: str) -> None:
    """
    Mark a batch as used in GCS to prevent double-sends on re-tap.

    This MUST be called BEFORE the send loop starts. If the send loop is
    interrupted, the batch is still marked used — re-tapping the approve
    link will return "already approved" rather than firing a second send.
    """
    data = read_pending_batch(batch_id)
    if not data:
        logger.warning("mark_batch_used: batch_id=%s not found in GCS — cannot mark used", batch_id)
        return
    data["used"] = True
    data["used_at"] = time.time()
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(f"{PREFIX}/{batch_id}.json")
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    logger.info("Marked batch used: batch_id=%s", batch_id)
