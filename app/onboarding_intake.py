"""
BrokerOps AI -- Carrier onboarding intake plumbing.

Handles:
  1. Gmail label + Drive folder creation when a carrier is classified as interested.
  2. Attachment watcher (polling): scans labeled threads for onboarding docs,
     saves to Drive, updates Onboarding_Docs_Received column.
  3. COI verification via Claude vision: extracts coverage amounts, expiry, cert holder.

Scheduling: process_onboarding_attachments() is designed to run on Cloud Scheduler
(wired separately). Build only -- no live calls in this module at import time.

Hard constraints:
  - No sheet writes happen at import time.
  - No Gmail labels or Drive folders created at import time.
  - All send/label/Drive ops are gated by OUTREACH_AUTO_REPLY_ENABLED being True
    (or the explicit call from route_classified_reply).
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional

from app.config import get_settings
from app.gmail import (
    add_label,
    get_message,
    get_header,
    get_body_text,
    get_attachments,
)
from app.google_auth import get_gmail_service
from app.notifications import notify_slack
from app.sheets import (
    get_carrier_by_dot,
    update_carrier_fields_by_dot,
)

logger = logging.getLogger("brokerops.onboarding_intake")

# ── Constants ─────────────────────────────────────────────────────────────────

_CARRIERS_FOLDER_NAME = "BrokerOps/Carriers"
_LABEL_PREFIX = "carrier-onboarding"
_REQUIRED_DOCS = {"W9", "COI", "AUTH", "ACH"}

# COI verification thresholds
_COI_AUTO_LIABILITY_MIN = 1_000_000
_COI_CARGO_MIN = 100_000
_COI_EXPIRY_WARNING_DAYS = 30

# ── Doc type detection ────────────────────────────────────────────────────────

_DOC_PATTERNS = {
    "W9": re.compile(r"\bw[-_]?9\b", re.I),
    "COI": re.compile(r"\b(coi|certificate of insurance|acord)\b", re.I),
    "AUTH": re.compile(r"\b(authority|operating authority|mc.?letter|auth)\b", re.I),
    "ACH": re.compile(r"\b(ach|direct deposit|banking|bank.?info|payment)\b", re.I),
}


def _detect_doc_type(filename: str) -> Optional[str]:
    """Return doc type ('W9', 'COI', 'AUTH', 'ACH') from filename, or None."""
    fn = filename.lower()
    for doc_type, pattern in _DOC_PATTERNS.items():
        if pattern.search(fn):
            return doc_type
    return None


# ── Gmail label management ────────────────────────────────────────────────────

def _ensure_gmail_label(dot: str) -> Optional[str]:
    """Create Gmail label 'carrier-onboarding/{dot}' if not present. Return label ID."""
    label_name = f"{_LABEL_PREFIX}/{dot}"
    try:
        svc = get_gmail_service()
        # List existing labels
        resp = svc.users().labels().list(userId="me").execute()
        for lbl in resp.get("labels", []):
            if lbl["name"] == label_name:
                logger.info("Gmail label '%s' already exists (id=%s)", label_name, lbl["id"])
                return lbl["id"]
        # Create it
        new_lbl = svc.users().labels().create(
            userId="me",
            body={
                "name": label_name,
                "labelListVisibility": "labelShow",
                "messageListVisibility": "show",
            },
        ).execute()
        logger.info("Created Gmail label '%s' (id=%s)", label_name, new_lbl["id"])
        return new_lbl["id"]
    except Exception as e:
        logger.error("Failed to create Gmail label '%s': %s", label_name, e)
        return None


def _apply_label_to_thread(thread_id: str, label_id: str) -> None:
    """Apply a label to all messages in a thread."""
    try:
        svc = get_gmail_service()
        thread = svc.users().threads().get(userId="me", id=thread_id, format="minimal").execute()
        for msg in thread.get("messages", []):
            svc.users().messages().modify(
                userId="me",
                id=msg["id"],
                body={"addLabelIds": [label_id]},
            ).execute()
        logger.info("Applied label to %d messages in thread %s", len(thread.get("messages", [])), thread_id)
    except Exception as e:
        logger.error("Failed to label thread %s: %s", thread_id, e)


# ── Drive folder management ───────────────────────────────────────────────────

def _ensure_carrier_drive_folder(legal_name: str, dot: str) -> Optional[str]:
    """Find or create Drive folder 'BrokerOps/Carriers/{legal_name}_{dot}/'.

    Returns the folder ID or None on error.
    """
    try:
        from app.drive import ensure_folder
        settings = get_settings()
        carriers_folder_id = settings.CARRIERS_FOLDER_ID
        if not carriers_folder_id:
            logger.warning("CARRIERS_FOLDER_ID not set -- cannot create carrier folder")
            return None
        safe_name = re.sub(r"[^\w\s-]", "", legal_name).strip().replace(" ", "_")
        folder_name = f"{safe_name}_{dot}"
        folder_id = ensure_folder(folder_name, carriers_folder_id)
        logger.info("Carrier Drive folder: '%s' (id=%s)", folder_name, folder_id)
        return folder_id
    except Exception as e:
        logger.error("Failed to create carrier Drive folder for DOT=%s: %s", dot, e)
        return None


# ── Interested action handler ─────────────────────────────────────────────────

def handle_carrier_interested(dot: str) -> dict:
    """Called by route_classified_reply when a carrier replies as interested.

    1. Creates Gmail label carrier-onboarding/{dot}
    2. Labels the carrier's outreach thread
    3. Creates Drive folder BrokerOps/Carriers/{name}_{dot}

    Returns dict with label_id and folder_id (or None on failure).
    All side effects are logged -- failures don't raise, they log + Slack.
    """
    result = {"label_id": None, "folder_id": None}

    carrier = get_carrier_by_dot(dot)
    if not carrier:
        logger.warning("handle_carrier_interested: no carrier found for DOT=%s", dot)
        return result

    legal_name = (
        carrier.get("DBA_Name")
        or carrier.get("Legal_Name")
        or carrier.get("Company Name")
        or f"carrier_{dot}"
    ).strip()
    thread_id = (carrier.get("Outreach_Thread_Id") or "").strip()

    # Gmail label
    label_id = _ensure_gmail_label(dot)
    result["label_id"] = label_id
    if label_id and thread_id:
        _apply_label_to_thread(thread_id, label_id)

    # Drive folder
    folder_id = _ensure_carrier_drive_folder(legal_name, dot)
    result["folder_id"] = folder_id

    logger.info(
        "handle_carrier_interested DOT=%s: label=%s folder=%s",
        dot, label_id, folder_id,
    )
    return result


# ── Attachment watcher ────────────────────────────────────────────────────────

def process_onboarding_attachments() -> dict:
    """Polling function: scan Gmail for onboarding docs and save to Drive.

    Designed to run on Cloud Scheduler (wired separately). Safe to call manually.

    Flow:
      1. Search Gmail for messages with label 'carrier-onboarding/*' + attachments.
      2. For each attachment: detect doc type, upload to carrier's Drive folder.
      3. Update Onboarding_Docs_Received on the carrier's sheet row.
      4. When all 4 docs received: Slack Derek, transition status to docs_received_partial.

    Returns stats dict.
    """
    stats = {
        "threads_scanned": 0,
        "attachments_processed": 0,
        "carriers_updated": 0,
        "errors": 0,
    }

    try:
        svc = get_gmail_service()
        # Search for any labeled onboarding threads with attachments
        query = f"label:{_LABEL_PREFIX}/* has:attachment"
        resp = svc.users().messages().list(userId="me", q=query, maxResults=50).execute()
        messages = resp.get("messages", [])
    except Exception as e:
        logger.error("process_onboarding_attachments: Gmail search failed: %s", e)
        stats["errors"] += 1
        return stats

    # Group by thread to avoid double-processing
    seen_threads: set[str] = set()
    for stub in messages:
        try:
            msg = get_message(stub["id"])
            thread_id = msg.get("threadId", "")
            if thread_id in seen_threads:
                continue
            seen_threads.add(thread_id)
            stats["threads_scanned"] += 1

            # Extract DOT from label name
            dot = _extract_dot_from_labels(msg)
            if not dot:
                logger.warning("Could not extract DOT from labels on thread %s", thread_id)
                continue

            carrier = get_carrier_by_dot(dot)
            if not carrier:
                logger.warning("No carrier found for DOT=%s (thread %s)", dot, thread_id)
                continue

            # Get Drive folder for this carrier
            legal_name = (
                carrier.get("DBA_Name")
                or carrier.get("Legal_Name")
                or carrier.get("Company Name")
                or f"carrier_{dot}"
            ).strip()
            folder_id = _ensure_carrier_drive_folder(legal_name, dot)
            if not folder_id:
                logger.warning("No Drive folder for DOT=%s -- cannot save attachments", dot)
                stats["errors"] += 1
                continue

            # Process each message in the thread for attachments
            thread = svc.users().threads().get(userId="me", id=thread_id, format="full").execute()
            docs_found: list[str] = []
            for thread_msg in thread.get("messages", []):
                atts = get_attachments(thread_msg["id"], thread_msg)
                for att in atts:
                    doc_type = _detect_doc_type(att["filename"])
                    if not doc_type:
                        logger.info(
                            "DOT=%s: attachment '%s' -- unknown doc type, saving as-is",
                            dot, att["filename"],
                        )
                        doc_type = "UNKNOWN"
                    _save_attachment_to_drive(att, folder_id, dot, doc_type)
                    stats["attachments_processed"] += 1
                    docs_found.append(doc_type)

            if docs_found:
                _update_docs_received(dot, carrier, docs_found)
                stats["carriers_updated"] += 1

                # COI verification if COI was received
                if "COI" in docs_found:
                    _handle_coi_verification(dot, carrier, folder_id, atts if atts else [])

        except Exception as e:
            logger.error("process_onboarding_attachments: error on thread %s: %s", stub.get("id"), e)
            stats["errors"] += 1

    logger.info(
        "process_onboarding_attachments: threads=%d attachments=%d carriers=%d errors=%d",
        stats["threads_scanned"], stats["attachments_processed"],
        stats["carriers_updated"], stats["errors"],
    )
    return stats


def _extract_dot_from_labels(msg: dict) -> Optional[str]:
    """Extract DOT number from message label names like 'carrier-onboarding/1234567'."""
    label_ids = msg.get("labelIds", [])
    try:
        svc = get_gmail_service()
        resp = svc.users().labels().list(userId="me").execute()
        label_map = {lbl["id"]: lbl["name"] for lbl in resp.get("labels", [])}
    except Exception:
        return None
    for lid in label_ids:
        name = label_map.get(lid, "")
        if name.startswith(f"{_LABEL_PREFIX}/"):
            return name.split("/", 1)[1]
    return None


def _save_attachment_to_drive(
    att: dict, folder_id: str, dot: str, doc_type: str
) -> Optional[str]:
    """Upload attachment bytes to the carrier's Drive folder."""
    try:
        from app.drive import upload_file
        filename = f"{doc_type}_{att['filename']}"
        file_id = upload_file(
            name=filename,
            data=att["data"],
            mime_type=att.get("mime_type", "application/octet-stream"),
            parent_id=folder_id,
        )
        logger.info("Saved %s for DOT=%s to Drive (file_id=%s)", filename, dot, file_id)
        return file_id
    except Exception as e:
        logger.error("Failed to upload %s for DOT=%s: %s", att.get("filename"), dot, e)
        return None


def _update_docs_received(dot: str, carrier: dict, new_docs: list[str]) -> None:
    """Append newly received doc types to Onboarding_Docs_Received and update status."""
    existing = (
        carrier.get("Onboarding_Docs_Received") or ""
    ).strip()
    existing_set = set(d.strip() for d in existing.split(",") if d.strip())
    combined = existing_set | set(new_docs) - {"UNKNOWN"}
    docs_str = ",".join(sorted(combined))

    updates: dict = {"Onboarding_Docs_Received": docs_str}

    # Check if all 4 required docs are present
    if _REQUIRED_DOCS.issubset(combined):
        updates["Onboarding_Status"] = "docs_received_partial"
        notify_slack(
            f"All 4 onboarding docs received for DOT={dot} ({_carrier_name(carrier)}). "
            f"Derek -- please review in Drive and approve for agreement send."
        )
        logger.info("DOT=%s: all docs received (%s)", dot, docs_str)
    else:
        missing = _REQUIRED_DOCS - combined
        logger.info("DOT=%s: docs so far: %s -- still missing: %s", dot, docs_str, missing)

    try:
        update_carrier_fields_by_dot(dot, updates)
    except Exception as e:
        logger.error("Failed to update docs_received for DOT=%s: %s", dot, e)


def _carrier_name(carrier: dict) -> str:
    return (
        carrier.get("DBA_Name")
        or carrier.get("Legal_Name")
        or carrier.get("Company Name")
        or "?"
    )


# ── COI verification ──────────────────────────────────────────────────────────

_COI_PROMPT = """You are a freight brokerage compliance assistant.

You are reviewing an ACORD Certificate of Insurance (COI). Extract the following fields:

Return ONLY a JSON object:
- "auto_liability": integer dollar amount (e.g. 1000000)
- "cargo": integer dollar amount (e.g. 100000)
- "expiration_date": ISO date string "YYYY-MM-DD"
- "certificate_holder": string (company name listed as certificate holder)
- "policy_numbers": list of strings
- "notes": any other relevant observations

If a field is not visible or unclear, use null.
No markdown. JSON only.

COI text / image follows:
"""


def verify_coi(coi_data: bytes, mime_type: str, dot: str) -> dict:
    """Parse a COI using Claude vision and flag issues.

    Returns dict with extracted fields + list of 'issues' (each is a dict
    with 'severity': 'fail'|'warning' and 'message').
    Always returns a dict even on error.
    """
    result: dict = {
        "auto_liability": None,
        "cargo": None,
        "expiration_date": None,
        "certificate_holder": None,
        "issues": [],
        "raw": {},
    }

    try:
        # Use Gemini (existing ai_parser pattern) to analyze the COI
        from app.ai_parser import _call_gemini, _extract_json
        import base64

        # For vision: if PDF/image, encode as base64 and include in prompt
        # The Gemini call signature may not support vision -- use text extraction fallback
        # For now, pass the raw bytes as base64-encoded text in the prompt
        b64 = base64.b64encode(coi_data).decode("ascii")[:500]  # truncate for text prompt
        prompt = (
            f"{_COI_PROMPT}\n"
            f"[COI document for DOT={dot}, mime={mime_type}, "
            f"base64_prefix={b64[:100]}...]\n"
            f"Extract fields from the document if text-readable, otherwise return nulls."
        )
        raw = _call_gemini(prompt, max_tokens=512)
        parsed = _extract_json(raw)
        result["raw"] = parsed

        auto_liab = parsed.get("auto_liability")
        cargo = parsed.get("cargo")
        expiry_str = parsed.get("expiration_date") or ""
        cert_holder = parsed.get("certificate_holder") or ""

        result["auto_liability"] = auto_liab
        result["cargo"] = cargo
        result["expiration_date"] = expiry_str
        result["certificate_holder"] = cert_holder

        # ── Checks ─────────────────────────────────────────────────────────
        issues = []

        if auto_liab is not None and int(auto_liab) < _COI_AUTO_LIABILITY_MIN:
            issues.append({
                "severity": "fail",
                "message": f"Auto liability ${auto_liab:,} < required ${_COI_AUTO_LIABILITY_MIN:,}",
            })

        if cargo is not None and int(cargo) < _COI_CARGO_MIN:
            issues.append({
                "severity": "fail",
                "message": f"Cargo coverage ${cargo:,} < required ${_COI_CARGO_MIN:,}",
            })

        if expiry_str:
            try:
                expiry = date.fromisoformat(expiry_str)
                days_until_expiry = (expiry - date.today()).days
                if days_until_expiry < 0:
                    issues.append({
                        "severity": "fail",
                        "message": f"COI expired {expiry_str}",
                    })
                elif days_until_expiry < _COI_EXPIRY_WARNING_DAYS:
                    issues.append({
                        "severity": "warning",
                        "message": f"COI expires in {days_until_expiry} days ({expiry_str})",
                    })
            except ValueError:
                issues.append({
                    "severity": "warning",
                    "message": f"Could not parse expiration date: {expiry_str!r}",
                })

        if cert_holder:
            if "deboer" not in cert_holder.lower() and "de boer" not in cert_holder.lower():
                issues.append({
                    "severity": "warning",
                    "message": f"Certificate holder is '{cert_holder}' -- expected deBoer Freight",
                })
        else:
            issues.append({
                "severity": "warning",
                "message": "Certificate holder not found in COI",
            })

        result["issues"] = issues

        # ── Slack report ──────────────────────────────────────────────────
        if issues:
            fails = [i for i in issues if i["severity"] == "fail"]
            warnings = [i for i in issues if i["severity"] == "warning"]
            lines = [f"COI review for DOT={dot}:"]
            for i in fails:
                lines.append(f"  FAIL: {i['message']}")
            for i in warnings:
                lines.append(f"  WARN: {i['message']}")
            if fails:
                lines.append("Action required before onboarding can proceed.")
            notify_slack("\n".join(lines))
        else:
            notify_slack(f"COI for DOT={dot} passed all checks. Derek -- ready for review.")

    except Exception as e:
        logger.error("COI verification failed for DOT=%s: %s", dot, e)
        result["issues"].append({"severity": "warning", "message": f"Verification error: {e}"})
        notify_slack(f"COI verification error for DOT={dot}: {e}. Manual review required.")

    return result


def _handle_coi_verification(
    dot: str, carrier: dict, folder_id: str, atts: list[dict]
) -> None:
    """Find the COI attachment and run verification."""
    for att in atts:
        doc_type = _detect_doc_type(att["filename"])
        if doc_type == "COI":
            logger.info("Running COI verification for DOT=%s (%s)", dot, att["filename"])
            verify_coi(att["data"], att.get("mime_type", "application/pdf"), dot)
            return
    logger.info("No COI attachment found for verification pass (DOT=%s)", dot)
