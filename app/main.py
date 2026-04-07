"""
BrokerOps AI – FastAPI application.

Endpoints:
  GET  /health              – Health check
  GET  /oauth/start         – Start OAuth2 flow
  GET  /oauth2callback      – OAuth2 callback
  POST /jobs/poll           – Main polling job (called by Cloud Scheduler)
  POST /jobs/compliance     – Run compliance sync for active-load carriers
"""
from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import RedirectResponse, JSONResponse

from app.config import get_settings, Settings
from app.google_auth import build_oauth_flow, exchange_code

# ── Logging setup ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger("brokerops.main")

# ── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="BrokerOps AI",
    description="Internal freight brokerage automation service",
    version="0.1.0",
)


def get_config() -> Settings:
    return get_settings()


# ── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "brokerops-ai"}


# ── OAuth flow ───────────────────────────────────────────────────────────────

@app.get("/oauth/start")
def oauth_start(config: Settings = Depends(get_config)):
    redirect_uri = f"{config.SERVICE_URL}/oauth2callback"
    flow = build_oauth_flow(redirect_uri)
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    return RedirectResponse(auth_url)


@app.get("/oauth2callback")
def oauth_callback(code: str, config: Settings = Depends(get_config)):
    redirect_uri = f"{config.SERVICE_URL}/oauth2callback"
    try:
        creds = exchange_code(code, redirect_uri)
        return {"status": "authenticated", "scopes": creds.scopes}
    except Exception as exc:
        logger.exception("OAuth callback failed")
        raise HTTPException(status_code=500, detail=str(exc))


# ── Main polling job ─────────────────────────────────────────────────────────

@app.post("/jobs/poll")
def poll_job():
    """
    Single endpoint that runs the full processing cycle:
      1. Ingest new loads (OPS/NEW_LOAD)
      2. Source carriers & send RFQs for NEW loads
      3. Expand RFQs for stale RFQ_SENT loads
      4. Parse quotes and select carriers
      5. Send onboarding requests
      6. Check onboarding document submissions
      7. Send approval packets
      8. Check approval replies
      9. Generate & send rate confirmations
      10. Run compliance sync for active-load carriers
    """
    start = time.time()
    report: dict[str, Any] = {}

    try:
        # 0. Inbox scanner – auto-label new emails before ingestion
        from app.workflows.inbox_scanner import run as scanner_run
        auto_labeled = scanner_run()
        report["auto_labeled"] = auto_labeled
        logger.info("Auto-labeled %d inbox message(s).", len(auto_labeled))
    except Exception:
        logger.exception("Inbox scanner failed")
        report["inbox_scanner_error"] = True

    try:
        # 1. Load ingestion
        from app.workflows.load_ingestion import run as ingest_run
        new_loads = ingest_run()
        report["loads_ingested"] = new_loads
        logger.info("Ingested %d new load(s).", len(new_loads))
    except Exception:
        logger.exception("Load ingestion failed")
        report["loads_ingested_error"] = True

    try:
        # 2. Carrier sourcing
        from app.workflows.carrier_sourcing import run as sourcing_run
        sourced = sourcing_run()
        report["rfqs_sent_for"] = sourced
    except Exception:
        logger.exception("Carrier sourcing failed")
        report["carrier_sourcing_error"] = True

    try:
        # 3. RFQ expansion
        from app.workflows.carrier_sourcing import run_expansion
        expanded = run_expansion()
        report["rfqs_expanded_for"] = expanded
    except Exception:
        logger.exception("RFQ expansion failed")
        report["rfq_expansion_error"] = True

    try:
        # 4. Quote processing
        from app.workflows.quote_processing import run as quotes_run
        selected = quotes_run()
        report["carriers_selected_for"] = selected
    except Exception:
        logger.exception("Quote processing failed")
        report["quote_processing_error"] = True

    try:
        # 4b. Outreach reply processing (carrier replies to general outreach)
        from app.workflows.outreach_reply import run as outreach_reply_run
        outreach_processed = outreach_reply_run()
        report["outreach_replies_processed"] = outreach_processed
    except Exception:
        logger.exception("Outreach reply processing failed")
        report["outreach_reply_error"] = True

    try:
        # 5. Onboarding requests
        from app.workflows.onboarding import run_send_requests
        onboard_sent = run_send_requests()
        report["onboarding_requests_sent"] = onboard_sent
    except Exception:
        logger.exception("Onboarding send failed")
        report["onboarding_send_error"] = True

    try:
        # 6. Check onboarding docs
        from app.workflows.onboarding import run_check_documents
        docs_processed = run_check_documents()
        report["onboarding_docs_processed"] = docs_processed
    except Exception:
        logger.exception("Onboarding doc check failed")
        report["onboarding_docs_error"] = True

    try:
        # 7. Approval packets
        from app.workflows.approval import run_send_packets
        approval_sent = run_send_packets()
        report["approval_packets_sent"] = approval_sent
    except Exception:
        logger.exception("Approval packet send failed")
        report["approval_send_error"] = True

    try:
        # 8. Check approval replies
        from app.workflows.approval import run_check_replies
        approved = run_check_replies()
        report["approvals_processed"] = approved
    except Exception:
        logger.exception("Approval reply check failed")
        report["approval_check_error"] = True

    try:
        # 9. Rate confirmation
        from app.workflows.rate_confirmation import run as rateconf_run
        rate_confs = rateconf_run()
        report["rate_confirmations_sent"] = rate_confs
    except Exception:
        logger.exception("Rate confirmation failed")
        report["rate_confirmation_error"] = True

    try:
        # 10. Compliance sync
        from app.workflows.compliance_sync import run_for_active_loads
        synced = run_for_active_loads()
        report["compliance_synced"] = synced
    except Exception:
        logger.exception("Compliance sync failed")
        report["compliance_sync_error"] = True

    elapsed = time.time() - start
    report["elapsed_seconds"] = round(elapsed, 2)
    logger.info("Poll job completed in %.2fs. Report: %s", elapsed, report)

    return JSONResponse(content=report)


# ── Dedicated compliance endpoint ────────────────────────────────────────────

@app.post("/jobs/compliance")
def compliance_job():
    """Run compliance sync independently."""
    from app.workflows.compliance_sync import run_for_active_loads
    synced = run_for_active_loads()
    return {"synced_carriers": synced}


@app.post("/debug/parse-test")
def parse_test():
    """Test the Gemini parser with a sample email body."""
    import traceback
    from app.ai_parser import _call_gemini, classify_email, parse_with_gemini, check_completeness

    test_body = (
        "Hey, this is Derek with Atlantic seafood. I need 30,000 pounds of frozen shrimp "
        "moved from key West to Miami tomorrow at 8 AM. Can you get the job done for $800? "
        "Let me know thanks.\n\nSent from my iPhone"
    )
    test_subject = "Freight request"

    output: dict[str, Any] = {}
    output["test_email"] = test_body

    # Step 1: Test raw Gemini call directly
    try:
        raw = _call_gemini("Return ONLY this JSON: {\"test\": \"hello\"}", max_tokens=64)
        output["gemini_raw_test"] = raw
    except Exception as e:
        output["gemini_raw_error"] = str(e)
        output["gemini_raw_traceback"] = traceback.format_exc()

    # Step 2: Classification
    try:
        classification = classify_email(test_body, test_subject, "derek@atlanticseafood.com")
        output["classification"] = classification
    except Exception as e:
        output["classification_error"] = str(e)
        output["classification_traceback"] = traceback.format_exc()

    # Step 3: Parsing
    try:
        parsed = parse_with_gemini(test_body, test_subject)
        output["gemini_parsed"] = parsed
        completeness = check_completeness(parsed)
        output["completeness"] = completeness
    except Exception as e:
        output["parse_error"] = str(e)
        output["parse_traceback"] = traceback.format_exc()

    return JSONResponse(content=output)


@app.post("/jobs/ingest-test")
def ingest_test():
    """Run ONLY load ingestion with verbose output for debugging."""
    import traceback
    from app.gmail import search_messages, _get_label_id, _label_cache, get_gmail_service
    from app.sheets import is_message_processed

    output: dict[str, Any] = {}

    # 1. Check credentials
    try:
        svc = get_gmail_service()
        profile = svc.users().getProfile(userId="me").execute()
        output["gmail_account"] = profile.get("emailAddress")
    except Exception as e:
        output["gmail_auth_error"] = str(e)
        return JSONResponse(content=output)

    # 2. Check label resolution
    try:
        label_id = _get_label_id("OPS/NEW_LOAD")
        output["label_id"] = label_id
        output["label_cache_keys"] = list(_label_cache.keys())
    except Exception as e:
        output["label_error"] = str(e)

    # 3. Direct API call (like debug endpoint)
    try:
        if label_id:
            msg_resp = svc.users().messages().list(
                userId="me", labelIds=[label_id]
            ).execute()
            direct_msgs = msg_resp.get("messages", [])
            output["direct_api_count"] = len(direct_msgs)
            output["direct_api_messages"] = direct_msgs[:5]
        else:
            output["direct_api_count"] = "SKIPPED – no label_id"
    except Exception as e:
        output["direct_api_error"] = str(e)

    # 4. search_messages call (same as load_ingestion uses)
    try:
        sm_results = search_messages("OPS/NEW_LOAD")
        output["search_messages_count"] = len(sm_results)
        output["search_messages_results"] = sm_results[:5]
    except Exception as e:
        output["search_messages_error"] = str(e)
        output["search_messages_traceback"] = traceback.format_exc()

    # 5. Check processed status + classify each message
    if sm_results:
        from app.gmail import get_message, get_body_text, get_header
        from app.ai_parser import classify_email
        for m in sm_results[:5]:
            mid = m["id"]
            try:
                output[f"is_processed_{mid}"] = is_message_processed(mid)
            except Exception as e:
                output[f"processed_check_error_{mid}"] = str(e)
            # Show classification for each message
            try:
                full_msg = get_message(mid)
                subj = get_header(full_msg, "Subject")
                body = get_body_text(full_msg)
                from_a = get_header(full_msg, "From")
                classification = classify_email(body, subj, from_a)
                output[f"classification_{mid}"] = classification
            except Exception as e:
                output[f"classification_error_{mid}"] = str(e)

    # 6. Actually run load_ingestion
    try:
        from app.workflows.load_ingestion import run as ingest_run
        # Clear any previous error state
        if hasattr(ingest_run, "_last_errors"):
            ingest_run._last_errors = []
        created = ingest_run()
        output["ingestion_result"] = created
        # Capture internal errors that run() caught silently
        if hasattr(ingest_run, "_last_errors") and ingest_run._last_errors:
            output["ingestion_internal_errors"] = ingest_run._last_errors
    except Exception as e:
        output["ingestion_error"] = str(e)
        output["ingestion_traceback"] = traceback.format_exc()

    return JSONResponse(content=output)


@app.get("/debug/labels")
def debug_labels():
    """Debug: show Gmail labels and search results."""
    from app.gmail import get_gmail_service, _get_label_id, search_messages
    from app.sheets import is_message_processed
    svc = get_gmail_service()

    # List all OPS labels
    resp = svc.users().labels().list(userId="me").execute()
    ops_labels = [
        {"name": lbl["name"], "id": lbl["id"]}
        for lbl in resp.get("labels", [])
        if lbl["name"].startswith("OPS/")
    ]

    # Try to find OPS/NEW_LOAD label
    label_id = _get_label_id("OPS/NEW_LOAD")

    # Search for messages
    messages = []
    processed_status = []
    if label_id:
        msg_resp = svc.users().messages().list(
            userId="me", labelIds=[label_id]
        ).execute()
        messages = msg_resp.get("messages", [])
        for m in messages:
            processed_status.append({
                "id": m["id"],
                "is_processed": is_message_processed(m["id"]),
            })

    # Get user email
    profile = svc.users().getProfile(userId="me").execute()

    return {
        "gmail_account": profile.get("emailAddress"),
        "ops_labels": ops_labels,
        "new_load_label_id": label_id,
        "messages_found": len(messages),
        "messages": messages,
        "processed_status": processed_status,
    }
