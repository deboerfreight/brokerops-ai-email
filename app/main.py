"""
BrokerOps AI – FastAPI application.

Endpoints:
  GET  /health              – Health check
  GET  /oauth/start         – Start OAuth2 flow
  GET  /oauth/callback      – OAuth2 callback
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
