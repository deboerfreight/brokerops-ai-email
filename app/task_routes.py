"""
BrokerOps AI — Cloud Scheduler task entry points.

Each route here corresponds to one Cloud Scheduler job.  All routes are
protected by a shared-secret header (X-Scheduler-Token) verified against
the SCHEDULER_TOKEN env var.  Cloud Scheduler is the only caller — if the
token is wrong, 401.

Route → schtasks equivalent mapping
────────────────────────────────────
POST /tasks/vetting-sweep          BrokerOps-Vetting-Daily-Sweep    (04:00 daily)
POST /tasks/poll-replies           BrokerOps-ProcessReplies          (was disabled)
POST /tasks/assemble-outreach-batch  [new — Manley batch]            (08:30 M-F)
POST /tasks/process-attachments    [new — onboarding doc scanner]    (every 15 min)
POST /tasks/mdl-vendor-dispatch    BrokerOps-MDL-Vendor-Dispatcher  (every 5 min)
POST /tasks/daily-report           [new — ops summary email]         (18:00 M-F)

Parallel work notice
────────────────────
The *bodies* of /tasks/assemble-outreach-batch and /tasks/poll-replies are
stubs marked TODO(bolt-outreach).  The parallel Bolt instance that owns
carrier_outreach.py / reply_classifier.py will wire the implementation.
This file only owns the routing layer, auth guard, and error wrapper.

Auth
────
Cloud Scheduler sets the header:
    X-Scheduler-Token: <value of SCHEDULER_TOKEN env var>

For OIDC-authenticated Cloud Run services, Cloud Scheduler also sends an
OIDC token in the Authorization header, but we keep the shared-secret layer
as a belt-and-suspenders guard regardless of IAM configuration.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import JSONResponse

logger = logging.getLogger("brokerops.tasks")

router = APIRouter(prefix="/tasks", tags=["scheduled-tasks"])

# ---------------------------------------------------------------------------
# Auth guard
# ---------------------------------------------------------------------------

def _verify_token(x_scheduler_token: str | None) -> None:
    """Raise 401 if the token header does not match SCHEDULER_TOKEN env var."""
    expected = os.environ.get("SCHEDULER_TOKEN", "")
    if not expected:
        # If the env var is not set at all, block all calls — misconfiguration
        # is safer than an open endpoint.
        logger.error("SCHEDULER_TOKEN env var is not set — rejecting all task calls")
        raise HTTPException(status_code=500, detail="SCHEDULER_TOKEN not configured")
    if not x_scheduler_token or x_scheduler_token != expected:
        logger.warning("Task route rejected: bad or missing X-Scheduler-Token")
        raise HTTPException(status_code=401, detail="Unauthorized")


# ---------------------------------------------------------------------------
# /tasks/vetting-sweep  —  BrokerOps-Vetting-Daily-Sweep replacement
# ---------------------------------------------------------------------------

@router.post("/vetting-sweep")
def vetting_sweep(x_scheduler_token: str | None = Header(default=None)) -> JSONResponse:
    """
    Re-vet every row in Carrier_Master and Carrier Quarantine against the
    canonical hard-reject rules.  Releases any quarantine rows that now pass.
    Does NOT re-fetch FMCSA data (use ?refetch=true for that — slow).

    Replaces: BrokerOps-Vetting-Daily-Sweep schtask (04:00 daily)
    """
    _verify_token(x_scheduler_token)
    start = time.time()
    result: dict[str, Any] = {}

    try:
        from app.vetting.sweep import sweep_carrier_database
        sweep_result = sweep_carrier_database(re_fetch_fmcsa=False)
        result["sweep"] = sweep_result
        logger.info("vetting-sweep completed: %s", sweep_result)
    except Exception as exc:
        logger.exception("vetting-sweep failed")
        result["error"] = str(exc)
        result["elapsed_seconds"] = round(time.time() - start, 2)
        return JSONResponse(status_code=500, content=result)

    result["elapsed_seconds"] = round(time.time() - start, 2)
    result["status"] = "ok"
    return JSONResponse(content=result)


# ---------------------------------------------------------------------------
# /tasks/poll-replies  —  BrokerOps-ProcessReplies replacement
# ---------------------------------------------------------------------------

@router.post("/poll-replies")
def poll_replies(x_scheduler_token: str | None = Header(default=None)) -> JSONResponse:
    """
    Poll Gmail for carrier outreach reply emails and MDL vendor replies.
    Classifies, updates sheets, triggers follow-ups per the reply workflow.

    Replaces: BrokerOps-ProcessReplies schtask (was disabled)
    TODO(bolt-outreach): wire the actual reply classifier body here.
    """
    _verify_token(x_scheduler_token)
    start = time.time()
    result: dict[str, Any] = {}

    try:
        # TODO(bolt-outreach): replace stub with reply classifier invocation
        # from app.workflows.outreach_reply import run as reply_run
        # processed = reply_run()
        # result["carrier_replies_processed"] = processed
        logger.info("poll-replies: stub invoked — awaiting bolt-outreach implementation")
        result["status"] = "stub"
        result["message"] = "Reply poller not yet wired — see TODO(bolt-outreach)"

        # Amendment 2: fire any E4 docs requests that have passed their scheduled time
        from app.reply_classifier import process_scheduled_doc_requests
        e4_sent = process_scheduled_doc_requests()
        result["e4_docs_requests_sent"] = e4_sent
        if e4_sent:
            logger.info("poll-replies: %d scheduled E4(s) dispatched", e4_sent)
    except Exception as exc:
        logger.exception("poll-replies failed")
        result["error"] = str(exc)
        result["elapsed_seconds"] = round(time.time() - start, 2)
        return JSONResponse(status_code=500, content=result)

    result["elapsed_seconds"] = round(time.time() - start, 2)
    return JSONResponse(content=result)


# ---------------------------------------------------------------------------
# /tasks/assemble-outreach-batch  —  Manley daily batch builder
# ---------------------------------------------------------------------------

@router.post("/assemble-outreach-batch")
def assemble_outreach_batch(x_scheduler_token: str | None = Header(default=None)) -> JSONResponse:
    """
    Builds the daily carrier outreach batch for Manley DeBoer loads.
    Posts a Slack message with the batch preview and waits for Derek approval
    before any sends.

    New job — no schtask equivalent.
    TODO(bolt-outreach): wire the batch assembly and Slack approval flow here.
    """
    _verify_token(x_scheduler_token)
    start = time.time()
    result: dict[str, Any] = {}

    try:
        # TODO(bolt-outreach): replace stub with batch assembler
        # from app.workflows.carrier_outreach import assemble_batch
        # batch = assemble_batch()
        # result["batch_carriers"] = len(batch)
        # result["slack_approval_sent"] = True
        logger.info("assemble-outreach-batch: stub invoked — awaiting bolt-outreach implementation")
        result["status"] = "stub"
        result["message"] = "Outreach batch assembler not yet wired — see TODO(bolt-outreach)"
    except Exception as exc:
        logger.exception("assemble-outreach-batch failed")
        result["error"] = str(exc)
        result["elapsed_seconds"] = round(time.time() - start, 2)
        return JSONResponse(status_code=500, content=result)

    result["elapsed_seconds"] = round(time.time() - start, 2)
    return JSONResponse(content=result)


# ---------------------------------------------------------------------------
# /tasks/process-attachments  —  Onboarding doc scanner
# ---------------------------------------------------------------------------

@router.post("/process-attachments")
def process_attachments(x_scheduler_token: str | None = Header(default=None)) -> JSONResponse:
    """
    Scans Gmail OPS/ONBOARDING and OPS/APPROVAL_REPLY labels for inbound
    carrier documents (W-9, COI, Authority Letter, ACH forms).  Files docs
    to Drive and updates Carrier_Master onboarding status columns.

    New job — no schtask equivalent (was part of the main /jobs/poll cycle).
    """
    _verify_token(x_scheduler_token)
    start = time.time()
    result: dict[str, Any] = {}

    try:
        from app.workflows.onboarding import run_check_documents
        docs_processed = run_check_documents()
        result["docs_processed"] = docs_processed
        logger.info("process-attachments: processed %s document(s)", len(docs_processed))
    except Exception as exc:
        logger.exception("process-attachments failed")
        result["error"] = str(exc)
        result["elapsed_seconds"] = round(time.time() - start, 2)
        return JSONResponse(status_code=500, content=result)

    result["elapsed_seconds"] = round(time.time() - start, 2)
    result["status"] = "ok"
    return JSONResponse(content=result)


# ---------------------------------------------------------------------------
# /tasks/mdl-vendor-dispatch  —  BrokerOps-MDL-Vendor-Dispatcher replacement
# ---------------------------------------------------------------------------

@router.post("/mdl-vendor-dispatch")
def mdl_vendor_dispatch(x_scheduler_token: str | None = Header(default=None)) -> JSONResponse:
    """
    Runs one cycle of MDL vendor outreach dispatcher + reply sweep.
    Equivalent to one execution of run_mdl_vendor_loop.bat.

    Replaces: BrokerOps-MDL-Vendor-Dispatcher schtask (every 5 minutes)
    """
    _verify_token(x_scheduler_token)
    start = time.time()
    result: dict[str, Any] = {}

    try:
        from app.workflows.mdl_vendor_outreach_dispatcher import run as dispatch_run
        dispatch_result = dispatch_run()
        result["dispatcher"] = dispatch_result
        logger.info("mdl-vendor-dispatch: dispatcher result: %s", dispatch_result)
    except Exception as exc:
        logger.exception("mdl-vendor-dispatch (dispatcher) failed")
        result["dispatcher_error"] = str(exc)

    try:
        from app.workflows.outreach_reply import run_mdl_vendor_replies
        reply_result = run_mdl_vendor_replies()
        result["reply_sweep"] = reply_result
        logger.info("mdl-vendor-dispatch: reply sweep result: %s", reply_result)
    except Exception as exc:
        logger.exception("mdl-vendor-dispatch (reply sweep) failed")
        result["reply_sweep_error"] = str(exc)

    # Dispatcher is critical path — fail if it errored
    if "dispatcher_error" in result:
        result["elapsed_seconds"] = round(time.time() - start, 2)
        return JSONResponse(status_code=500, content=result)

    result["elapsed_seconds"] = round(time.time() - start, 2)
    result["status"] = "ok"
    return JSONResponse(content=result)


# ---------------------------------------------------------------------------
# /tasks/daily-report  —  Ops summary email
# ---------------------------------------------------------------------------

@router.post("/daily-report")
def daily_report(x_scheduler_token: str | None = Header(default=None)) -> JSONResponse:
    """
    Assembles and emails Derek a daily operations summary:
    - Active loads + status distribution
    - Carrier pipeline (prospects added, vetted, quarantined today)
    - MDL vendor outreach stats (sent, replied, pending)
    - Any Cloud Scheduler job failures in the last 24h (from Cloud Logging)
    - Upcoming tasks / needs attention

    New job — no schtask equivalent.
    """
    _verify_token(x_scheduler_token)
    start = time.time()
    result: dict[str, Any] = {}

    try:
        from app.workflows.daily_summary import run as daily_run
        summary = daily_run()
        result["report_sent"] = summary.get("sent", False)
        result["summary"] = summary
        logger.info("daily-report: sent=%s", summary.get("sent"))
    except ImportError:
        # daily_summary module doesn't exist yet — graceful stub
        logger.info("daily-report: app.workflows.daily_summary not yet implemented — skipping")
        result["status"] = "stub"
        result["message"] = "daily_summary workflow not yet implemented"
    except Exception as exc:
        logger.exception("daily-report failed")
        result["error"] = str(exc)
        result["elapsed_seconds"] = round(time.time() - start, 2)
        return JSONResponse(status_code=500, content=result)

    result["elapsed_seconds"] = round(time.time() - start, 2)
    if "status" not in result:
        result["status"] = "ok"
    return JSONResponse(content=result)
