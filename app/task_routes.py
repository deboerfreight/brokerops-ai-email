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

import hmac
import logging
import os
import time
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, HTMLResponse

logger = logging.getLogger("brokerops.tasks")

router = APIRouter(prefix="/tasks", tags=["scheduled-tasks"])

# ---------------------------------------------------------------------------
# Mobile approval router — no /tasks prefix; these are public signed-URL routes.
# Auth is enforced via HMAC-SHA256 signed query params on every request.
# These routes are intentionally NOT behind _verify_token() — they carry their
# own per-request cryptographic proof of authorization.
# ---------------------------------------------------------------------------

approval_router = APIRouter(tags=["mobile-approval"])

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
    if not x_scheduler_token or not hmac.compare_digest(x_scheduler_token, expected):
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
    """
    _verify_token(x_scheduler_token)
    start = time.time()
    result: dict[str, Any] = {}

    try:
        from app.workflows.outreach_reply import run as reply_run
        processed = reply_run()
        result["carrier_replies_processed"] = len(processed)
        result["carrier_reply_ids"] = processed
        logger.info("poll-replies: processed %d carrier reply message(s)", len(processed))
    except Exception as exc:
        logger.exception("poll-replies: carrier reply sweep failed")
        result["carrier_replies_error"] = str(exc)

    try:
        # Fire any E4 docs requests that have passed their scheduled send time
        from app.reply_classifier import process_scheduled_doc_requests
        e4_sent = process_scheduled_doc_requests()
        result["e4_docs_requests_sent"] = e4_sent
        if e4_sent:
            logger.info("poll-replies: %d scheduled E4(s) dispatched", e4_sent)
    except Exception as exc:
        logger.exception("poll-replies: scheduled doc-request sweep failed")
        result["e4_docs_requests_error"] = str(exc)

    if "carrier_replies_error" in result and "carrier_replies_processed" not in result:
        result["elapsed_seconds"] = round(time.time() - start, 2)
        return JSONResponse(status_code=500, content=result)

    result["elapsed_seconds"] = round(time.time() - start, 2)
    result["status"] = "ok"
    return JSONResponse(content=result)


# ---------------------------------------------------------------------------
# /tasks/assemble-outreach-batch  —  Manley daily batch builder
# ---------------------------------------------------------------------------

@router.post("/assemble-outreach-batch")
def assemble_outreach_batch(
    x_scheduler_token: str | None = Header(default=None),
    limit: int = 20,
    dry_run: bool = False,
) -> JSONResponse:
    """
    Builds the daily carrier outreach batch for Manley DeBoer loads.
    Regenerates fresh candidates from the live sheet (no preview JSON on Cloud Run).
    Posts a Slack message with the batch preview and waits for Derek approval
    before any sends.

    Query param `limit` (default 20) caps the batch size. Use ?limit=5 for
    conservative first sends during ramp-up.

    Query param `dry_run=true` (default false): renders the batch but writes
    nothing to GCS and sends no Slack DM. Returns a `records` field in the
    JSON response containing each carrier with its rendered E1 body. Safe to
    call without triggering any side effects.
    """
    _verify_token(x_scheduler_token)
    start = time.time()
    result: dict[str, Any] = {}

    try:
        from app.carrier_outreach import run_daily_outreach_batch
        batch_result = run_daily_outreach_batch(dry_run=dry_run, limit=limit)
        result["sent"] = batch_result.sent
        result["skipped"] = batch_result.skipped
        result["errors"] = batch_result.errors
        result["bounces_detected"] = batch_result.bounces_detected
        result["approval_token"] = batch_result.approval_token
        result["thread_ids"] = batch_result.thread_ids
        result["skipped_details"] = batch_result.skipped_details
        result["dry_run"] = batch_result.dry_run
        if dry_run:
            result["status"] = "dry_run"
            result["records"] = getattr(batch_result, "records", [])
        logger.info(
            "assemble-outreach-batch: dry_run=%s sent=%d skipped=%d errors=%d token=%s",
            dry_run, batch_result.sent, batch_result.skipped, batch_result.errors,
            batch_result.approval_token,
        )
    except Exception as exc:
        logger.exception("assemble-outreach-batch failed")
        result["error"] = str(exc)
        result["elapsed_seconds"] = round(time.time() - start, 2)
        return JSONResponse(status_code=500, content=result)

    result["elapsed_seconds"] = round(time.time() - start, 2)
    if "status" not in result:
        result["status"] = "ok"
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


# ---------------------------------------------------------------------------
# /tasks/send-test-email  —  End-to-end Gmail workload identity verification
# ---------------------------------------------------------------------------

@router.post("/send-test-email")
def send_test_email(
    to: str = Query(..., description="Recipient email address"),
    x_scheduler_token: str | None = Header(default=None),
) -> JSONResponse:
    """
    Renders the E1 template against a synthetic sample carrier and sends via
    the real Gmail workload identity path.  Use this to verify the send path
    works end-to-end before touching real carriers.

    Sample carrier: Hotshot Driving Services LLC / DOT 3766455 / FL / dry_van
    DO NOT deploy this endpoint to production triggers — it is for manual
    pre-flight verification only.
    """
    _verify_token(x_scheduler_token)
    start = time.time()

    _SAMPLE_CARRIER = {
        "DOT Number": "3766455",
        "Legal_Name": "Hotshot Driving Services LLC",
        "State": "FL",
        "Equipment_Type": "dry_van",
        "Primary_Email": "mike@hotshotdriving.com",
        "Contact_Name": "Mike",
        "Service Type": "General",
        "Outreach_Status": "",
    }

    try:
        from app.carrier_outreach import render_e1
        subject, body = render_e1(_SAMPLE_CARRIER)
    except Exception as exc:
        logger.exception("send-test-email: template render failed")
        return JSONResponse(status_code=500, content={"error": f"render failed: {exc}"})

    try:
        from app.gmail import send_email
        sent = send_email(to=to, subject=subject, body_text=body)
        message_id = sent.get("id", "")
        thread_id = sent.get("threadId", "")
    except Exception as exc:
        logger.exception("send-test-email: Gmail send failed")
        return JSONResponse(status_code=500, content={"error": f"send failed: {exc}"})

    import datetime as _dt
    sent_at = _dt.datetime.now(_dt.timezone.utc).isoformat()
    result = {
        "status": "ok",
        "message_id": message_id,
        "thread_id": thread_id,
        "sent_at": sent_at,
        "rendered_subject": subject,
        "rendered_body_preview": body[:200],
        "to": to,
        "elapsed_seconds": round(time.time() - start, 2),
    }
    logger.info(
        "send-test-email: sent to=%s message_id=%s thread_id=%s subject=%r",
        to, message_id, thread_id, subject,
    )
    return JSONResponse(content=result)


# ---------------------------------------------------------------------------
# /approve  —  Side-effect-free preview page (GET) + confirm action (POST)
# ---------------------------------------------------------------------------

@approval_router.get("/approve")
async def approve_batch_preview(
    token: str,
    sig: str,
    exp: int,
    request: Request,
) -> HTMLResponse:
    """
    SIDE-EFFECT FREE preview page. Slack's unfurl bot hits this GET to build
    the link card — it must never fire any sends or write any state.

    Verifies HMAC signature and expiration, reads the batch from GCS, and
    returns an HTML page with carrier summary + email preview + two form
    buttons (Confirm and Send / Cancel Batch). Both buttons POST to their
    respective routes with hidden token/sig/exp inputs.

    NO writes to GCS. NO sends. NO state changes of any kind.
    """
    from app.signed_urls import verify_token
    from app.pending_batch_store import read_pending_batch

    # ── 1. Verify HMAC signature ───────────────────────────────────────────
    secret = os.environ.get("APPROVAL_SIGNING_SECRET", "")
    if not secret:
        logger.error("/approve GET: APPROVAL_SIGNING_SECRET not configured")
        return HTMLResponse(
            "<h1>Configuration error</h1><p>Approval signing secret not set.</p>",
            status_code=500,
            headers={"Cache-Control": "no-store"},
        )

    valid, reason = verify_token(token, sig, int(exp), secret)
    if not valid:
        logger.warning("/approve GET: token verification failed — %s (token=%s)", reason, token)
        status = 410 if reason == "expired" else 400
        return HTMLResponse(
            f"<h1>Link {'expired' if reason == 'expired' else 'invalid'}: {reason}</h1>",
            status_code=status,
            headers={"Cache-Control": "no-store"},
        )

    # ── 2. Read batch from GCS (read-only) ────────────────────────────────
    batch = read_pending_batch(token)
    if not batch:
        logger.warning("/approve GET: batch not found in GCS — token=%s", token)
        return HTMLResponse(
            "<h1>Batch not found</h1><p>The batch may have already been processed or the link expired.</p>",
            status_code=404,
            headers={"Cache-Control": "no-store"},
        )

    # ── 3. Already actioned — show previous result, no state change ───────
    if batch.get("used"):
        logger.info("/approve GET: batch already used — returning info page (token=%s)", token)
        actioned_at = batch.get("used_at", "")
        action_label = "cancelled" if batch.get("cancelled") else "approved and sent"
        return HTMLResponse(
            f"""<html><head><meta name="viewport" content="width=device-width,initial-scale=1">
            <style>body{{font-family:sans-serif;padding:32px;max-width:520px;margin:auto}}</style></head>
            <body>
            <h1>Already actioned</h1>
            <p>This batch was already <strong>{action_label}</strong>.</p>
            {'<p>Actioned at: ' + str(actioned_at) + '</p>' if actioned_at else ''}
            <p>Check Slack for the send summary.</p>
            </body></html>""",
            status_code=200,
            headers={"Cache-Control": "no-store"},
        )

    # ── 4. Build preview HTML — carrier list + first email body ───────────
    records = batch.get("records", [])
    hidden_inputs = (
        f'<input type="hidden" name="token" value="{token}">'
        f'<input type="hidden" name="sig" value="{sig}">'
        f'<input type="hidden" name="exp" value="{exp}">'
    )

    carrier_rows_html = ""
    for i, r in enumerate(records, 1):
        carrier_rows_html += (
            f"<tr><td>{i}</td>"
            f"<td>{r.get('legal_name','')}</td>"
            f"<td>{r.get('dot','')}</td>"
            f"<td>{r.get('email','')}</td>"
            f"<td>{r.get('subject','')}</td></tr>\n"
        )

    first_body = ""
    if records:
        raw_body = records[0].get("body", "")
        first_body = raw_body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\n", "<br>")

    preview_html = f"""<html>
<head>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>
    body{{font-family:sans-serif;padding:20px;max-width:680px;margin:auto}}
    table{{border-collapse:collapse;width:100%;font-size:13px}}
    th,td{{border:1px solid #ccc;padding:6px 8px;text-align:left}}
    th{{background:#f5f5f5}}
    .preview-box{{background:#f9f9f9;border:1px solid #ddd;padding:16px;margin:16px 0;
                  white-space:pre-wrap;font-size:13px;font-family:monospace;border-radius:4px}}
    .btn-confirm{{background:#2ecc71;color:#fff;border:none;padding:14px 28px;
                  font-size:16px;border-radius:6px;cursor:pointer;margin-right:12px}}
    .btn-cancel{{background:#e74c3c;color:#fff;border:none;padding:14px 28px;
                 font-size:16px;border-radius:6px;cursor:pointer}}
    .actions{{margin-top:24px}}
  </style>
</head>
<body>
  <h2>Outreach batch — {len(records)} carriers</h2>
  <table>
    <tr><th>#</th><th>Name</th><th>DOT</th><th>Email</th><th>Subject</th></tr>
    {carrier_rows_html}
  </table>

  <h3>First email preview</h3>
  <div class="preview-box">{first_body}</div>

  <div class="actions">
    <form method="POST" action="/approve/confirm" style="display:inline">
      {hidden_inputs}
      <button class="btn-confirm" type="submit">Confirm and Send</button>
    </form>
    <form method="POST" action="/cancel/confirm" style="display:inline">
      {hidden_inputs}
      <button class="btn-cancel" type="submit">Cancel Batch</button>
    </form>
  </div>
</body>
</html>"""

    logger.info(
        "/approve GET: preview served for token=%s (%d carriers) — no state changed",
        token, len(records),
    )
    return HTMLResponse(
        preview_html,
        status_code=200,
        headers={"Cache-Control": "no-store"},
    )


# ---------------------------------------------------------------------------
# /approve/confirm  —  POST: fires the send loop
# ---------------------------------------------------------------------------

@approval_router.post("/approve/confirm")
async def approve_batch_confirm(request: Request) -> HTMLResponse:
    """
    POST-only confirm handler. Reads token/sig/exp from form body (not query
    string), re-verifies the HMAC signature (defense in depth), marks the
    batch used BEFORE firing the send loop, then returns HTML result page.

    Double-submit safe: checks used=true before marking; returns "already
    actioned" if the batch was already consumed.
    """
    from app.signed_urls import verify_token
    from app.pending_batch_store import read_pending_batch, mark_batch_used
    from app.carrier_outreach import run_approved_batch_send
    from app.notifications import notify_slack

    form = await request.form()
    token = form.get("token", "")
    sig = form.get("sig", "")
    exp_raw = form.get("exp", "0")

    # ── 1. Re-verify HMAC signature (defense in depth) ────────────────────
    secret = os.environ.get("APPROVAL_SIGNING_SECRET", "")
    if not secret:
        logger.error("/approve/confirm: APPROVAL_SIGNING_SECRET not configured")
        return HTMLResponse(
            "<h1>Configuration error</h1><p>Approval signing secret not set.</p>",
            status_code=500,
        )

    try:
        exp = int(exp_raw)
    except (ValueError, TypeError):
        return HTMLResponse("<h1>Invalid request</h1><p>Bad expiration value.</p>", status_code=400)

    valid, reason = verify_token(token, sig, exp, secret)
    if not valid:
        logger.warning("/approve/confirm: signature verification failed — %s (token=%s)", reason, token)
        return HTMLResponse(
            f"<h1>Request rejected: {reason}</h1>",
            status_code=400,
        )

    # ── 2. Read batch from GCS ────────────────────────────────────────────
    batch = read_pending_batch(token)
    if not batch:
        logger.warning("/approve/confirm: batch not found in GCS — token=%s", token)
        return HTMLResponse(
            "<h1>Batch not found</h1><p>The batch may have already been processed or expired.</p>",
            status_code=404,
        )

    # ── 3. Double-submit guard ────────────────────────────────────────────
    if batch.get("used"):
        logger.info("/approve/confirm: batch already used — returning info (token=%s)", token)
        action_label = "cancelled" if batch.get("cancelled") else "approved and sent"
        return HTMLResponse(
            f"<html><body style='font-family:sans-serif;padding:32px;max-width:520px;margin:auto'>"
            f"<h1>Already actioned</h1>"
            f"<p>This batch was already {action_label}. No duplicate send fired.</p>"
            f"<p>Check Slack for the send summary.</p>"
            f"</body></html>",
            status_code=200,
        )

    # ── 4. Mark used BEFORE firing send loop (idempotency guard) ──────────
    mark_batch_used(token)
    logger.info("/approve/confirm: batch marked used, starting send loop — token=%s", token)

    # ── 5. Fire the send loop synchronously ──────────────────────────────
    try:
        result = run_approved_batch_send(batch)
    except Exception as exc:
        logger.exception("/approve/confirm: send loop failed — token=%s", token)
        notify_slack(
            f":rotating_light: Outreach send loop failed after approval.\n"
            f"batch_id={token}\nError: {exc}"
        )
        return HTMLResponse(
            f"<h1>Send failed</h1><p>The batch was approved but sending encountered an error: {exc}</p>",
            status_code=500,
        )

    # ── 6. Post Slack results summary ────────────────────────────────────
    notify_slack(
        f"\u2705 Outreach batch sent.\n"
        f"batch_id={token}\n"
        f"Sent: {result['sent']} | Bounced: {result['bounced']} | Errored: {result['errored']}"
    )

    # ── 7. Return HTML result page ────────────────────────────────────────
    return HTMLResponse(
        f"""<html><head><meta name="viewport" content="width=device-width,initial-scale=1">
        <style>body{{font-family:sans-serif;padding:32px;max-width:520px;margin:auto}}</style></head>
        <body>
        <h1>\u2705 Batch sent</h1>
        <p><strong>Sent:</strong> {result['sent']}<br>
           <strong>Bounced:</strong> {result['bounced']}<br>
           <strong>Errored:</strong> {result['errored']}</p>
        <p>Check Slack for the post-send summary.</p>
        </body></html>""",
        status_code=200,
    )


# ---------------------------------------------------------------------------
# /cancel/confirm  —  POST: marks batch cancelled, no emails sent
# ---------------------------------------------------------------------------

@approval_router.post("/cancel/confirm")
async def cancel_batch_confirm(request: Request) -> HTMLResponse:
    """
    POST-only cancel handler. Re-verifies HMAC signature, marks batch used
    with a 'cancelled' marker, returns HTML confirmation. No emails sent.
    """
    from app.signed_urls import verify_token
    from app.pending_batch_store import read_pending_batch, mark_batch_used
    from app.notifications import notify_slack

    form = await request.form()
    token = form.get("token", "")
    sig = form.get("sig", "")
    exp_raw = form.get("exp", "0")

    # ── 1. Re-verify HMAC signature (defense in depth) ────────────────────
    secret = os.environ.get("APPROVAL_SIGNING_SECRET", "")
    if not secret:
        logger.error("/cancel/confirm: APPROVAL_SIGNING_SECRET not configured")
        return HTMLResponse(
            "<h1>Configuration error</h1><p>Approval signing secret not set.</p>",
            status_code=500,
        )

    try:
        exp = int(exp_raw)
    except (ValueError, TypeError):
        return HTMLResponse("<h1>Invalid request</h1><p>Bad expiration value.</p>", status_code=400)

    valid, reason = verify_token(token, sig, exp, secret)
    if not valid:
        logger.warning("/cancel/confirm: signature verification failed — %s (token=%s)", reason, token)
        return HTMLResponse(
            f"<h1>Request rejected: {reason}</h1>",
            status_code=400,
        )

    # ── 2. Read batch from GCS ────────────────────────────────────────────
    batch = read_pending_batch(token)
    if not batch:
        return HTMLResponse(
            "<h1>Batch not found</h1><p>The batch may have already been processed or expired.</p>",
            status_code=404,
        )

    # ── 3. Already actioned guard ─────────────────────────────────────────
    if batch.get("used"):
        action_label = "cancelled" if batch.get("cancelled") else "approved and sent"
        return HTMLResponse(
            f"<html><body style='font-family:sans-serif;padding:32px;max-width:520px;margin:auto'>"
            f"<h1>Already actioned</h1>"
            f"<p>This batch was already {action_label}.</p>"
            f"</body></html>",
            status_code=200,
        )

    # ── 4. Mark used with cancelled marker BEFORE any side effects ────────
    import time as _time
    import json as _json
    from google.cloud import storage as _storage
    _client = _storage.Client()
    _bucket = _client.bucket("wide-decoder-489023-p1-brokerops")
    _blob = _bucket.blob(f"pending_batches/{token}.json")
    batch["used"] = True
    batch["cancelled"] = True
    batch["used_at"] = _time.time()
    _blob.upload_from_string(_json.dumps(batch), content_type="application/json")
    logger.info("/cancel/confirm: batch marked cancelled — token=%s", token)

    notify_slack(f"\u274c Outreach batch cancelled by Derek.\nbatch_id={token}")

    return HTMLResponse(
        """<html><head><meta name="viewport" content="width=device-width,initial-scale=1">
        <style>body{font-family:sans-serif;padding:32px;max-width:520px;margin:auto}</style></head>
        <body>
        <h1>\u274c Batch cancelled</h1>
        <p>No emails were sent. The batch has been discarded.</p>
        </body></html>""",
        status_code=200,
    )


# ---------------------------------------------------------------------------
# /reply-approve  —  GET preview + POST confirm/discard for reply drafts
# (Fix 6, 2026-04-15)
# ---------------------------------------------------------------------------

@approval_router.get("/reply-approve")
async def reply_approve_preview(
    token: str,
    sig: str,
    exp: int,
    request: Request,
) -> HTMLResponse:
    """
    Side-effect-free preview of a carrier reply draft.

    Verifies HMAC signature (24h TTL), reads the draft from GCS, returns an
    HTML page showing the carrier context, original reply, and an EDITABLE
    textarea with the drafted response. Two POST buttons: Send Reply / Discard.
    """
    from app.signed_urls import verify_token
    from app.reply_draft_store import read_reply_draft

    secret = os.environ.get("APPROVAL_SIGNING_SECRET", "")
    if not secret:
        return HTMLResponse("<h1>Configuration error</h1>", status_code=500,
                            headers={"Cache-Control": "no-store"})

    valid, reason = verify_token(token, sig, int(exp), secret)
    if not valid:
        status = 410 if reason == "expired" else 400
        return HTMLResponse(
            f"<h1>Link {'expired' if reason == 'expired' else 'invalid'}: {reason}</h1>",
            status_code=status, headers={"Cache-Control": "no-store"},
        )

    draft = read_reply_draft(token)
    if not draft:
        return HTMLResponse("<h1>Draft not found or already used</h1>", status_code=404,
                            headers={"Cache-Control": "no-store"})

    if draft.get("used"):
        action = "sent" if draft.get("sent") else "discarded"
        return HTMLResponse(
            f"<html><body style='font-family:sans-serif;padding:32px;max-width:520px;margin:auto'>"
            f"<h1>Already actioned</h1><p>This draft was already {action}.</p></body></html>",
            status_code=200, headers={"Cache-Control": "no-store"},
        )

    carrier_name = draft.get("carrier_name", "")
    carrier_dot  = draft.get("carrier_dot", "")
    orig_reply   = (draft.get("original_reply") or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    draft_text   = (draft.get("draft") or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    hidden = (
        f'<input type="hidden" name="token" value="{token}">'
        f'<input type="hidden" name="sig" value="{sig}">'
        f'<input type="hidden" name="exp" value="{exp}">'
    )

    html = f"""<html>
<head>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>
    body{{font-family:sans-serif;padding:20px;max-width:680px;margin:auto}}
    .orig{{background:#f5f5f5;border:1px solid #ddd;padding:12px;white-space:pre-wrap;font-size:13px;border-radius:4px;margin-bottom:16px}}
    textarea{{width:100%;height:220px;font-size:13px;font-family:monospace;padding:10px;border:1px solid #ccc;border-radius:4px;box-sizing:border-box}}
    .btn-send{{background:#2ecc71;color:#fff;border:none;padding:14px 28px;font-size:16px;border-radius:6px;cursor:pointer;margin-right:12px}}
    .btn-discard{{background:#e74c3c;color:#fff;border:none;padding:14px 28px;font-size:16px;border-radius:6px;cursor:pointer}}
    .actions{{margin-top:16px}}
  </style>
</head>
<body>
  <h2>Reply draft — {carrier_name} (DOT {carrier_dot})</h2>
  <h3>Their reply</h3>
  <div class="orig">{orig_reply}</div>
  <h3>Your draft (edit as needed)</h3>
  <form method="POST" action="/reply-approve/confirm">
    {hidden}
    <textarea name="draft_text">{draft_text}</textarea>
    <div class="actions">
      <button class="btn-send" type="submit">Send Reply</button>
    </div>
  </form>
  <form method="POST" action="/reply-approve/discard" style="margin-top:12px">
    {hidden}
    <button class="btn-discard" type="submit">Discard</button>
  </form>
</body>
</html>"""

    logger.info("/reply-approve GET: preview served for token=%s DOT=%s", token, carrier_dot)
    return HTMLResponse(html, status_code=200, headers={"Cache-Control": "no-store"})


@approval_router.post("/reply-approve/confirm")
async def reply_approve_confirm(request: Request) -> HTMLResponse:
    """POST confirm: verify sig, read draft (use edited textarea if provided), send reply in-thread."""
    from app.signed_urls import verify_token
    from app.reply_draft_store import read_reply_draft, mark_draft_used
    from app.notifications import notify_slack as _ns

    form = await request.form()
    token   = form.get("token", "")
    sig     = form.get("sig", "")
    exp_raw = form.get("exp", "0")
    edited  = form.get("draft_text", "").strip()

    secret = os.environ.get("APPROVAL_SIGNING_SECRET", "")
    if not secret:
        return HTMLResponse("<h1>Configuration error</h1>", status_code=500)

    try:
        exp = int(exp_raw)
    except (ValueError, TypeError):
        return HTMLResponse("<h1>Invalid request</h1>", status_code=400)

    valid, reason = verify_token(token, sig, exp, secret)
    if not valid:
        return HTMLResponse(f"<h1>Request rejected: {reason}</h1>", status_code=400)

    draft = read_reply_draft(token)
    if not draft:
        return HTMLResponse("<h1>Draft not found</h1>", status_code=404)

    if draft.get("used"):
        action = "sent" if draft.get("sent") else "discarded"
        return HTMLResponse(
            f"<html><body style='font-family:sans-serif;padding:32px;max-width:520px;margin:auto'>"
            f"<h1>Already actioned</h1><p>This draft was already {action}.</p></body></html>",
            status_code=200,
        )

    # Use the edited textarea version if Derek changed it; fall back to stored draft
    final_body = edited if edited else (draft.get("draft") or "")
    thread_id  = draft.get("thread_id", "")
    email      = draft.get("contact_email", "")
    carrier_name = draft.get("carrier_name", "")
    carrier_dot  = draft.get("carrier_dot", "")

    # Mark used BEFORE sending (idempotency)
    mark_draft_used(token, sent=True)

    try:
        from app.gmail import reply_to_thread, send_email
        subject = "Re: Introduction -- deBoer Freight"
        if thread_id and email:
            reply_to_thread(thread_id=thread_id, to=email, subject=subject, body_text=final_body)
        elif email:
            send_email(to=email, subject=subject, body_text=final_body)
        logger.info("/reply-approve/confirm: sent reply to DOT=%s (%s)", carrier_dot, email)
    except Exception as exc:
        logger.exception("/reply-approve/confirm: send failed for DOT=%s", carrier_dot)
        _ns(f"Reply send failed for {carrier_name} (DOT {carrier_dot}): {exc}")
        return HTMLResponse(
            f"<h1>Send failed</h1><p>{exc}</p>",
            status_code=500,
        )

    # Update carrier row
    try:
        from app.sheets import update_carrier_fields_by_dot
        update_carrier_fields_by_dot(carrier_dot, {"Onboarding_Status": "docs_requested"})
    except Exception as e:
        logger.warning("/reply-approve/confirm: sheet update failed for DOT=%s: %s", carrier_dot, e)

    _ns(f"Reply sent to {carrier_name} (DOT {carrier_dot}). Onboarding status -> docs_requested.")

    return HTMLResponse(
        f"""<html><head><meta name="viewport" content="width=device-width,initial-scale=1">
        <style>body{{font-family:sans-serif;padding:32px;max-width:520px;margin:auto}}</style></head>
        <body>
        <h1>Reply sent</h1>
        <p>Your reply to <strong>{carrier_name}</strong> was sent in-thread.</p>
        <p>Carrier status updated to docs_requested.</p>
        </body></html>""",
        status_code=200,
    )


@approval_router.post("/reply-approve/discard")
async def reply_approve_discard(request: Request) -> HTMLResponse:
    """POST discard: verify sig, mark draft used/not-sent, no email sent."""
    from app.signed_urls import verify_token
    from app.reply_draft_store import read_reply_draft, mark_draft_used

    form = await request.form()
    token   = form.get("token", "")
    sig     = form.get("sig", "")
    exp_raw = form.get("exp", "0")

    secret = os.environ.get("APPROVAL_SIGNING_SECRET", "")
    if not secret:
        return HTMLResponse("<h1>Configuration error</h1>", status_code=500)

    try:
        exp = int(exp_raw)
    except (ValueError, TypeError):
        return HTMLResponse("<h1>Invalid request</h1>", status_code=400)

    valid, reason = verify_token(token, sig, exp, secret)
    if not valid:
        return HTMLResponse(f"<h1>Request rejected: {reason}</h1>", status_code=400)

    draft = read_reply_draft(token)
    if not draft:
        return HTMLResponse("<h1>Draft not found</h1>", status_code=404)

    if draft.get("used"):
        action = "sent" if draft.get("sent") else "discarded"
        return HTMLResponse(
            f"<html><body style='font-family:sans-serif;padding:32px;max-width:520px;margin:auto'>"
            f"<h1>Already actioned</h1><p>This draft was already {action}.</p></body></html>",
            status_code=200,
        )

    mark_draft_used(token, sent=False)
    carrier_name = draft.get("carrier_name", "")
    carrier_dot  = draft.get("carrier_dot", "")
    logger.info("/reply-approve/discard: draft discarded for DOT=%s", carrier_dot)

    return HTMLResponse(
        f"""<html><head><meta name="viewport" content="width=device-width,initial-scale=1">
        <style>body{{font-family:sans-serif;padding:32px;max-width:520px;margin:auto}}</style></head>
        <body>
        <h1>Draft discarded</h1>
        <p>No reply was sent to <strong>{carrier_name}</strong>.</p>
        </body></html>""",
        status_code=200,
    )


# ---------------------------------------------------------------------------
# /cancel  —  Redirect to /approve (legacy link support)
# ---------------------------------------------------------------------------

@approval_router.get("/cancel")
async def cancel_batch_redirect(
    token: str,
    sig: str,
    exp: int,
    request: Request,
) -> HTMLResponse:
    """
    Legacy GET /cancel links now redirect to the /approve preview page,
    which surfaces both Confirm and Cancel buttons. Side-effect free.
    """
    from fastapi.responses import RedirectResponse
    return RedirectResponse(
        url=f"/approve?token={token}&sig={sig}&exp={exp}",
        status_code=302,
    )
