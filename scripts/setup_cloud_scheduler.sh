#!/usr/bin/env bash
# =============================================================================
# BrokerOps — Create Cloud Scheduler jobs (replaces Windows schtasks)
#
# Usage (dry-run — prints all commands, does NOT execute):
#   ./scripts/setup_cloud_scheduler.sh
#
# Usage (live — creates all jobs):
#   ./scripts/setup_cloud_scheduler.sh --apply
#
# Pre-reqs:
#   1. brokerops-ai Cloud Run service is live (run deploy_cloud_run.sh first)
#   2. brokerops-gmail SA has roles/run.invoker on the service
#   3. SCHEDULER_TOKEN env var is known (copy from vault / .env)
#   4. gcloud authed as sales@deboerfreight.com
#
# Jobs created (6 total):
#
#   vetting-sweep          0 4 * * *         04:00 daily (ET)         → /tasks/vetting-sweep
#   poll-replies           */5 * * * *        every 5 minutes          → /tasks/poll-replies
#   assemble-outreach-batch 30 8 * * 1-5     08:30 M-F (ET)           → /tasks/assemble-outreach-batch
#   process-attachments    */15 * * * *       every 15 minutes         → /tasks/process-attachments
#   mdl-vendor-dispatch    */5 * * * *        every 5 minutes          → /tasks/mdl-vendor-dispatch
#   daily-report           0 18 * * 1-5       18:00 M-F (ET)           → /tasks/daily-report
#   health-check           */5 * * * *        every 5 minutes          → /healthz (GET)
#
# schtasks being replaced:
#   BrokerOps-Vetting-Daily-Sweep  → vetting-sweep
#   BrokerOps-MDL-Vendor-Dispatcher → mdl-vendor-dispatch
#   BrokerOps-ProcessReplies (was disabled) → poll-replies
#   BrokerOps-FollowUp-RefrigeratedExpress → DECOMMISSION (dead, points at deprecated TypeScript repo)
#
# Decommission schtasks AFTER verifying Cloud Scheduler jobs fire correctly:
#   powershell "Disable-ScheduledTask -TaskName 'BrokerOps-Vetting-Daily-Sweep'"
#   powershell "Disable-ScheduledTask -TaskName 'BrokerOps-MDL-Vendor-Dispatcher'"
#   powershell "Disable-ScheduledTask -TaskName 'BrokerOps-ProcessReplies'"
#   powershell "Unregister-ScheduledTask -TaskName 'BrokerOps-FollowUp-RefrigeratedExpress' -Confirm:$false"
# =============================================================================

set -euo pipefail

PROJECT="wide-decoder-489023-p1"
REGION="us-central1"
SA_EMAIL="brokerops-gmail@${PROJECT}.iam.gserviceaccount.com"
TIMEZONE="America/New_York"
MAX_RETRY=3
RETRY_MIN_BACKOFF=10s
RETRY_MAX_BACKOFF=300s

LOG="C:/Users/Owner/brokerops-ai/scripts/logs/cloud_run_migration_20260415.log"
mkdir -p "$(dirname "$LOG")"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$LOG"; }

# ── Require SCHEDULER_TOKEN ───────────────────────────────────────────────────
if [[ -z "${SCHEDULER_TOKEN:-}" ]]; then
  log "ERROR: SCHEDULER_TOKEN env var is not set."
  log "  Set it from vault: export SCHEDULER_TOKEN=<value from vault>"
  log "  Or hydrate .env and source it: source .env"
  exit 1
fi

# ── Service URL — detect from running Cloud Run service ───────────────────────
SERVICE_URL=$(gcloud run services describe brokerops-ai \
  --region="$REGION" \
  --project="$PROJECT" \
  --format="value(status.url)" 2>/dev/null || echo "")

if [[ -z "$SERVICE_URL" ]]; then
  log "ERROR: Could not detect brokerops-ai service URL."
  log "  Is the service deployed? Run scripts/deploy_cloud_run.sh --apply first."
  exit 1
fi

log "Service URL: $SERVICE_URL"

# ── Mode switch ───────────────────────────────────────────────────────────────
APPLY="false"
for arg in "$@"; do
  case "$arg" in
    --apply) APPLY="true" ;;
    -h|--help) grep '^#' "$0" | sed 's/^# //;s/^#//'; exit 0 ;;
    *) echo "Unknown flag: $arg" >&2; exit 2 ;;
  esac
done

if [[ "$APPLY" != "true" ]]; then
  log "================================================================"
  log "  DRY RUN — no jobs will be created."
  log "  Re-run with --apply to create for real."
  log "================================================================"
fi

# Redact the X-Scheduler-Token header value in logged command output so the
# shared secret never appears in stdout or the log file.
redact_args() {
  local redacted=""
  for arg in "$@"; do
    if [[ "$arg" == --headers=*X-Scheduler-Token=* ]]; then
      redacted+=" --headers=X-Scheduler-Token=<redacted>"
    else
      redacted+=" $arg"
    fi
  done
  echo "${redacted# }"
}

run() {
  if [[ "$APPLY" == "true" ]]; then
    log "+ $(redact_args "$@")"
    "$@" 2>&1 | tee -a "$LOG"
  else
    log "  [DRY-RUN] $(redact_args "$@")"
  fi
}

# Helper: create one Cloud Scheduler http job
create_job() {
  local JOB_NAME="$1"
  local SCHEDULE="$2"
  local URI="$3"
  local HTTP_METHOD="${4:-POST}"
  local DESCRIPTION="$5"
  local DEADLINE="${6:-120s}"

  log "--- Creating job: $JOB_NAME  schedule='$SCHEDULE'  uri=$URI"

  # Delete if already exists (idempotent re-run)
  if [[ "$APPLY" == "true" ]]; then
    gcloud scheduler jobs describe "$JOB_NAME" \
      --location="$REGION" \
      --project="$PROJECT" &>/dev/null \
    && {
      log "  Job $JOB_NAME exists — deleting before recreate"
      gcloud scheduler jobs delete "$JOB_NAME" \
        --location="$REGION" \
        --project="$PROJECT" \
        --quiet 2>&1 | tee -a "$LOG"
    } || true
  fi

  run gcloud scheduler jobs create http "$JOB_NAME" \
    --project="$PROJECT" \
    --location="$REGION" \
    --schedule="$SCHEDULE" \
    --time-zone="$TIMEZONE" \
    --uri="$URI" \
    --http-method="$HTTP_METHOD" \
    --headers="X-Scheduler-Token=${SCHEDULER_TOKEN}" \
    --oidc-service-account-email="$SA_EMAIL" \
    --oidc-token-audience="$SERVICE_URL" \
    --attempt-deadline="$DEADLINE" \
    --max-retry-attempts="$MAX_RETRY" \
    --min-backoff="$RETRY_MIN_BACKOFF" \
    --max-backoff="$RETRY_MAX_BACKOFF" \
    --description="$DESCRIPTION"
}

# ── Job 1: Vetting sweep (replaces BrokerOps-Vetting-Daily-Sweep) ────────────
create_job \
  "brokerops-vetting-sweep" \
  "0 4 * * *" \
  "${SERVICE_URL}/tasks/vetting-sweep" \
  "POST" \
  "BrokerOps daily vetting sweep — re-vets all carriers against hard-reject rules (replaces schtask)" \
  "300s"

# ── Job 2: Poll replies (replaces BrokerOps-ProcessReplies — was disabled) ───
create_job \
  "brokerops-poll-replies" \
  "*/5 * * * *" \
  "${SERVICE_URL}/tasks/poll-replies" \
  "POST" \
  "BrokerOps reply poller — carrier and MDL vendor Gmail reply processing (every 5 min)" \
  "120s"

# ── Job 3: Assemble outreach batch (new — Manley loads) ──────────────────────
create_job \
  "brokerops-assemble-outreach-batch" \
  "30 8 * * 1-5" \
  "${SERVICE_URL}/tasks/assemble-outreach-batch" \
  "POST" \
  "BrokerOps daily outreach batch — assembles Manley DeBoer load batch, sends Slack approval request" \
  "300s"

# ── Job 4: Process attachments (every 15 min) ────────────────────────────────
create_job \
  "brokerops-process-attachments" \
  "*/15 * * * *" \
  "${SERVICE_URL}/tasks/process-attachments" \
  "POST" \
  "BrokerOps attachment scanner — onboarding docs (COI, W-9, ACH) to Drive + Carrier_Master update" \
  "120s"

# ── Job 5: MDL vendor dispatch (replaces BrokerOps-MDL-Vendor-Dispatcher) ────
create_job \
  "brokerops-mdl-vendor-dispatch" \
  "*/5 * * * *" \
  "${SERVICE_URL}/tasks/mdl-vendor-dispatch" \
  "POST" \
  "BrokerOps MDL vendor outreach — one dispatch cycle + reply sweep (replaces schtask loop)" \
  "120s"

# ── Job 6: Daily report ───────────────────────────────────────────────────────
create_job \
  "brokerops-daily-report" \
  "0 18 * * 1-5" \
  "${SERVICE_URL}/tasks/daily-report" \
  "POST" \
  "BrokerOps daily ops summary — emails Derek loads/carriers/MDL stats at 18:00 ET M-F" \
  "120s"

# ── Job 7: Health check (GET /healthz every 5 min) ───────────────────────────
log "--- Creating health-check job (GET /healthz)"
if [[ "$APPLY" == "true" ]]; then
  gcloud scheduler jobs describe "brokerops-health-check" \
    --location="$REGION" \
    --project="$PROJECT" &>/dev/null \
  && {
    log "  Job brokerops-health-check exists — deleting before recreate"
    gcloud scheduler jobs delete "brokerops-health-check" \
      --location="$REGION" \
      --project="$PROJECT" \
      --quiet 2>&1 | tee -a "$LOG"
  } || true
fi

run gcloud scheduler jobs create http "brokerops-health-check" \
  --project="$PROJECT" \
  --location="$REGION" \
  --schedule="*/5 * * * *" \
  --time-zone="$TIMEZONE" \
  --uri="${SERVICE_URL}/healthz" \
  --http-method="GET" \
  --oidc-service-account-email="$SA_EMAIL" \
  --oidc-token-audience="$SERVICE_URL" \
  --attempt-deadline="30s" \
  --max-retry-attempts="3" \
  --min-backoff="5s" \
  --max-backoff="60s" \
  --description="BrokerOps health check — /healthz every 5 min; failures trigger Slack alert"

# ── Done ──────────────────────────────────────────────────────────────────────
if [[ "$APPLY" == "true" ]]; then
  log "================================================================"
  log "  ALL JOBS CREATED."
  log ""
  log "  Verify:"
  log "    gcloud scheduler jobs list --project=$PROJECT --location=$REGION"
  log ""
  log "  Smoke test one job:"
  log "    gcloud scheduler jobs run brokerops-vetting-sweep --project=$PROJECT --location=$REGION"
  log ""
  log "  Watch logs:"
  log "    gcloud logging read 'resource.type=cloud_run_revision' --project=$PROJECT --limit=50"
  log ""
  log "  After verifying each job fires correctly, decommission Windows schtasks:"
  log "    powershell \"Disable-ScheduledTask -TaskName 'BrokerOps-Vetting-Daily-Sweep'\""
  log "    powershell \"Disable-ScheduledTask -TaskName 'BrokerOps-MDL-Vendor-Dispatcher'\""
  log "    powershell \"Disable-ScheduledTask -TaskName 'BrokerOps-ProcessReplies'\""
  log "    powershell \"Unregister-ScheduledTask -TaskName 'BrokerOps-FollowUp-RefrigeratedExpress' -Confirm:\$false\""
  log "================================================================"
else
  log "================================================================"
  log "  DRY RUN COMPLETE — re-run with --apply to create jobs."
  log "================================================================"
fi
