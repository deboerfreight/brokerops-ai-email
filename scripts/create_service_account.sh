#!/usr/bin/env bash
# =============================================================================
# BrokerOps — Create brokerops-gmail service account (Workload Identity)
#
# Pivot 2026-04-15: SA key creation is blocked by org policy
# (constraints/iam.disableServiceAccountKeyCreation).  Using Workload Identity
# instead — the SA is attached as the Cloud Run runtime identity.  Tokens are
# minted from the Cloud Run metadata server on demand.  No key file ever exists.
#
# Run this ONCE from Derek's terminal after:
#   gcloud config set account sales@deboerfreight.com
#   gcloud auth login
#
# What it does:
#   1. Creates the service account brokerops-gmail in wide-decoder-489023-p1
#      (idempotent — skips if already exists)
#   2. Grants run.invoker on the brokerops-ai Cloud Run service
#   3. Attaches SA as the Cloud Run runtime identity (Workload Identity)
#
# Pre-req: gcloud authed as sales@deboerfreight.com (project owner)
# DO NOT run as --apply until gcloud is authed. Default is dry-run.
# =============================================================================

set -euo pipefail

PROJECT="wide-decoder-489023-p1"
REGION="us-central1"
SA_NAME="brokerops-gmail"
SA_EMAIL="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
SA_DISPLAY="BrokerOps Gmail/Drive"
SERVICE="brokerops-ai"

LOG="C:/Users/Owner/brokerops-ai/scripts/logs/cloud_run_migration_20260415.log"
mkdir -p "$(dirname "$LOG")"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$LOG"; }

APPLY="false"
for arg in "$@"; do
  case "$arg" in
    --apply) APPLY="true" ;;
    -h|--help) grep '^#' "$0" | sed 's/^# //;s/^#//'; exit 0 ;;
    *) echo "Unknown flag: $arg" >&2; exit 2 ;;
  esac
done

run() {
  if [[ "$APPLY" == "true" ]]; then
    log "+ $*"
    "$@"
  else
    log "  [DRY-RUN] $*"
  fi
}

if [[ "$APPLY" != "true" ]]; then
  log "================================================================"
  log "  DRY RUN — no changes applied.  Re-run with --apply to execute."
  log "================================================================"
fi

# ── Step 1: Create service account (idempotent) ───────────────────────────────
log "Step 1: Create service account $SA_EMAIL (skip if already exists)"
if [[ "$APPLY" == "true" ]]; then
  if gcloud iam service-accounts describe "$SA_EMAIL" --project="$PROJECT" &>/dev/null; then
    log "  SA $SA_EMAIL already exists — skipping create."
  else
    gcloud iam service-accounts create "$SA_NAME" \
      --display-name="$SA_DISPLAY" \
      --project="$PROJECT"
    log "  SA $SA_EMAIL created."
  fi
else
  log "  [DRY-RUN] gcloud iam service-accounts create $SA_NAME --display-name=$SA_DISPLAY --project=$PROJECT"
fi

# ── Step 2: Grant run.invoker to SA on Cloud Run service ─────────────────────
log "Step 2: Grant roles/run.invoker to $SA_EMAIL on $SERVICE"
run gcloud run services add-iam-policy-binding "$SERVICE" \
  --region="$REGION" \
  --project="$PROJECT" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/run.invoker"

# ── Step 3: Attach SA as Cloud Run runtime identity (Workload Identity) ───────
log "Step 3: Attach $SA_EMAIL as runtime identity for $SERVICE (Workload Identity)"
run gcloud run services update "$SERVICE" \
  --service-account="$SA_EMAIL" \
  --region="$REGION" \
  --project="$PROJECT"

# ── Done ──────────────────────────────────────────────────────────────────────
if [[ "$APPLY" == "true" ]]; then
  log "================================================================"
  log "  Service account setup complete (Workload Identity)."
  log "  SA email: $SA_EMAIL"
  log "  Runtime identity attached: Cloud Run will mint tokens via metadata server."
  log "  No key file created. No vault entry needed."
  log "  Next: Derek must grant domain-wide delegation in Google Workspace Admin."
  log "  See: docs/cloud_run_audit_20260415.md — Section 3"
  log "================================================================"
  log ""
  log "  Derek — domain-wide delegation steps:"
  log "  1. Open: https://admin.google.com/ac/owl/domainwidedelegation"
  log "  2. Click 'Add new'"
  log "  3. Client ID: run 'gcloud iam service-accounts describe $SA_EMAIL --format=value(uniqueId)'"
  log "  4. OAuth Scopes (paste all, comma-separated):"
  log "     https://www.googleapis.com/auth/gmail.send,"
  log "     https://www.googleapis.com/auth/gmail.readonly,"
  log "     https://www.googleapis.com/auth/gmail.labels,"
  log "     https://www.googleapis.com/auth/gmail.modify,"
  log "     https://www.googleapis.com/auth/drive.file"
  log "  5. Click Authorize"
  log "  6. Impersonated account: sales@deboerfreight.com"
  log "================================================================"
else
  log "================================================================"
  log "  DRY RUN complete.  Re-run with --apply to execute."
  log "================================================================"
fi
