#!/usr/bin/env bash
#
# BrokerOps AI — Cloud Scheduler activation (DRY-RUN BY DEFAULT)
#
# Creates three scheduled jobs to poll Cloud Run endpoints on deBoer's
# wide-decoder-489023-p1 project. The script PRINTS commands by default.
# Pass --apply to actually execute. Refuses to run without --apply.
#
# Usage:
#   ./scripts/scheduler_activation.sh            # dry-run (prints commands)
#   ./scripts/scheduler_activation.sh --apply    # execute for real
#
# Jobs:
#   brokerops-poll-business   /jobs/poll        */15 6-20 * * 1-5  (ET)
#   brokerops-poll-offhours   /jobs/poll        0 */2 * * *
#   brokerops-compliance-daily /jobs/compliance 0 7 * * *
#
# Pre-steps:
#   1. Clamp Cloud Run --max-instances=1 --concurrency=1 (concurrency guard).
#   2. Grant SA roles/run.invoker on the service.
#
# Post-step:
#   3. Smoke-test: gcloud scheduler jobs run <name>
#

set -euo pipefail

PROJECT="wide-decoder-489023-p1"
REGION="us-central1"
SERVICE="brokerops-ai"
SERVICE_URL="https://brokerops-ai-oqlgwjslta-uc.a.run.app"
SA_EMAIL="brokerops-sa@wide-decoder-489023-p1.iam.gserviceaccount.com"
TIMEZONE="America/New_York"

# ── Mode switch ───────────────────────────────────────────────────────────
APPLY="false"
for arg in "$@"; do
  case "$arg" in
    --apply) APPLY="true" ;;
    -h|--help)
      sed -n '2,30p' "$0"
      exit 0
      ;;
    *) echo "Unknown flag: $arg" >&2; exit 2 ;;
  esac
done

if [[ "$APPLY" != "true" ]]; then
  echo "======================================================================"
  echo "  DRY RUN — no commands will be executed."
  echo "  To actually apply these changes, re-run with: $0 --apply"
  echo "======================================================================"
  echo
fi

# ── Helper: either echo or execute ────────────────────────────────────────
run() {
  if [[ "$APPLY" == "true" ]]; then
    echo "+ $*"
    "$@"
  else
    echo "  $*"
  fi
}

echo "### Pre-step 1: Clamp Cloud Run concurrency (max 1 instance, 1 req at a time)"
run gcloud run services update "$SERVICE" \
  --region "$REGION" \
  --project "$PROJECT" \
  --max-instances=1 \
  --concurrency=1
echo

echo "### Pre-step 2: Grant run.invoker to service account"
run gcloud run services add-iam-policy-binding "$SERVICE" \
  --region "$REGION" \
  --project "$PROJECT" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/run.invoker"
echo

# ── Job 1: Business hours poll ─────────────────────────────────────────────
echo "### Create job: brokerops-poll-business (business hours, every 15 min M-F)"
run gcloud scheduler jobs create http brokerops-poll-business \
  --project "$PROJECT" \
  --location "$REGION" \
  --schedule="*/15 6-20 * * 1-5" \
  --time-zone="$TIMEZONE" \
  --uri="${SERVICE_URL}/jobs/poll" \
  --http-method=POST \
  --oidc-service-account-email="$SA_EMAIL" \
  --oidc-token-audience="$SERVICE_URL" \
  --attempt-deadline=540s \
  --max-retry-attempts=1 \
  --description="BrokerOps poll during business hours (ET)"
echo

# ── Job 2: Off-hours poll ──────────────────────────────────────────────────
echo "### Create job: brokerops-poll-offhours (every 2 hours, 24/7)"
run gcloud scheduler jobs create http brokerops-poll-offhours \
  --project "$PROJECT" \
  --location "$REGION" \
  --schedule="0 */2 * * *" \
  --time-zone="$TIMEZONE" \
  --uri="${SERVICE_URL}/jobs/poll" \
  --http-method=POST \
  --oidc-service-account-email="$SA_EMAIL" \
  --oidc-token-audience="$SERVICE_URL" \
  --attempt-deadline=540s \
  --max-retry-attempts=1 \
  --description="BrokerOps poll during off-hours"
echo

# ── Job 3: Compliance daily ────────────────────────────────────────────────
echo "### Create job: brokerops-compliance-daily (every day 07:00 ET)"
run gcloud scheduler jobs create http brokerops-compliance-daily \
  --project "$PROJECT" \
  --location "$REGION" \
  --schedule="0 7 * * *" \
  --time-zone="$TIMEZONE" \
  --uri="${SERVICE_URL}/jobs/compliance" \
  --http-method=POST \
  --oidc-service-account-email="$SA_EMAIL" \
  --oidc-token-audience="$SERVICE_URL" \
  --attempt-deadline=900s \
  --max-retry-attempts=2 \
  --description="BrokerOps daily compliance sync"
echo

# ── Smoke test ─────────────────────────────────────────────────────────────
echo "### Smoke test (trigger each job once on-demand)"
for job in brokerops-poll-business brokerops-poll-offhours brokerops-compliance-daily; do
  run gcloud scheduler jobs run "$job" \
    --project "$PROJECT" \
    --location "$REGION"
done
echo

if [[ "$APPLY" != "true" ]]; then
  echo "======================================================================"
  echo "  DRY RUN COMPLETE — nothing was applied."
  echo "  Re-run with --apply to create the jobs for real."
  echo "======================================================================"
else
  echo "======================================================================"
  echo "  APPLIED. Verify with:"
  echo "    gcloud scheduler jobs list --project $PROJECT --location $REGION"
  echo "======================================================================"
fi
