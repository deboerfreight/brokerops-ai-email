#!/usr/bin/env bash
# =============================================================================
# BrokerOps — Deploy to Cloud Run
#
# Pulls secrets from the org vault, builds --set-env-vars for gcloud run deploy,
# and deploys the brokerops-ai service to Cloud Run.
#
# Usage (dry-run — prints the gcloud command, does NOT execute):
#   ./scripts/deploy_cloud_run.sh
#
# Usage (live deploy):
#   ./scripts/deploy_cloud_run.sh --apply
#
# Pre-reqs:
#   1. gcloud authed as sales@deboerfreight.com (project owner)
#   2. Docker image already built and pushed (run cloudbuild.yaml first)
#   3. brokerops-gmail SA created (scripts/create_service_account.sh --apply)
#   4. Domain-wide delegation granted in Google Workspace Admin (Derek manual step)
#   5. org vault has all operation-tier secrets current
#
# Secrets policy:
#   All secrets flow through vault -> hydrate_from_vault() -> env var injection
#   at deploy time.  Values are transmitted once via gcloud CLI to Cloud Run's
#   managed environment.  They never touch disk or Secret Manager.
#   Every secret rotation requires a redeploy (accepted cost per protocol).
#
# Cost note:
#   Cloud Run charges only for request execution time.  An idle service costs ~$0.
#   See scripts/setup_cloud_scheduler.sh for estimated monthly costs.
# =============================================================================

set -euo pipefail

# ── Config ───────────────────────────────────────────────────────────────────
PROJECT="wide-decoder-489023-p1"
REGION="us-central1"
SERVICE="brokerops-ai"
SA_EMAIL="brokerops-gmail@${PROJECT}.iam.gserviceaccount.com"
MEMORY="512Mi"
TIMEOUT="300"
MAX_INSTANCES="3"
CONCURRENCY="10"

# Python interpreter — use same one Derek has configured
PYTHON="${PYTHON:-C:/Python314/python.exe}"

LOG="C:/Users/Owner/brokerops-ai/scripts/logs/cloud_run_migration_20260415.log"
mkdir -p "$(dirname "$LOG")"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$LOG"; }

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
  log "  DRY RUN — no deploy will be executed."
  log "  Re-run with --apply to deploy for real."
  log "================================================================"
fi

# ── Step 1: Pin image to latest tag in the dedicated AR repo ────────────────
# cloudbuild.yaml always tags both BUILD_ID and latest, so :latest always
# points at the freshest build.
log "Step 1: Resolving latest image"
IMAGE="us-central1-docker.pkg.dev/${PROJECT}/brokerops-ai/brokerops-ai:latest"

# Sanity check: confirm the image exists before trying to deploy it
if ! gcloud artifacts docker images describe "$IMAGE" --project="$PROJECT" &>/dev/null; then
  log "ERROR: Image not found at $IMAGE"
  log "  Run: gcloud builds submit --config cloudbuild.yaml . --project=$PROJECT"
  exit 1
fi

log "  Using image: $IMAGE"

# ── Step 2: Hydrate secrets from vault (dry-run first to enumerate keys) ─────
log "Step 2: Enumerating secrets from vault (dry-run)"

VAULT_DB="C:/Users/Owner/Desktop/Claude Work/team/org/org.db"
VAULT_KEY="C:/Users/Owner/Desktop/Claude Work/team/org/.vault_key"

ENV_VARS_JSON=$("$PYTHON" - <<'PYEOF'
import sys, json, sqlite3, os
from pathlib import Path
from cryptography.fernet import Fernet

vault_db = Path("C:/Users/Owner/Desktop/Claude Work/team/org/org.db")
vault_key_file = Path("C:/Users/Owner/Desktop/Claude Work/team/org/.vault_key")

if not vault_db.exists():
    print(json.dumps({"error": f"Vault DB not found: {vault_db}"}))
    sys.exit(1)
if not vault_key_file.exists():
    print(json.dumps({"error": f"Vault key not found: {vault_key_file}"}))
    sys.exit(1)

fernet = Fernet(vault_key_file.read_bytes().strip())
conn = sqlite3.connect(str(vault_db))
conn.row_factory = sqlite3.Row

# Fetch operations tier (the tier injected into Cloud Run)
rows = conn.execute(
    "SELECT key_name, encrypted_value FROM vault WHERE access_tier = 'operations' ORDER BY id"
).fetchall()
conn.close()

secrets = {}
for row in rows:
    try:
        val = fernet.decrypt(row["encrypted_value"]).decode()
        secrets[row["key_name"]] = val
    except Exception as exc:
        print(f"[WARN] Could not decrypt {row['key_name']}: {exc}", file=sys.stderr)

print(json.dumps(secrets))
PYEOF
)

if echo "$ENV_VARS_JSON" | python -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if 'error' not in d else 1)" 2>/dev/null; then
  SECRET_COUNT=$(echo "$ENV_VARS_JSON" | python -c "import sys,json; print(len(json.load(sys.stdin)))")
  log "  Loaded $SECRET_COUNT secret(s) from vault (tier: operations)"
else
  ERROR=$(echo "$ENV_VARS_JSON" | python -c "import sys,json; d=json.load(sys.stdin); print(d.get('error','unknown'))")
  log "ERROR: Vault read failed: $ERROR"
  exit 1
fi

# ── Step 3: Build --set-env-vars argument ────────────────────────────────────
log "Step 3: Building --set-env-vars argument"

# Use pipe (|) as the KEY=VALUE pair delimiter via gcloud's ^DELIM^ prefix.
# Pipe is chosen because it does NOT appear in email addresses (the @ in
# GMAIL_DELEGATE_EMAIL=sales@deboerfreight.com collides with @ as a delimiter),
# and is extremely unlikely to appear in any vault secret value.

# Static config — keys that are not secrets
STATIC_ENV="GCP_PROJECT_ID=${PROJECT}|GCP_REGION=${REGION}|GMAIL_AUTH_MODE=service_account|GMAIL_DELEGATE_EMAIL=sales@deboerfreight.com"

ENV_VARS_ARG=$("$PYTHON" - <<PYEOF
import sys, json

secrets = json.loads('''$ENV_VARS_JSON''')

# Build KEY=VALUE pairs joined by pipe. If any value contains a pipe,
# that's a bug — log it so we can switch delimiters.
pairs = []
for k, v in secrets.items():
    if "|" in v:
        print(f"[ERROR] Secret {k} contains a pipe character — delimiter collision!", file=sys.stderr)
        sys.exit(1)
    pairs.append(f"{k}={v}")

print("|".join(pairs))
PYEOF
)

FULL_ENV="${STATIC_ENV}|${ENV_VARS_ARG}"

# ── Build a REDACTED version of ENV_VARS_ARG for log output ──────────────────
# Vault values are redacted to "<N chars>" so they never appear in logs/stdout.
# Static config vars are kept as-is (they're not secrets).
REDACTED_ENV_VARS_ARG=$("$PYTHON" - <<PYEOF
env_arg = '''$ENV_VARS_ARG'''
parts = env_arg.split("|") if env_arg else []
redacted = []
for p in parts:
    if "=" not in p:
        redacted.append(p)
        continue
    k, v = p.split("=", 1)
    redacted.append(f"{k}=<{len(v)} chars>")
print("|".join(redacted))
PYEOF
)
REDACTED_FULL_ENV="${STATIC_ENV}|${REDACTED_ENV_VARS_ARG}"

# ── Step 4: Get git commit SHA for logging ───────────────────────────────────
COMMIT_SHA=$(git -C "C:/Users/Owner/brokerops-ai" rev-parse --short HEAD 2>/dev/null || echo "unknown")
log "  Commit SHA: $COMMIT_SHA"

# ── Step 5: Run gcloud run deploy ────────────────────────────────────────────
log "Step 5: Deploying $SERVICE to Cloud Run"

DEPLOY_CMD=(
  gcloud run deploy "$SERVICE"
  "--image=$IMAGE"
  "--region=$REGION"
  "--project=$PROJECT"
  "--platform=managed"
  "--no-allow-unauthenticated"
  "--memory=$MEMORY"
  "--timeout=$TIMEOUT"
  "--max-instances=$MAX_INSTANCES"
  "--concurrency=$CONCURRENCY"
  "--service-account=$SA_EMAIL"
  "--set-env-vars=^|^${FULL_ENV}"
)

# Redacted display version — same args but with secret values masked
DEPLOY_CMD_DISPLAY=(
  gcloud run deploy "$SERVICE"
  "--image=$IMAGE"
  "--region=$REGION"
  "--project=$PROJECT"
  "--platform=managed"
  "--no-allow-unauthenticated"
  "--memory=$MEMORY"
  "--timeout=$TIMEOUT"
  "--max-instances=$MAX_INSTANCES"
  "--concurrency=$CONCURRENCY"
  "--service-account=$SA_EMAIL"
  "--set-env-vars=^|^${REDACTED_FULL_ENV}"
)

if [[ "$APPLY" == "true" ]]; then
  log "+ ${DEPLOY_CMD_DISPLAY[*]}"
  # Execute the REAL command — its stdout/stderr go to tee, but gcloud run
  # deploy itself does NOT echo env var values, so the secret never reaches
  # the log through the tee pipe.
  "${DEPLOY_CMD[@]}" 2>&1 | tee -a "$LOG"

  # ── Step 6: Log deployed revision URL ────────────────────────────────────
  log "Step 6: Fetching deployed revision URL"
  SERVICE_URL=$(gcloud run services describe "$SERVICE" \
    --region="$REGION" \
    --project="$PROJECT" \
    --format="value(status.url)" 2>/dev/null || echo "unknown")
  log "  Deployed service URL: $SERVICE_URL"
  log "  Commit SHA:            $COMMIT_SHA"
  log "  Image:                 $IMAGE"

  log "================================================================"
  log "  DEPLOY COMPLETE"
  log "  Service URL: $SERVICE_URL"
  log "  Next: run scripts/setup_cloud_scheduler.sh --apply"
  log "================================================================"
else
  log ""
  log "  [DRY-RUN] Would run (secret values redacted for log safety):"
  log "    ${DEPLOY_CMD_DISPLAY[*]}"
  log ""
  log "  Secret count to inject: $(echo "$ENV_VARS_ARG" | tr '@' '\n' | wc -l) vault secrets + 4 static vars"
  log "  Image: $IMAGE"
  log "  Commit: $COMMIT_SHA"
  log ""
  log "================================================================"
  log "  DRY RUN COMPLETE — re-run with --apply to deploy."
  log "================================================================"
fi
