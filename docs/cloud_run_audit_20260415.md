# BrokerOps Cloud Run Migration — Audit Baseline

**Date:** 2026-04-15
**Author:** Bolt (Backend / Systems Architect)
**Project:** `wide-decoder-489023-p1` (display: "BrokerOps") — us-central1

---

## 1. Dockerfile Status

**File:** `C:/Users/Owner/brokerops-ai/Dockerfile`
**Base image:** `python:3.12-slim` — production-appropriate, minimal.
**Entrypoint:** `gunicorn app.main:app --bind 0.0.0.0:${PORT} --workers 1 --worker-class uvicorn.workers.UvicornWorker --timeout 120`
**Issues found:**
- Runs as root (no `USER` directive). Cloud Run tolerates this but it's a security gap.
- `COPY tests/ tests/` copies test code into production image — unnecessary weight.
- No explicit `EXPOSE` directive (Cloud Run doesn't require it, but it aids documentation).

**Minimal surgical fixes applied (see Dockerfile after this audit):**
- Added `RUN addgroup --system app && adduser --system --ingroup app app` and `USER app`
- Removed `COPY tests/ tests/` — tests don't belong in prod
- Added `EXPOSE 8080`

**Assessment:** Production-ready after the two-line fix above. No base image change needed.

---

## 2. cloudbuild.yaml Status

**File:** `C:/Users/Owner/brokerops-ai/cloudbuild.yaml`
**Status:** Functional but incomplete for our pattern.

**What it does:** Build Docker image → push to Container Registry → `gcloud run deploy`
**Gaps:**
- `--set-env-vars` in the Cloud Build step only injects `GCP_PROJECT_ID`. All other secrets are absent — the deployed container would start with empty env vars for Sheets, Gmail, Slack, etc.
- Uses deprecated Container Registry (`gcr.io`) instead of Artifact Registry (`us-central1-docker.pkg.dev`). Works, but GCR is sunset-announced.
- No `COMMIT_SHA` substitution passed from outside — works fine if triggered by Cloud Build trigger; manual `gcloud builds submit` will use `""` for `$COMMIT_SHA`.

**Assessment:** The `cloudbuild.yaml` is a build-only stub — it does NOT handle secrets injection. That is handled by `scripts/deploy_cloud_run.sh` (see Task 4). Derek should use `deploy_cloud_run.sh` for all production deploys; `cloudbuild.yaml` is for CI-triggered builds if that path is ever wired.

---

## 3. Cloud Run Services — Current State

**Command attempted:** `gcloud run services list --project=wide-decoder-489023-p1 --region=us-central1`
**Result:** Permission denied for `derekndeboer@gmail.com`. The active gcloud account is `derekndeboer@gmail.com` but the project IAM expects `sales@deboerfreight.com`.

**Prior evidence from memory + `scheduler_activation.sh`:**
- Service `brokerops-ai` is referenced in `scheduler_activation.sh` with hardcoded URL: `https://brokerops-ai-oqlgwjslta-uc.a.run.app`
- This URL pattern (`oqlgwjslta`) is a real Cloud Run service hash, NOT a placeholder — the service almost certainly exists.
- **Conclusion:** `brokerops-ai` is live on Cloud Run in us-central1. Current revision status unknown without correct IAM credentials.

**Action for Derek:** Run `gcloud config set account sales@deboerfreight.com && gcloud run services describe brokerops-ai --region=us-central1 --project=wide-decoder-489023-p1` to confirm.

---

## 4. Gmail Auth Mode — Current State

**File:** `C:/Users/Owner/brokerops-ai/app/google_auth.py`
**Mode:** Hybrid — Secret Manager primary, `token.json` fallback for local dev.

**How it works today:**
1. Tries to load `token.json` from CWD (local dev path).
2. Falls back to fetching refresh token from **GCP Secret Manager** (`brokerops-oauth-refresh-token`).
3. Builds `Credentials` from the refresh token + OAuth client config (also from Secret Manager: `brokerops-oauth-client`).

**Important finding:** `google_auth.py` ALREADY uses Secret Manager for the refresh token — this contradicts our "no Secret Manager" protocol. The refresh token lives in Secret Manager, not in vault+.env.

**What this means for Cloud Run:**
- The existing auth path WOULD work on Cloud Run IF the service account (`brokerops-sa@`) has Secret Manager accessor rights and the refresh token is valid.
- However, per `feedback_secret_management_protocol.md`, we should not expand Secret Manager usage. The preferred path going forward is service account with domain-wide delegation (see Task 3).
- The `GMAIL_AUTH_MODE` env var approach (added in Task 3) provides a clean escape hatch without removing the existing OAuth path.

**Risk:** The OAuth refresh token in Secret Manager will expire if not refreshed (user OAuth tokens expire after ~6 months of non-use, or immediately on password change). Service account delegation is the correct headless solution.

---

## 5. requirements.txt — Version Pinning

All packages are pinned with exact versions (`==`). Current state:

```
fastapi==0.115.6
uvicorn[standard]==0.34.0
gunicorn==23.0.0
google-auth==2.37.0
google-auth-oauthlib==1.2.1
google-auth-httplib2==0.2.0
google-api-python-client==2.159.0
google-cloud-secret-manager==2.21.1
httpx==0.28.1
python-dateutil==2.9.0
pydantic==2.10.3
pydantic-settings==2.7.1
openpyxl==3.1.5
playwright==1.58.0
pytest==8.3.4
pytest-asyncio==0.24.0
```

**Issues:**
- `playwright==1.58.0` is included — Playwright requires `playwright install` to download browser binaries after pip install. The Dockerfile does NOT run `playwright install chromium`. This will cause a runtime crash if any Cloud Run route calls PlaywrightFetcher. Playwright is not suitable for Cloud Run serverless (no persistent browser binary, cold start weight). **Cloud Run routes should NOT call Playwright directly.** If enrichment via Playwright is needed, it must run on a separate compute target (Cloud Run Job, Compute Engine).
- `pytest` and `pytest-asyncio` are dev-only dependencies — they add weight to the production image. Should be split to `requirements-dev.txt` eventually. Not blocking.
- `google-cloud-secret-manager` is included — consistent with current auth, even though we're migrating away from Secret Manager for new secrets.
- `cryptography` is NOT listed — required by `hydrate_from_vault.py` (`from cryptography.fernet import Fernet`). This works locally because `cryptography` is likely installed as a transitive dep of `google-auth`. Needs an explicit pin for safety. **Added to requirements.txt.**

---

## 6. Existing schtasks — Inventory

| Task Name | State | Trigger | Script invoked |
|---|---|---|---|
| `BrokerOps-Vetting-Daily-Sweep` | Ready | Daily 04:00 | `scripts/run_vetting_sweep.bat` → `scripts/run_vetting_sweep.py --all` → `sweep_carrier_database()` |
| `BrokerOps-MDL-Vendor-Dispatcher` | Ready | Time trigger (one-time, then every 5 min via loop bat) | `scripts/run_mdl_vendor_loop.bat` → `dispatch_mdl_vendor_outreach.py --once` + `process_mdl_vendor_replies.py --once` |
| `BrokerOps-FollowUp-RefrigeratedExpress` | Ready | Time trigger (old BrokerOps-AI-local path) | `BrokerOps-AI-local/scripts/followup-refrigerated-express.bat` — **DEPRECATED REPO** |
| `BrokerOps-ProcessReplies` | Disabled | Time trigger (old BrokerOps-AI-local path) | `BrokerOps-AI-local/scripts/poll-replies.bat` — **DEPRECATED REPO** |

**Key finding:** `BrokerOps-FollowUp-RefrigeratedExpress` and `BrokerOps-ProcessReplies` still point to the old `BrokerOps-AI-local` TypeScript repo (DEPRECATED). These should be deleted — they point at dead code.

---

## 8. Pivot 2026-04-15 — Workload Identity (SA Key Blocked by Org Policy)

**Blocker:** GCP org policy `constraints/iam.disableServiceAccountKeyCreation` prevents
downloading SA key files. The original plan (key → vault → `GOOGLE_SERVICE_ACCOUNT_JSON`
env var) was abandoned.

**Workaround:** Workload Identity.

- `brokerops-gmail` SA is attached as the Cloud Run runtime identity (revision 00078-vgs).
- Cloud Run's metadata server mints short-lived tokens for the SA on demand.
- No key file exists anywhere — not on disk, not in the vault.
- No rotation needed.

**Code path (app/google_auth.py `_get_service_account_credentials`):**
1. `google.auth.default()` returns Compute Engine credentials from the metadata server.
2. `google.auth.impersonated_credentials.Credentials(source_credentials, target_principal=SA_EMAIL, target_scopes=..., subject=GMAIL_DELEGATE_EMAIL)` wraps those into user-delegated credentials for domain-wide delegation.
3. `googleapiclient.discovery.build('gmail', 'v1', credentials=delegated)` uses those creds.

**Library version:** `google-auth==2.37.0` (pinned in requirements.txt). Supports
`impersonated_credentials.Credentials` with `subject=` (requires >=2.17.0). No upgrade needed.

**Remaining manual step:** Derek must grant domain-wide delegation in Google Workspace Admin
(see Section 3 above for exact scopes). Code is ready; auth will fail until that click is done.

---

## 7. Surprises

1. **Playwright in requirements.txt** — Not installable on Cloud Run without custom base image work. Any Cloud Run route that imports PlaywrightFetcher will fail on cold start. Routes must not call enrichment workflows that use Playwright.
2. **Secret Manager already in use** — `google_auth.py` pulls OAuth credentials from GCP Secret Manager, which conflicts with the vault-only protocol. This is pre-existing technical debt, not introduced today.
3. **Service account name collision** — Memory references `brokerops-sa@wide-decoder-489023-p1.iam.gserviceaccount.com` (existing general SA) and we are creating `brokerops-gmail@wide-decoder-489023-p1.iam.gserviceaccount.com` (new Gmail/Drive SA). These are distinct. The existing SA is used by Cloud Build; the new one is the headless Gmail identity.
4. **`BrokerOps-FollowUp-RefrigeratedExpress` is dead** — Points at deprecated TypeScript repo. Recommend `schtasks /delete /tn "BrokerOps-FollowUp-RefrigeratedExpress" /f` and `schtasks /delete /tn "BrokerOps-ProcessReplies" /f`.
