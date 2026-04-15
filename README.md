# BrokerOps AI

Internal freight brokerage automation system. Runs as a FastAPI service on Google Cloud Run, using Gmail + Google Drive + Google Sheets as the system of record.

**Canonical carrier search protocol:** `docs/carrier_search_protocol.md`

## Last major changes — 2026-04-15

1. **Towing/service-type denylist** — `EXCLUDED_SERVICE_TYPE_PATTERNS` added to `scripts/prospect_carriers.py:76`. Blocks towing, moving, excavating, livestock, logging, and 20+ other non-target service types from entering the DB. Root cause of 21 towing rows in overnight L&I run.
2. **Service Type tagging** — `Service_Type` column on Carrier_Master: General / Heavy Haul / Auto Transport / Fuel. Heavy Haul, Auto Transport, and Fuel are kept (future business), not quarantined.
3. **State backfill** — `scripts/enrich_carriers_playwright.py --backfill-states` fills blank State fields (~42 rows). Integrated into enrichment workflow.
4. **`--state` mode for `prospect_carriers.py`** — new flags: `--state XX`, `--buckets flatbed,dry_van,box_truck,reefer`, `--zip-prefixes`, `--limit N`. Replaces deprecated per-state scripts (MN/OH/TX).
5. **SAFER scraping removed** — `_scrape_safer()` deleted from `app/email_enrichment.py`. Was bot-blocked (JS-gated portal); root-caused 2026-04-13. Enrichment waterfall is now 3-step: Apollo → Google CSE → PHONE_ONLY.

## Architecture Overview

```
Cloud Scheduler  ──(POST /jobs/poll)──►  Cloud Run (FastAPI)
                                              │
                ┌─────────────────────────────┤
                ▼              ▼              ▼              ▼
            Gmail API    Sheets API     Drive API     CarrierOK API
            (read/send/  (Carrier &     (folders,      (authority &
             label)       Load Master)   templates,     insurance
                                         PDF export)    verification)
```

## Processing Pipeline

Each `/jobs/poll` invocation runs the full cycle:

1. **Load Ingestion** – reads `OPS/NEW_LOAD` emails, parses fields, generates `Load_ID`, inserts into `Load_Master`, creates Drive folder.
2. **Carrier Sourcing** – for `NEW` loads: filters eligible carriers by equipment + compliance, ranks by lane match / on-time score / claims, sends RFQ emails to top 5.
3. **RFQ Expansion** – for `RFQ_SENT` loads stale > 2 hours: sends next batch of 5 carriers.
4. **Quote Processing** – parses carrier replies ("first dollar amount" rule), selects lowest valid rate (tie-break: On_Time_Score, Last_Load_Date).
5. **Onboarding** – if selected carrier is missing W9: sends onboarding request, watches for document submissions, updates carrier record.
6. **Approval Gate** – sends approval packet to `Broker_Operations_Email`; watches for exact `APPROVE {Load_ID}` reply from broker.
7. **Rate Confirmation** – copies Google Doc template, replaces placeholders, exports PDF, emails to carrier, stores in Drive.
8. **Compliance Sync** – calls CarrierOK API for all carriers assigned to active loads.

## Dispatch Eligibility Rules

A carrier is dispatch-eligible only if ALL conditions are met:
- `Authority_Status` = `ACTIVE`
- `Compliance_Status` = `CLEAR`
- `Insurance_Expiration` >= today
- `Auto_Liability_Coverage` >= 1,000,000
- `Cargo_Coverage` >= 100,000
- `W9_On_File` = TRUE
- `Active` = TRUE

## Project Structure

```
app/
  main.py              FastAPI application + endpoints
  config.py            Environment-based configuration (Pydantic Settings)
  google_auth.py       OAuth2 + Secret Manager credential management
  gmail.py             Gmail API helpers (search, send, label)
  sheets.py            Sheets API helpers (CRUD for Carrier/Load Master)
  drive.py             Drive API helpers (folders, template copy, PDF export)
  carrierok.py         CarrierOK API integration
  parsers.py           Email parsing (load, quote, approval)
  workflows/
    load_ingestion.py    OPS/NEW_LOAD → Load_Master + Drive folder
    carrier_sourcing.py  Filter/rank carriers → send RFQ emails
    quote_processing.py  Parse replies → select carrier
    onboarding.py        Request + process W9/COI documents
    approval.py          Send approval packets → process APPROVE/REJECT
    rate_confirmation.py Google Doc template → PDF → email carrier
    compliance_sync.py   CarrierOK API → update Carrier_Master
tests/
  test_parsers.py      Unit tests for all parsing logic
  conftest.py          Test configuration
Dockerfile             Production container image
cloudbuild.yaml        Cloud Build + Cloud Run deployment config
requirements.txt       Python dependencies
```

## Required Secrets (Google Secret Manager)

| Secret Name | Contents |
|---|---|
| `brokerops-oauth-client` | Full OAuth2 client JSON (web type) from GCP Console |
| `brokerops-oauth-refresh-token` | OAuth refresh token (populated via `/oauth/start` flow) |
| `brokerops-carrierok-api-key` | CarrierOK API key |

## Environment Variables

| Variable | Description | Example |
|---|---|---|
| `GCP_PROJECT_ID` | GCP project ID | `brokerops-prod` |
| `CARRIER_MASTER_SHEET_ID` | Spreadsheet ID for Carrier_Master | `1AbC...` |
| `LOAD_MASTER_SHEET_ID` | Spreadsheet ID for Load_Master | `1XyZ...` |
| `BROKEROPS_ROOT_FOLDER_ID` | Drive folder ID for BrokerOps root | `1a2b3c...` |
| `LOADS_FOLDER_ID` | Drive folder ID for BrokerOps/Loads | `4d5e6f...` |
| `CARRIERS_FOLDER_ID` | Drive folder ID for BrokerOps/Carriers | `7g8h9i...` |
| `TEMPLATES_FOLDER_ID` | Drive folder ID for BrokerOps/Templates | `0j1k2l...` |
| `RATE_CONFIRMATION_TEMPLATE_ID` | Google Doc ID for Rate Confirmation template | `3m4n5o...` |
| `BROKER_EMAIL` | Gmail address used for operations | `ops@yourdomain.com` |
| `SERVICE_URL` | Public URL of the Cloud Run service | `https://brokerops-ai-xxx.run.app` |

Optional overrides: `RFQ_BATCH_SIZE` (default 5), `RFQ_EXPANSION_DELAY_SECONDS` (default 7200), `MIN_AUTO_LIABILITY` (default 1000000), `MIN_CARGO_COVERAGE` (default 100000), `CARRIEROK_API_BASE_URL`.

## Local Development

### 1. Prerequisites
- Python 3.11+
- A GCP project with Gmail, Drive, Sheets APIs enabled
- OAuth2 client credentials (download JSON from GCP Console)

### 2. Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Create a `.env` file

```env
GCP_PROJECT_ID=your-project-id
CARRIER_MASTER_SHEET_ID=...
LOAD_MASTER_SHEET_ID=...
BROKEROPS_ROOT_FOLDER_ID=...
LOADS_FOLDER_ID=...
CARRIERS_FOLDER_ID=...
TEMPLATES_FOLDER_ID=...
RATE_CONFIRMATION_TEMPLATE_ID=...
BROKER_EMAIL=your-email@yourdomain.com
SERVICE_URL=http://localhost:8000
```

### 4. Bootstrap OAuth (first time)

For local dev, you can either:

**Option A – Web flow:**
```bash
uvicorn app.main:app --reload --port 8000
# Visit http://localhost:8000/oauth/start in your browser
# Complete the consent screen; the refresh token is stored in Secret Manager
```

**Option B – Manual token.json:**
```bash
# Use the Google OAuth Playground or a quick script to get a token.json
# Place it in the project root (it's in .gitignore)
```

### 5. Run the server

```bash
uvicorn app.main:app --reload --port 8000
```

### 6. Trigger a poll manually

```bash
curl -X POST http://localhost:8000/jobs/poll
```

### 7. Run tests

```bash
pytest tests/ -v
```

## Deploying to Cloud Run

### Prerequisites
- GCP project with Cloud Run, Secret Manager, and required APIs enabled
- Service account `brokerops-sa@PROJECT_ID.iam.gserviceaccount.com` with roles: Secret Manager Secret Accessor, plus any needed for Gmail/Drive/Sheets
- Secrets created in Secret Manager (see table above)

### Deploy via Cloud Build

```bash
gcloud builds submit --config cloudbuild.yaml .
```

### Deploy manually

```bash
# Build
gcloud builds submit --tag gcr.io/PROJECT_ID/brokerops-ai

# Deploy
gcloud run deploy brokerops-ai \
  --image gcr.io/PROJECT_ID/brokerops-ai \
  --region us-central1 \
  --no-allow-unauthenticated \
  --memory 512Mi \
  --timeout 300 \
  --set-env-vars "GCP_PROJECT_ID=PROJECT_ID,CARRIER_MASTER_SHEET_ID=...,LOAD_MASTER_SHEET_ID=...,..." \
  --service-account brokerops-sa@PROJECT_ID.iam.gserviceaccount.com
```

### Set up Cloud Scheduler

```bash
# Get the Cloud Run service URL
SERVICE_URL=$(gcloud run services describe brokerops-ai --region us-central1 --format='value(status.url)')

# Create a scheduler job that polls every 5 minutes
gcloud scheduler jobs create http brokerops-poll \
  --location us-central1 \
  --schedule "*/5 * * * *" \
  --uri "${SERVICE_URL}/jobs/poll" \
  --http-method POST \
  --oidc-service-account-email brokerops-sa@PROJECT_ID.iam.gserviceaccount.com \
  --oidc-token-audience "${SERVICE_URL}"
```

### Trigger /jobs/poll manually (authenticated)

```bash
TOKEN=$(gcloud auth print-identity-token --audiences=SERVICE_URL)
curl -X POST -H "Authorization: Bearer $TOKEN" SERVICE_URL/jobs/poll
```

## Idempotency

Every Gmail message ID is recorded in the `Processed` tab of the Load_Master spreadsheet before any side effects. Re-running `/jobs/poll` will skip already-processed messages. The tab is auto-created on first use.

## Assumptions & Design Decisions

1. **OAuth2 web flow** chosen over domain-wide delegation for MVP simplicity. The refresh token is stored in Secret Manager and refreshed automatically.
2. **Polling via Cloud Scheduler** chosen over Gmail push notifications for MVP. Push notifications can be added later by implementing `users.watch` and a `/webhooks/gmail` endpoint.
3. **Google Sheets as data store** – no external database. The `Processed` tab provides idempotency. This works for MVP scale (hundreds of loads/carriers). For production scale, migrate to Cloud SQL or Firestore.
4. **Carrier_Master Sheet1 tab** – the implementation reads from `Sheet1`. Rename the tab if needed or update the range in `sheets.py`.
5. **Load_Master Loads tab** – load data is stored in a tab named `Loads`. The `Settings` tab holds counters and broker constants.
6. **Time zones** – all times are assumed origin-local per spec. No timezone conversion is applied.
7. **"First dollar amount" rule** – the quote parser treats the first `$X,XXX` pattern in a carrier's reply as their rate quote.
8. **RFQ expansion** – runs every poll cycle. If a load has been in `RFQ_SENT` for >= 2 hours since last update, the next batch of 5 carriers is contacted.
9. **Rate confirmation template** uses `{Placeholder}` syntax. Ensure your Google Doc template uses these exact placeholders (see `rate_confirmation.py` for the full list).
10. **Scoring strategy** – carrier ranking uses simple sorting (lane match → On_Time_Score → Claims_Count → Last_Load_Date). Designed for future pluggable scoring by extracting the sort key into a strategy pattern.
11. **No SMS** – email only for MVP.
12. **Compliance provider** – CarrierOK API integration handles authority status normalization for various response formats (AUTHORIZED→ACTIVE, REVOKED→INACTIVE, etc.).

## API Endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/health` | None | Health check |
| GET | `/oauth/start` | None | Initiate OAuth2 consent flow |
| GET | `/oauth/callback` | None | OAuth2 callback (exchanges code for tokens) |
| POST | `/jobs/poll` | IAM (Cloud Scheduler) | Full processing cycle |
| POST | `/jobs/compliance` | IAM | Compliance sync only |
