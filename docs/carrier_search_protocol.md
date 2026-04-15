# BrokerOps — Carrier Search Protocol

**Last updated:** 2026-04-15
**Status:** Canonical. This is the single source of truth for how new carriers enter the system.

---

## 1. The One Canonical Flow

```
L&I SQLite sourcing
        │
        ▼
search_cluster_carriers()       ← denylist check here
        │
        ▼
enrich_and_store()              ← insert_carrier() + email enrichment waterfall
        │
        ▼
enrich_carriers_playwright.py   ← state backfill + Playwright enrichment (manual, on demand)
        │
        ▼
app/vetting/ (RULES + vet_complete)  ← quarantine gate inside insert_carrier()
        │
        ▼
Carrier_Master sheet (main tab or Carrier Quarantine tab)
```

| Step | File | Function |
|---|---|---|
| Sourcing | `app/vetting/li_insurance_lookup.py` | `search_carriers_by_state()` |
| Denylist | `scripts/prospect_carriers.py:76` | `EXCLUDED_SERVICE_TYPE_PATTERNS` (regex constant) |
| Hydration | `app/fmcsa.py` | `get_carrier_details()` |
| Scoring | `app/fmcsa.py` | `score_carrier()` |
| Vetting | `app/vetting/rules.py` | `RULES` singleton + `vet_complete()` |
| Insert + quarantine gate | `app/sheets.py` | `insert_carrier()` |
| Email enrichment | `app/email_enrichment.py` | `enrich_carrier_email()` |
| State backfill | `scripts/enrich_carriers_playwright.py` | `backfill_blank_states()` |

---

## 2. Entry Points

**There is exactly one sourcing entry point:**

```bash
python -m scripts.prospect_carriers --state XX --buckets flatbed,dry_van,box_truck,reefer --limit N
```

Or for cluster mode (geo-lane targeting):

```bash
python -m scripts.prospect_carriers --cluster SOUTH_FL --limit 30
```

**Manual only. No RFQ-triggered auto-search.** Per `feedback_carrier_search_manual.md`:
- Do not write scripts that call `insert_carrier()` directly without running `EXCLUDED_SERVICE_TYPE_PATTERNS`.
- Do not add logic that triggers carrier search from an incoming RFQ or scheduler job.
- Build the DB manually until we have enough carriers to train on.

State-mode examples:

```bash
# Minnesota — 5 per bucket, 3 buckets
python -m scripts.prospect_carriers --state MN --buckets flatbed,dry_van,box_truck --limit 5

# Ohio — 10 per bucket, reefer included
python -m scripts.prospect_carriers --state OH --buckets flatbed,dry_van,reefer,box_truck --limit 10

# Texas — 10 per bucket with ZIP narrowing
python -m scripts.prospect_carriers --state TX --buckets flatbed,dry_van,reefer,box_truck --limit 10

# Dry run (no writes)
python -m scripts.prospect_carriers --state MN --buckets flatbed,dry_van --limit 5 --dry-run
```

---

## 3. Denylist

**Constant:** `EXCLUDED_SERVICE_TYPE_PATTERNS` at `scripts/prospect_carriers.py:76`

Applied inside `search_cluster_carriers()` after hydration and scoring, before any insert.
Checks both `Legal_Name` and `DBA_Name`.

Current denylist keywords (see constant for full regex):

- towing, wrecker, recovery
- passenger, bus, coach, shuttle, tours, charter
- excavating, grading, paving, concrete
- waste, garbage, refuse, disposal, sanitation, septic, roll-off
- landscaping, lawn care, arborist
- oilfield, fracturing, drilling
- logging, timber, pulpwood
- livestock, cattle, equine
- van lines, movers, moving

**Policy source:** `feedback_carrier_category_rules.md`

To add a new pattern: extend the regex in `prospect_carriers.py:76`. To temporarily disable: comment out (don't delete) and add a note.

---

## 4. Service Type Tags

The `Service_Type` column on Carrier_Master is the canonical classification.

| Tag | When assigned | Notes |
|---|---|---|
| `General` | Default for flatbed, dry van, box truck, reefer carriers | Most carriers |
| `Heavy Haul` | Name-matched at ingest (specialty flatbed, oversized) | Keep in DB, skip standard outreach |
| `Auto Transport` | Name-matched at ingest (car haulers) | Keep in DB, skip standard outreach |
| `Fuel` | Name-matched at ingest (tanker, petroleum, propane) | Keep in DB, skip standard outreach |

Heavy Haul / Auto Transport / Fuel are NOT on the hard denylist — they are valid future-business categories. They get tagged and retained, not quarantined. See `feedback_carrier_category_rules.md` for the rescue rule (Stewart's Grading & Hauling pattern).

---

## 5. Enrichment Layers

`app/email_enrichment.py` runs a 3-step waterfall on every new carrier:

| Step | Source | Returns | Status |
|---|---|---|---|
| 1 | Apollo.io (`_search_apollo`) | email, website | Live (API key in `.env`) — parked on free tier, limited results |
| 2 | Brave Search (`_search_brave`) | email, website | Pending — `BRAVE_SEARCH_API_KEY` needed in vault → `.env` |
| 3 | PHONE_ONLY fallback | None | Last resort |

**Note:** SAFER scraping (`_scrape_safer`) was removed 2026-04-15. SAFER web portal is JS-gated and returns bot-block walls on plain httpx requests (root-caused 2026-04-13). The Playwright path handles website discovery for carriers where Apollo/Brave miss.

**Playwright enrichment** (`scripts/enrich_carriers_playwright.py`): run manually when Apollo/Brave hit rates are low.

```bash
# Enrich carriers missing email
python scripts/enrich_carriers_playwright.py

# Backfill missing State fields
python scripts/enrich_carriers_playwright.py --backfill-states
```

**Apollo is currently on a free/low-tier plan.** People-search returns limited results. Step up the plan before relying on Apollo for bulk enrichment.

---

## 6. Vetting Thresholds

Source of truth: `app/vetting/rules.py` → `RULES` singleton. See `feedback_carrier_vetting_standards.md`.

**Hard reject thresholds:**

| Criterion | Threshold |
|---|---|
| Authority status | Must be ACTIVE |
| Fleet size | `RULES.fleet_min` (default: 3 trucks) |
| 0 drivers with >0 units | Auto-reject (shell/stale) |
| Safety rating | UNSATISFACTORY = reject |
| Vehicle OOS rate | > `RULES.vehicle_oos_max_pct` (30%) |
| Driver OOS rate | > `RULES.driver_oos_max_pct` (15%) |
| Crash rate | > `RULES.crash_rate_max_per_100` (30/100 units) |
| BIPD (liability) | < `RULES.liability_min` ($1M) |

**Reefer-specific (stricter):**

| Criterion | Threshold |
|---|---|
| Vehicle OOS rate | > `RULES.reefer_vehicle_oos_max_pct` (10%) |
| Vehicle inspection count | < `RULES.reefer_min_inspection_count` (10) → `needs_review` |

Note: cargo insurance (`RULES.cargo_min = 0`) is NOT enforced at prospect time. FMCSA does not publish cargo filings for general freight. Cargo verification happens at onboarding via COI.

---

## 7. Quarantine Rules

The `insert_carrier()` function in `app/sheets.py` routes rows based on vetting:

- **Passes all hard gates** → main `Carrier_Master` tab, `Onboarding_Status = PROSPECT`
- **Fails a hard gate** → `Carrier Quarantine` tab, `Onboarding_Status = QUARANTINE`, reason logged in `Internal_Notes`
- **Needs review** (reefer insufficient inspections, edge cases) → `Carrier Quarantine` tab with reason `needs_review:...`

**Reversibility:** Quarantine rows can be promoted to main tab manually after Derek reviews. The quarantine tab is a holding pen, not a delete.

Dedup logic: `seen_dots` is pre-seeded from both the main tab and `Carrier Quarantine` before each run. A carrier that was previously quarantined will not be re-inserted on the next run.

---

## 8. Secrets

All secrets flow through vault → `hydrate_from_vault()` → `.env`.

```bash
# Hydrate secrets from vault before running any script
python scripts/hydrate_vault.py
```

**Never:**
- Paste API keys directly into scripts
- Edit `.env` manually for anything in the vault
- Commit `.env` to git

Keys required for the carrier search flow:

| Key | Used by | Status |
|---|---|---|
| `CARRIER_MASTER_SHEET_ID` | `app/sheets.py` | Live |
| `APOLLO_API_KEY` | `app/email_enrichment.py` step 1 | Live |
| `BRAVE_SEARCH_API_KEY` | `app/email_enrichment.py` step 2 | Pending — Derek adding key; Rex stores in vault (tier: operations) |
| `GOOGLE_CSE_API_KEY` | dormant | Kept in vault; prune after Brave proven |
| `GOOGLE_CSE_CX` | dormant | Kept in vault; prune after Brave proven |
| `QCMOBILE_API_KEY` | `app/fmcsa.py` | Live |

---

## 9. What NOT To Do

- **Don't run `scripts/_deprecated/` scripts.** They bypass the denylist. See `scripts/_deprecated/README.md`.
- **Don't use `BrokerOps-AI-local/` (TypeScript repo).** It's archived as `BrokerOps-AI-local-DEPRECATED-20260415/`. It has no denylist, different scoring weights, and has never been tested against the current sheet schema.
- **Don't add Apollo-dependent required paths.** Apollo is parked at free tier — people-search is rate-limited. `BRAVE_SEARCH_API_KEY` is the bridge. Until Brave is configured, enrichment silently skips step 2.
- **Don't edit `.env` directly** for secrets that belong in the vault.
- **Don't add RFQ-triggered carrier search.** Manual-only is policy until we have a trained DB. See `feedback_carrier_search_manual.md`.
- **Don't write new per-state one-off scripts.** Use `--state XX` instead.

---

## 10. Known Open Items

| Item | Status | Impact |
|---|---|---|
| `BRAVE_SEARCH_API_KEY` in vault + `.env` | Pending — Derek adding key; Rex stores + hydrates | Estimated +30-40% email hit rate |
| Apollo plan upgrade | Parked | People-search returns thin results on free tier |
| State backfill on new runs | Manual — `enrich_carriers_playwright.py --backfill-states` | ~42 blank State rows as of 2026-04-15 |
| `vetting_pipeline.md` reefer rule docs | Stale — says "zero tolerance" but `rules.py` changed to 10% rate-based on 2026-04-15 | Misleading for any engineer reading the doc |
| `prospect-carriers-spec.md` | Stale spec from pre-build design — references `app/workflows/carrier_search.py` which doesn't exist | Misleading for onboarding |
| Google CSE (`_search_google_cse`) attempted | Dropped 2026-04-15 — Google deprecated "Search the entire web" toggle in CSE console; open-web search via CSE is no longer viable | N/A — replaced by Brave Search |
