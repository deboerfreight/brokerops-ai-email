# prospect-carriers Script Spec
## Authored by Rex — 2026-04-07

### Overview
New `scripts/prospect_carriers.py` that reads the enriched vendor DC list (394 facilities, 76 vendors), clusters them into geographic lanes destined for South FL / Key West, searches FMCSA for carriers servicing those lanes, vets them against strict thresholds, enriches with contact info, and stages them for Sofia outreach.

---

### Key Finding: Codebase is Python/FastAPI (not TypeScript)
Project location: `C:\Users\Owner\brokerops-ai\`
Runs on Google Cloud Run. Google Sheets as DB.

### Existing Pipeline (as found)

| File | Purpose |
|---|---|
| `app/fmcsa.py` | FMCSA Census API search, detail fetch, scoring, equipment detection |
| `app/carrierok.py` | CarrierOK API for authority + insurance verification |
| `app/sheets.py` | Google Sheets CRUD for Carrier_Master and Load_Master |
| `app/email_enrichment.py` | 4-step waterfall email finder: SAFER > Google CSE > Apollo.io > PHONE_ONLY |
| `app/equipment.py` | Trailer specs, commodity-to-equipment mapping |
| `app/config.py` | Pydantic Settings with all API keys |
| `app/workflows/carrier_search.py` | Orchestrates FMCSA search > detail > score > upsert > email enrichment |
| `app/workflows/outreach_reply.py` | Processes carrier replies to Sofia outreach |

### Lane Clusters

| Cluster | States | Priority | Search Strategy |
|---|---|---|---|
| SOUTH_FL | FL (south ZIP prefixes) | 1 | City-level |
| CENTRAL_FL | FL (central/north) | 2 | City-level |
| SOUTHEAST_US | GA, AL, SC, NC, TN, MS, VA | 3 | State-level |
| MID_ATLANTIC | MD, PA, NY, NJ, CT, MA, OH, WV | 4 | State-level |
| NATIONAL | All others | 5 | State-level |

### Vetting Thresholds (Strict)

| Check | Current Code | Required |
|---|---|---|
| Vehicle OOS rate | >30% subtracts 10pts (soft) | >30% = REJECT (hard) |
| Driver OOS rate | >20% subtracts 5pts (soft) | >15% = REJECT (hard) |
| Crash rate | Not checked | >30/100 power units = REJECT |
| Reefer maintenance | Not checked | Zero tolerance |

### Execution Flow

```
1. Load vendor DCs (CSV or Sheets) -> ~350 facilities after filtering
2. Cluster into 5 lane groups -> ~135 unique search targets
3. For each cluster (priority order):
   a. FMCSA search (2 equipment types per target: FLATBED, DRY_VAN)
   b. Score via fmcsa.score_carrier()
   c. Apply strict vetting
   d. Deduplicate across targets
   e. Store via carrier_search._upsert_carrier (insert + enrichment)
   f. Tag with lane cluster in Preferred_Lanes
   g. Set Onboarding_Status = PROSPECT -> NEW (for outreach)
4. Output summary
```

### CLI Interface

```
python -m scripts.prospect_carriers [OPTIONS]
  --dry-run          Log without writing
  --cluster NAME     Run one cluster only
  --limit N          Max carriers per target (default: 30)
  --min-score N      Minimum score (default: 40)
  --min-fleet N      Minimum power units (default: 5)
  --source TYPE      csv or sheets (default: csv)
  --resume FILE      Resume from checkpoint
  --verbose          Debug logging
```

### Gaps to Fill

1. **Crash rate data** — FMCSA Census API doesn't return crash rates. Need inspection endpoint or SMS snapshot bulk import.
2. **Reefer zero-tolerance check** — Requires inspection-level data not currently fetched.
3. **No initial outreach workflow** — `outreach_reply.py` handles replies, but Sofia's proactive 3-email sequence was never built. Need `carrier_outreach.py`.
4. **No `openpyxl` dependency** — Add to requirements.txt for xlsx reading.
5. **PROSPECT status** — New `Onboarding_Status` value, no code changes needed (free string in Sheets).
6. **Vendor DC data in Sheets** — Currently local CSV only. Should upload for production use.
7. **Rate limiting / resume** — ~270 FMCSA searches + ~500-1500 detail calls. ~20-40 min runtime. Needs checkpoint/resume.

### Integration Points

| New Function | Calls Existing | File |
|---|---|---|
| search_cluster_carriers() | fmcsa.search_carriers() | app/fmcsa.py:56 |
| search_cluster_carriers() | fmcsa.get_carrier_details() | app/fmcsa.py:126 |
| search_cluster_carriers() | fmcsa.score_carrier() | app/fmcsa.py:328 |
| enrich_and_store() | carrier_search._upsert_carrier() | app/workflows/carrier_search.py:134 |
| enrich_and_store() | sheets.find_carrier() | app/sheets.py:223 |
| enrich_and_store() | email_enrichment.enrich_carrier_email() | app/email_enrichment.py:345 |
| queue_for_outreach() | sheets.update_carrier_fields_by_key() | app/sheets.py:260 |

### API Budget

| API | Est. Calls | Cost |
|---|---|---|
| FMCSA Census | ~1,500-1,800 | Free |
| SAFER scrape | ~200-500 | Free |
| Google CSE | ~100-200 | Free tier |
| Apollo.io | ~50-100 | Free tier |
| Google Sheets | ~500-1,500 | Free |

Recommend running one cluster at a time with `--cluster` flag. Total runtime: ~20-40 min per full run.
