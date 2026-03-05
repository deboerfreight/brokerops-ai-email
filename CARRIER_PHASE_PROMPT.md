# BrokerOps AI — Phase 2: Carrier Outreach & Database Building

## Context

You are continuing development on BrokerOps AI, a freight brokerage automation system built with FastAPI on Google Cloud Run. The system already handles inbound load emails — parsing them with Gemini AI, writing to a Google Sheets Load_Master, and auto-replying via a personality named Sasha Dorsey (Sales Associate @ deBoer Freight).

**Existing stack:**
- FastAPI on Google Cloud Run (project: `wide-decoder-489023-p1`)
- Google Sheets as system of record (Load_Master sheet already exists)
- Gmail API for inbound/outbound email
- Google Drive for document storage
- Gemini 2.5 Flash for AI parsing (API key in Secret Manager: `brokerops-gemini-api-key`)
- Equipment intelligence module (`app/equipment.py`) with trailer specs and recommendation engine
- Cloud Scheduler polls every 5 minutes via `/jobs/poll`

**Key files:**
- `app/ai_parser.py` — Gemini-based email classification, load parsing, and Sasha reply builders
- `app/workflows/load_ingestion.py` — Main load processing pipeline
- `app/workflows/inbox_scanner.py` — Auto-labels inbound emails
- `app/sheets.py` — Google Sheets CRUD (Load_Master columns and insert/read functions)
- `app/gmail.py` — Gmail helpers (read, send, reply_to_thread, labels)
- `app/equipment.py` — Equipment recommendation engine
- `app/config.py` — Settings via env vars
- `cloudbuild.yaml` — Deploy config (Artifact Registry + Cloud Run, uses `--update-env-vars`)

**Deploy flow:** `git push origin main` → `gcloud builds submit --config cloudbuild.yaml --substitutions=COMMIT_SHA=$(git rev-parse --short HEAD)`

---

## Phase 2 Objective

Build a carrier sourcing and database system that can:
1. Search for carriers by city, state, or lane within a defined radius
2. Score and rank carriers by authority, insurance, safety, and fleet size
3. Store carrier data in a Carrier_Master Google Sheet
4. Automate outreach emails to qualified carriers (Sasha Dorsey voice)
5. Process carrier responses and update the database
6. Eventually auto-match loads to carriers and send RFQs

---

## Part 1: FMCSA Data Integration

### Data Sources

**FMCSA Census API (live queries):**
- Register at https://mobile.fmcsa.dot.gov/QCDevsite/
- Base URL: `https://mobile.fmcsa.dot.gov/qc/services/carriers`
- Endpoints: search by name, state, MC/DOT number
- Returns: legal name, DBA, address, phone, authority status, fleet size, safety data
- Rate limited — use for targeted searches, not bulk pulls

**FMCSA SMS Monthly Snapshots (bulk data):**
- Download from https://ai.fmcsa.dot.gov/SMS/Tools/Downloads.aspx
- CSV files: Census, Safety, Insurance, Inspections, Crashes
- Update monthly — download and store in Google Drive
- Use for bulk scoring and filtering

### Implementation

Create `app/fmcsa.py`:
- `search_carriers(city, state, radius_miles, equipment_type=None)` — Query Census API for carriers in area
- `get_carrier_details(dot_number)` — Full carrier profile from Census API
- `score_carrier(carrier_data)` — Score based on criteria below
- `bulk_import_snapshot(csv_path)` — Parse monthly SMS snapshot data
- Cache API responses to avoid rate limits (simple dict cache with TTL, or Google Sheets as cache)

### Carrier Scoring Model

Score each carrier 0-100 based on weighted criteria:

| Criteria | Weight | Scoring Rules |
|---|---|---|
| Operating Authority | 25 | Must be ACTIVE and AUTHORIZED. Reject if revoked/suspended. Authority age: <18 months = 0 pts, 18-36 months = 15 pts, 36+ months = 25 pts |
| Insurance — Liability | 20 | Must have minimum $1M. $1M = 15 pts, $2M+ = 20 pts. BIPD required. |
| Insurance — Cargo | 10 | Must have minimum $100K. $100K = 7 pts, $250K+ = 10 pts |
| Safety Rating | 20 | Satisfactory = 20 pts, Conditional = 10 pts, Unsatisfactory = 0 (reject). No rating = 12 pts (neutral). Vehicle OOS rate >30% = subtract 10 pts. Driver OOS rate >20% = subtract 5 pts. |
| Fleet Size (Power Units) | 15 | 1-5 = 5 pts, 6-20 = 10 pts, 21-50 = 13 pts, 51+ = 15 pts |
| Complaint History | 10 | 0 complaints = 10 pts, 1-2 = 7 pts, 3-5 = 3 pts, 6+ = 0 pts |

**Hard disqualifiers (auto-reject):**
- Authority status not ACTIVE
- Insurance below minimums ($1M liability, $100K cargo)
- Safety rating = Unsatisfactory
- Out-of-service order active

### Equipment Specialty Detection

From FMCSA data, classify carriers by equipment capabilities:
- Check cargo carried codes and operation classification
- Map to our types: DRY_VAN, FLATBED, REEFER, CONESTOGA, BOX_TRUCK, SPRINTER, HOTSHOT
- Store as comma-separated in `Equipment_Types` field
- If carrier has reefer authority or refrigerated cargo codes → flag as reefer specialist
- If carrier has heavy haul or oversize indicators → flag as oversize specialist

---

## Part 2: Carrier_Master Google Sheet

### Sheet Structure

Create a new sheet tab called `Carrier_Master` in the existing spreadsheet. Columns:

```
MC_Number, DOT_Number, Legal_Name, DBA, Contact_Name, Contact_Email, Contact_Phone,
City, State, Zip, Authority_Status, Authority_Date, Authority_Age_Months,
Insurance_Liability, Insurance_Cargo, Insurance_Verified,
Safety_Rating, Vehicle_OOS_Rate, Driver_OOS_Rate,
Power_Units, Driver_Count, Equipment_Types, Specialties, Lanes_Served,
Broker_Friendly, COI_On_File, Carrier_Score,
Last_Contacted, Outreach_Status, Outreach_Sequence_Step,
Rate_Competitiveness, Notes, Created_Date, Last_Updated
```

**Outreach_Status values:** NEW, CONTACTED, FOLLOW_UP_1, FOLLOW_UP_2, RESPONDED, QUALIFIED, DECLINED, UNRESPONSIVE, BLACKLISTED

### Implementation

Update `app/sheets.py`:
- Add `CARRIER_MASTER_COLUMNS` list
- Add `insert_carrier(fields)` — append row to Carrier_Master
- Add `update_carrier(mc_number, updates)` — update existing carrier row
- Add `get_carrier(mc_number)` — fetch carrier by MC number
- Add `search_carriers_in_sheet(filters)` — query existing carriers by state, equipment, score, status
- Add `get_carriers_for_outreach()` — get carriers due for next outreach step
- Add env var `CARRIER_MASTER_RANGE` (default: `Carrier_Master!A:AH`)

---

## Part 3: Carrier Search Endpoint

Create `app/workflows/carrier_search.py`:
- `search_and_score(city, state, radius_miles=50, equipment_type=None, limit=10)`:
  1. Query FMCSA Census API for carriers in the area
  2. Filter by hard disqualifiers
  3. Score remaining carriers
  4. Check if carrier already exists in Carrier_Master (by MC or DOT number)
  5. Insert new carriers, update existing ones
  6. Return top N sorted by score
- `search_by_lane(origin_city, origin_state, dest_city, dest_state, equipment_type=None)`:
  1. Search carriers near origin
  2. Search carriers near destination
  3. Merge and deduplicate
  4. Prefer carriers appearing in both searches (they likely run that lane)

Add API endpoint in `app/main.py`:
- `POST /carriers/search` — body: `{city, state, radius, equipment_type, limit}`
- Returns: list of scored carriers with all details
- `GET /carriers/{mc_number}` — get single carrier profile
- `POST /carriers/search-lane` — body: `{origin_city, origin_state, dest_city, dest_state, equipment_type}`

---

## Part 4: Automated Carrier Outreach

### Outreach Email Sequence (Sasha Dorsey voice)

Sasha's outreach tone to carriers is **professional, direct, and value-forward**. Lead with what's in it for them. Include deBoer Freight's MC number so they can verify us.

**Email 1 — Initial Outreach (Day 0):**
```
Subject: Freight opportunities — deBoer Freight (MC#1712065)

Hi [Contact_Name or "there"],

Sasha Dorsey from deBoer Freight. We move [relevant equipment type] freight in the [region] area and I'm looking for reliable carriers to work with.

What we offer:
  • Consistent freight — not just one-off loads
  • Quick pay options available
  • Easy to work with — straightforward booking, no runaround

If you're interested, just reply with your preferred lanes and equipment, and I'll start matching you with loads.

Sasha Dorsey
Sales Associate
deBoer Freight
MC#1712065
```

**Email 2 — Follow-up #1 (Day 3):**
```
Subject: Re: Freight opportunities — deBoer Freight (MC#1712065)

Hey [Contact_Name or "there"], just following up. We've got freight moving through your area and want to make sure you're on our list. Reply with your lanes and I'll get loads in front of you.

Sasha Dorsey
Sales Associate
deBoer Freight
```

**Email 3 — Final Touch (Day 7):**
```
Subject: Re: Freight opportunities — deBoer Freight (MC#1712065)

Last note from me — if you're ever looking for freight in the [region] area, we're here. Just reply anytime and I'll get you set up.

Sasha Dorsey
Sales Associate
deBoer Freight
```

After Email 3 with no response → mark as UNRESPONSIVE.

### Implementation

Create `app/workflows/carrier_outreach.py`:
- `run_outreach_cycle()`:
  1. Get carriers with Outreach_Status = NEW → send Email 1, update to CONTACTED
  2. Get carriers with Outreach_Status = CONTACTED and Last_Contacted >= 3 days ago → send Email 2, update to FOLLOW_UP_1
  3. Get carriers with Outreach_Status = FOLLOW_UP_1 and Last_Contacted >= 4 days ago → send Email 3, update to FOLLOW_UP_2
  4. Get carriers with Outreach_Status = FOLLOW_UP_2 and Last_Contacted >= 7 days ago → update to UNRESPONSIVE
- `send_carrier_email(carrier, template_name)` — send via Gmail API (NOT reply_to_thread, this is a new thread)
- `process_carrier_response(msg_id, body, from_addr)` — parse carrier replies:
  - Extract lanes they serve
  - Extract equipment types
  - Extract rate preferences
  - Update Carrier_Master with response data
  - Mark as RESPONDED
  - Use Gemini to parse unstructured carrier replies

Add to poll job in `app/main.py`:
- After load ingestion, run `run_outreach_cycle()` on each poll
- Add carrier response processing to inbox scanner — classify carrier replies separately from load emails

### Carrier Response Classification

Update `app/workflows/inbox_scanner.py`:
- Detect replies to outreach emails (subject contains "Freight opportunities" or references our outreach)
- Label as `OPS/CARRIER_REPLY` instead of `OPS/NEW_LOAD`
- In `classify_email()`, the CARRIER_QUOTE category already exists — extend it to catch outreach responses too

---

## Part 5: Rate Quotes Sheet (for future load matching)

Create a `Rate_Quotes` sheet tab for storing carrier rate data. Columns:

```
Quote_ID, MC_Number, Carrier_Name, Lane_Origin_City, Lane_Origin_State,
Lane_Dest_City, Lane_Dest_State, Equipment_Type, Rate_Per_Mile, Flat_Rate,
Date_Quoted, Valid_Until, Source, Notes, Created_Date
```

**Source values:** OUTREACH_RESPONSE, RFQ_REPLY, HISTORICAL, MANUAL

Update `app/sheets.py`:
- Add `insert_rate_quote(fields)`
- Add `get_rates_for_lane(origin_state, dest_state, equipment_type)`

---

## Part 6: Load-to-Carrier Matching (Future — Build the Foundation)

Create `app/workflows/carrier_matching.py`:
- `find_carriers_for_load(load_id)`:
  1. Read load from Load_Master
  2. Search Carrier_Master for carriers matching:
     - Equipment type matches or carrier has that equipment
     - Carrier serves the origin or destination area (by state, or by Lanes_Served field)
     - Outreach_Status = QUALIFIED or RESPONDED
     - Carrier_Score >= 60
     - Broker_Friendly != "NO"
  3. Sort by: Carrier_Score descending, then Rate_Competitiveness
  4. Return top 5-10 matches
- Don't auto-send RFQs yet — just populate a `Matched_Carriers` field in Internal_Notes on the Load_Master
- This sets up for Phase 3 where Sasha auto-sends RFQs

---

## Environment Variables to Add

```
FMCSA_API_KEY=3ef8fb54340dab1a471a2936c7f2f894d84498cc
CARRIER_MASTER_RANGE=Carrier_Master!A:AH
RATE_QUOTES_RANGE=Rate_Quotes!A:O
DEBOER_MC_NUMBER=1712065
```

Add these to Cloud Run via `gcloud run services update brokerops-ai --update-env-vars=...`

---

## Part 7: Email Enrichment Pipeline

The FMCSA API does not provide carrier email addresses. To enable fully automated outreach, implement a waterfall enrichment pipeline that tries multiple sources in order.

### Pipeline Order (stop at first success)

**Step 1 — SAFER Website Scrape:**
- FMCSA SAFER Company Snapshot pages sometimes link to the carrier's website
- URL: `https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=USDOT&query_string={dot_number}`
- If a website URL is found, fetch the homepage and scrape for email addresses
- Look for `mailto:` links, contact page links, and email patterns in page text
- Regex for emails: `[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}`
- Filter out generic no-reply addresses, prioritize dispatch@, freight@, loads@, operations@, info@
- Also store the website URL in Carrier_Master

**Step 2 — Google Search:**
- Search: `"{Legal_Name}" "{City}" "{State}" email trucking contact`
- Parse top 3-5 results for email addresses
- Cross-reference domain with carrier name to avoid false positives
- Prioritize results from freight directories (DAT, Truckstop, 123Loadboard, CarrierLists)

**Step 3 — Apollo.io API (fallback):**
- Apollo.io free tier: 250 email credits/month
- API docs: https://apolloio.github.io/apollo-api-docs/
- Use Organization Search to find the company by name + location
- Then People Search to find contacts with dispatch/operations titles
- Prioritize titles: Dispatcher, Operations Manager, Fleet Manager, Owner/Operator, Logistics
- Store Apollo person ID for future lookups
- Track credits used to stay within free tier limits

**Step 4 — Flag for phone outreach (future AI calling):**
- If all email sources fail, set `Contact_Email = "PHONE_ONLY"`
- Set `Outreach_Status = "NEEDS_PHONE_CONTACT"`
- These carriers will be handled by AI calling in Phase 3 (Twilio/Bland.ai)
- For now, just flag them — don't skip them entirely

### Implementation

Create `app/email_enrichment.py`:
- `enrich_carrier_email(carrier)` — runs the waterfall pipeline
- `_scrape_safer_website(dot_number)` — get carrier website URL from SAFER
- `_scrape_website_for_email(url)` — fetch website, extract emails
- `_google_search_email(legal_name, city, state)` — search Google for carrier email
- `_apollo_lookup(legal_name, city, state)` — Apollo.io API lookup
- `_pick_best_email(emails)` — rank found emails by relevance (dispatch@ > freight@ > info@ > generic)
- Track enrichment source in a new field: `Email_Source` (SAFER_WEBSITE, GOOGLE, APOLLO, MANUAL, PHONE_ONLY)

Update `app/workflows/carrier_search.py`:
- After scoring and inserting a carrier, run `enrich_carrier_email()` if Contact_Email is empty
- Don't block on enrichment failure — insert the carrier first, enrich async if possible
- Log enrichment results: "Enriched MC#123456 email via SAFER_WEBSITE: dispatch@carrier.com"

### Environment Variables

```
APOLLO_API_KEY=<get from Apollo.io after signup>
```

Sign up at https://www.apollo.io/ — free tier gives 250 email credits/month. Get API key from Settings → API Keys.

### Carrier_Master Updates

Add these columns to Carrier_Master:
```
Contact_Email_Source, Website
```

Updated column list:
```
MC_Number, DOT_Number, Legal_Name, DBA, Contact_Name, Contact_Email, Contact_Email_Source,
Contact_Phone, Website, City, State, Zip, Authority_Status, Authority_Date, Authority_Age_Months,
Insurance_Liability, Insurance_Cargo, Insurance_Verified,
Safety_Rating, Vehicle_OOS_Rate, Driver_OOS_Rate,
Power_Units, Driver_Count, Equipment_Types, Specialties, Lanes_Served,
Broker_Friendly, COI_On_File, Carrier_Score,
Last_Contacted, Outreach_Status, Outreach_Sequence_Step,
Rate_Competitiveness, Notes, Created_Date, Last_Updated
```

Update `CARRIER_MASTER_RANGE` if column count changes.

---

## Part 8: AI Phone Outreach (Phase 3 — Foundation Only)

For carriers where no email can be found, the system will eventually use AI voice calling to make initial contact. **Do not build this yet** — just lay the groundwork.

### What to build now:
- Add `Outreach_Method` field to Carrier_Master: EMAIL, PHONE, BOTH
- When a carrier has `Contact_Email = "PHONE_ONLY"`, set `Outreach_Method = "PHONE"`
- Create placeholder file `app/workflows/carrier_calling.py` with:
  - Docstring explaining this will use Twilio or Bland.ai for AI voice calls
  - `run_phone_outreach_cycle()` — stub that logs "Phone outreach not yet implemented"
  - `process_call_result(mc_number, result)` — stub for handling call outcomes
- Add `PHONE_ONLY` carriers to a separate view/filter in the sheet so Derek can see which carriers need calls

### What will be built in Phase 3 (later):
- Bland.ai or Twilio integration for AI voice calls
- Sasha Dorsey voice clone for phone outreach
- Call script: introduce deBoer Freight, ask if they work with brokers, collect email + lanes + equipment
- Call outcome tracking: ANSWERED, VOICEMAIL, NO_ANSWER, DECLINED, INTERESTED
- Voicemail drop with callback number
- Auto-update Carrier_Master with call results

---

## Implementation Order

1. **FMCSA integration** (`app/fmcsa.py`) — get carrier search and scoring working ✅ DONE
2. **Carrier_Master sheet** — add columns, CRUD functions in `app/sheets.py` ✅ DONE
3. **Search endpoint** — `POST /carriers/search` and `/carriers/search-lane` ✅ DONE
4. **Email enrichment pipeline** (`app/email_enrichment.py`) — SAFER scrape → Google → Apollo.io waterfall
5. **Test enrichment** — search carriers near Miami, verify emails populate
6. **Outreach system** — email templates, 3-email sequence logic, add to poll job
7. **Carrier response processing** — inbox scanner updates, Gemini parsing of replies
8. **Rate_Quotes sheet** — structure and CRUD
9. **Load-to-carrier matching** — foundation for auto-RFQ
10. **AI calling foundation** — placeholder module, PHONE_ONLY flagging
11. **Deploy and test full cycle** — search → enrich → outreach → response → qualification

---

## Important Notes

- All replies use Sasha Dorsey's voice. Customer-facing = friendly and brief. Carrier-facing = professional and value-forward.
- The sign-off format is always:
  ```
  Sasha Dorsey
  Sales Associate
  deBoer Freight
  ```
- The existing reply functions in `ai_parser.py` have already been updated by Claude Code — `build_missing_fields_reply(missing_required, missing_preferred)`, `build_verification_reply(verification_reasons, equipment_rec)`, `build_confirmation_reply(fields)`, and `build_missing_attachment_reply()`. Note: `load_id` was removed from all signatures. Do NOT modify these functions.
- Use `--update-env-vars` (not `--set-env-vars`) when deploying to Cloud Run to avoid wiping existing env vars.
- Gemini API key is in Secret Manager, not env vars. Access via `_get_gemini_api_key()` in `ai_parser.py`.
- The `reply_to_thread()` function in `gmail.py` replies in an existing thread. For new outreach emails to carriers, you'll need a `send_new_email()` function — check if it exists, create if not.
- Test with real FMCSA data but be mindful of API rate limits. Cache aggressively.
- FMCSA SAFER scraping: be respectful — add 1-2 second delays between requests, don't hammer the server.
- Apollo.io: track credit usage. Log every API call. Stop enrichment if monthly limit is near.
- Google search: use a proper search API (Google Custom Search, free 100 queries/day) or SerpAPI — don't scrape Google directly, they'll block you.
