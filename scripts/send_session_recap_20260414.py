"""One-shot: send session recap to derekndeboer@gmail.com from sales@."""
import base64
from email.mime.text import MIMEText
from app.google_auth import get_gmail_service

BODY = """BrokerOps session recap - 2026-04-14

TL;DR: Built and shipped the full MDL Vendor Outreach demand-side pipeline - approval-gated sheet, locked Nina first-touch template, dispatcher + reply catcher + privacy-enforced col F, Windows Task Scheduler firing every 5 min. Separately: carrier DB cleanup (5 spot-patches + classification column), FMCSA skeleton backfill (45 to 123 geocoded for_hire carriers, nearly tripling matchable bench), full AI-tells audit and patch pass on Sofia + Nina templates, lane coverage analysis identifying 5 pitch-ready vendors. DMARC work was scoped, then deferred as non-essential. Everything except the MDL cron is reversible; the cron requires running schtasks /change /tn BrokerOps-MDL-Vendor-Dispatcher /disable to pause.

===== WHAT SHIPPED =====

1. MDL Vendor Outreach pipeline (LIVE)
   - New sheet: https://docs.google.com/spreadsheets/d/1zRh5bIjPK2R0CSG7kVich4tGYdEIC6FGIBgdI-voxLw/edit
   - Workflow: warm phone call, drop row (cols A-F), flip col K checkbox, Nina sends within 5 min, replies auto-classified and routed
   - Col F Derek's Notes (PRIVATE) is walled off from every agent with static AND runtime test assertions
   - Locked Nina first-touch template in app/templates/mdl_vendor_first_touch.txt. Cleaned of AI tells, rigid string formatter (no LLM), blank first-name falls back to Hello,  blank referring-contact drops the phrase entirely
   - Dispatcher + reply catcher in app/workflows/, CLI wrappers in scripts/, 12 tests green
   - Scheduled every 5 min via Windows Task Scheduler: BrokerOps-MDL-Vendor-Dispatcher
   - Runbook: C:\\Users\\Owner\\brokerops-ai\\docs\\mdl_vendor_cron_wiring.md

2. Carrier Database cleanup
   - 5 spot-check patches applied (Colonial Fuel, CIRCUIT, Tri State, Sunstate, Shelton)
   - New column AF Classification: 123 for_hire / 4 private_fleet_review / 1 chameleon_review (CIRCUIT has 192 trucks + 0 inspections/24mo, textbook chameleon signature)
   - Boot file "50 carriers" claim updated to actual 128

3. FMCSA skeleton backfill (BIG WIN)
   - 78 skeleton rows had phone but no HQ city/state, invisible to lane matching
   - Pulled HQ location via FMCSA /carriers/{dot}, single batched write, 0 overwrites
   - 45/123 to 123/123 for_hire carriers now geocoded (was 36 percent, now 100 percent)
   - Surfaced: 28 carriers flagged FMCSA-inactive (candidate for do_not_contact), 6-8 likely private-fleet candidates hiding under friendly sheet names (need reclassification), 3 non-US carriers (Mexico + Canada), all queued for review

4. AI-tells audit + patch pass
   - New principle saved: feedback_avoid_ai_tells.md. Agent-composed text must avoid phrasings that reveal AI authorship; default fallbacks to shortest human-natural phrasing
   - Sofia audit: 8 HIGH, 12 MEDIUM, 4 LOW across carrier_outreach.py, onboarding.py, outreach_reply.py
   - Sofia HIGH fixes landed: name fallback no longer shouts ALL CAPS, em-dashes removed from subject + body, My name is Sofia Reyes and I work with... became I'm Sofia at deBoer Freight..., equipment renders dry van not dry_van, fabricated South Florida region removed, please don't hesitate to reach out deleted
   - Nina audit: 4 HIGH, 1 MEDIUM across ai_parser.py and rate_confirmation.py
   - Nina HIGH fixes landed (14 edits): all customer-reply builders now sign as Nina Weston (fixed systemic persona-drift bug where they were signing as Sasha, Sales Associate), all en-dashes/arrows/bullet chars cleaned, raw enum rendering fixed, Please find attached became Rate con attached for Load X, at your earliest convenience became Sign and send it back when you can
   - Rate-confirmation goes to carriers, signed as Sofia (correct persona call)

5. Lane coverage analysis (output/mdl-vendor-lane-coverage-20260414.md)
   - Headline: 5 vendors pitchable TODAY with STRONG coverage: Beacon Roofing Supply (10/10), Oldcastle Architectural (11/12), SouthernCarlson (8/8), Eastern Metal Supply (7/10), Titan Florida CRH (6/7)
   - Top 5 sourcing gaps: Ohio flatbed, Maryland corridor dry van + flatbed, Missouri dry van, IL+IN, New England

6. New feedback memories saved
   - feedback_avoid_ai_tells.md (org-wide)
   - feedback_default_to_action.md (Sasha behavior fix after you flagged my fence-sitting mid-session)

===== STILL BLOCKED / WAITING =====

1. DMARC p=reject flip. DEFERRED (not urgent, self-imposed date)
   - Dev brief saved at output/deboerfreight-dns-dev-brief.md, forward when your developer is available
   - Investigation surfaced that rua reports have never actually arrived (pointed at personal gmail without proper cross-domain auth TXT)
   - Concluded: not necessary for BrokerOps to function today; current p=quarantine is doing its job
   - When the dev makes the edits, we get visibility for the first time and can safely tighten

2. Cloud Run migration for MDL dispatcher
   - Local Task Scheduler is the MVP; requires your machine on + user session active
   - Cloud Run path requires: gcloud auth login (you, in terminal), new HTTP endpoint in app/main.py, redeploy, Cloud Scheduler job creation
   - Runbook has full migration pointer; queue when traffic justifies

3. Sofia MEDIUM/LOW AI-tell findings
   - 12 MEDIUM + 4 LOW items flagged but not patched (judgment calls on voice choices you may want to review)

4. Sofia voice persona audit
   - Retell-hosted prompts cannot be audited from the repo
   - Would require pulling llm_e9ba056acc00d3547caacb2a51ad config via Retell API

===== YOUR ACTION QUEUE =====

1. MDL GM conversation: subdivide vendor list, decide 5-10 pilot vendors
2. Start pitching: drop rows A-F in the new sheet, flip col K, Nina takes over within 5 min
3. Review 5 flagged carriers in the carrier DB (rows 13, 14, 22, 37, 75)
4. Review 6-8 private-fleet candidates surfaced by FMCSA backfill (legal-name mismatches)
5. Decide on 28 FMCSA-inactive carriers: do_not_contact or remove?
6. Forward DNS dev brief to developer when available
7. Eventually: Cloud Run migration for MDL dispatcher (when traffic justifies)

===== TOMORROW'S PICKUP =====

Three tracks, priority order:

Track 1 (highest leverage): First real vendor pitch end-to-end
- Pick one vendor from the top 5 STRONG-coverage list
- Drop the row, flip the checkbox, watch Nina fire
- Verify the full loop: send, reply, RFQ extraction, matching, your approval in Slack DM
- Real validation of the whole stack in one go

Track 2: Classifier v2 using FMCSA legal name
- Current classifier uses sheet name (DBA/friendly) only
- FMCSA legal name exposes hidden private-fleet candidates (AIKEN CONCRETE hiding under AIKEN FLATBED SERVICE etc.)
- ~30 minute script to pull legal name for 123 carriers and re-flag

Track 3: Inactive/non-US cleanup
- Tag the 28 FMCSA-inactive as do_not_contact
- Decide policy on the 3 non-US carriers (exclude? use for cross-border lanes only?)

===== NOTES =====

- Session was long but productive. Dispatched 9 background agents, all returned clean
- Two major architectural calls you made that I saved as rules:
    - Default to action, not clarification. Stop fence-sitting, only pause on genuine ambiguity
    - Avoid AI tells. Template fallbacks default to shortest human-natural phrasing, not fuller template substitutes
- Memory file index is up to date; next session can load BrokerOps tier 2 boot and pick up from the Action Queue above

- Sasha
"""

msg = MIMEText(BODY)
msg["To"] = "derekndeboer@gmail.com"
msg["From"] = "sales@deboerfreight.com"
msg["Subject"] = "BrokerOps session recap - 2026-04-14 (MDL pipeline live, DB cleanup, AI-tells pass)"
raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

svc = get_gmail_service()
sent = svc.users().messages().send(userId="me", body={"raw": raw}).execute()
print(f"Session recap sent: id={sent['id']}")
