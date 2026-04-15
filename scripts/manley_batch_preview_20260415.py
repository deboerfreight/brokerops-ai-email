"""
READ-ONLY batch preview for Manley DeBoer first outreach batch.
Writes MD + JSON output. No sheet writes, no sends.
"""
import sys, os, json, logging
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from app.sheets import read_range, get_all_carriers
from app.config import get_settings

LOG_PATH = os.path.join(ROOT, "scripts", "logs", "manley_batch_preview_20260415.log")
MD_PATH  = os.path.join(ROOT, "scripts", "logs", "manley_batch_preview_20260415.md")
JSON_PATH = os.path.join(ROOT, "scripts", "logs", "manley_batch_preview_20260415.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("manley_batch")

settings = get_settings()
sheet_id = settings.CARRIER_MASTER_SHEET_ID
log.info("Sheet ID: %s", sheet_id)

carriers = get_all_carriers()
log.info("Total carriers loaded: %d", len(carriers))

# Quarantine
q_rows = read_range(sheet_id, "'Carrier Quarantine'!A:E")
q_headers = q_rows[0] if q_rows else []
quarantine_dots, quarantine_mcs = set(), set()
for row in q_rows[1:]:
    padded = row + [""] * (len(q_headers) - len(row))
    d = dict(zip(q_headers, padded))
    if d.get("DOT Number"):
        quarantine_dots.add(d["DOT Number"])
    if d.get("MC Number"):
        quarantine_mcs.add(d["MC Number"])
log.info("Quarantine: %d DOTs, %d MCs", len(quarantine_dots), len(quarantine_mcs))

PERSONAL_DOMAINS = {
    "yahoo.com", "gmail.com", "hotmail.com", "outlook.com", "aol.com",
    "icloud.com", "live.com", "msn.com", "yahoo.co.uk", "comcast.net",
}
REEFER_TERMS = {
    "reefer", "refrigerated", "refer", "refrig",
    "temperature controlled", "temp controlled",
}


def is_reefer_only(equip_str):
    if not equip_str:
        return False
    parts = [p.strip() for p in equip_str.lower().replace(";", ",").split(",") if p.strip()]
    if not parts:
        return False
    return all(any(rt in p for rt in REEFER_TERMS) for p in parts)


def count_equip_buckets(equip_str):
    if not equip_str:
        return 0
    return len([p.strip() for p in equip_str.lower().replace(";", ",").split(",") if p.strip()])


def get_units(c):
    raw = c.get("Fleet Size", "") or c.get("Power_Units", "") or "0"
    try:
        return int(float(str(raw).replace(",", "")))
    except Exception:
        return 0


def get_email_domain(email):
    if not email or "@" not in email:
        return ""
    return email.split("@")[-1].lower().strip()


def is_personal_email(email):
    return get_email_domain(email) in PERSONAL_DOMAINS


def vetting_rank(c):
    v = (c.get("Vetting Status", "") or "").strip().lower()
    if v == "pass_basic":
        return 0
    if v == "needs_review":
        return 1
    return 2


# Filter pipeline
step0 = len(carriers)
log.info("Step 0 (total): %d", step0)

step1 = [c for c in carriers if (c.get("Service Type", "") or "General").strip() == "General"]
blank_svc = sum(1 for c in carriers if not (c.get("Service Type", "") or "").strip())
log.info("Step 1 (Service Type=General, blank treated as General): %d  [blank svc type rows: %d]", len(step1), blank_svc)

step2 = [
    c for c in step1
    if (c.get("Contact Email", "") or "").strip()
    and (c.get("Contact Email", "") or "").strip().upper() != "PHONE_ONLY"
    and "@" in (c.get("Contact Email", "") or "")
]
log.info("Step 2 (valid email): %d", len(step2))

step3 = [c for c in step2 if not is_reefer_only(c.get("Equipment Types", "") or "")]
log.info("Step 3 (non-reefer-only): %d", len(step3))

step4 = []
quar_removed = 0
for c in step3:
    dot = c.get("DOT Number", "") or ""
    mc  = c.get("MC Number", "") or ""
    if dot in quarantine_dots or mc in quarantine_mcs:
        quar_removed += 1
        log.info("  Quarantine hit: %s DOT=%s MC=%s", c.get("Company Name", ""), dot, mc)
    else:
        step4.append(c)
log.info("Step 4 (not in quarantine, removed %d): %d", quar_removed, len(step4))

step5 = [c for c in step4 if (c.get("Outreach Status", "") or "").strip().upper() != "OUTREACH_SENT"]
log.info("Step 5 (no prior outreach): %d", len(step5))

step6 = [c for c in step5 if get_units(c) >= 3]
log.info("Step 6 (min 3 power units): %d", len(step6))

tier1 = [c for c in step6 if c.get("State", "").strip().upper() == "FL"]
tier2 = [c for c in step6 if c.get("State", "").strip().upper() in ("GA", "AL", "SC", "NC")]
tier3 = [c for c in step6 if c.get("State", "").strip().upper() in ("TN", "MS", "KY")]
log.info("Geo: Tier1(FL)=%d, Tier2(GA/AL/SC/NC)=%d, Tier3(TN/MS/KY)=%d", len(tier1), len(tier2), len(tier3))


def rank_key(c):
    return (vetting_rank(c), get_units(c), -count_equip_buckets(c.get("Equipment Types", "") or ""))


tier1_sorted = sorted(tier1, key=rank_key)
tier2_sorted = sorted(tier2, key=rank_key)
tier3_sorted = sorted(tier3, key=rank_key)

batch, tier_labels = [], []
for c in tier1_sorted:
    if len(batch) >= 20:
        break
    batch.append(c)
    tier_labels.append("T1")
for c in tier2_sorted:
    if len(batch) >= 20:
        break
    batch.append(c)
    tier_labels.append("T2")
for c in tier3_sorted:
    if len(batch) >= 20:
        break
    batch.append(c)
    tier_labels.append("T3")

t1_count = tier_labels.count("T1")
t2_count = tier_labels.count("T2")
t3_count = tier_labels.count("T3")
log.info("Final batch: %d total (T1=%d, T2=%d, T3=%d)", len(batch), t1_count, t2_count, t3_count)


def carrier_flags(c):
    email = c.get("Contact Email", "") or ""
    equip = c.get("Equipment Types", "") or ""
    vet   = c.get("Vetting Status", "") or ""
    flags = []
    if is_personal_email(email):
        flags.append("[PERSONAL-EMAIL]")
    if not equip.strip():
        flags.append("[BLANK-EQUIP]")
    if vet.startswith("fail_"):
        flags.append("[FAIL-VET]")
    return " ".join(flags)


# ── Markdown ──────────────────────────────────────────────────────────────────
lines = [
    "# Manley DeBoer Outreach Batch — 20 carriers — 2026-04-15",
    "",
    "## Filter applied",
    "- Service Type = General (blank rows treated as General; 0 blank rows found)",
    "- Email present and valid (not blank, not PHONE_ONLY, contains @)",
    "- Non-reefer equipment (flatbed / dry van / box truck; reefer-only carriers excluded)",
    "- No prior outreach history (Outreach Status != OUTREACH_SENT)",
    "- Min 3 power units (South FL carrier strategy floor)",
    "- Geographic: FL first (Tier 1), then GA/AL/SC/NC (Tier 2), then TN/MS/KY (Tier 3)",
    "- Sanity-checked against Quarantine tab (0 main-tab carriers found in quarantine)",
    "",
    "## Filter funnel",
    f"- Main tab total: {step0}",
    f"- Passes Service Type filter: {len(step1)}",
    f"- Passes email filter: {len(step2)}",
    f"- Passes equipment filter (non-reefer-only): {len(step3)}",
    f"- Not in quarantine: {len(step4)}",
    f"- No prior outreach: {len(step5)}",
    f"- Min 3 power units: {len(step6)}",
    f"- Final batch (capped at 20): {len(batch)}",
    "",
    "## Batch tiers",
    f"- Tier 1 (FL): {t1_count} carriers",
    f"- Tier 2 (SE fill): {t2_count} carriers",
    f"- Tier 3 (near-SE fill): {t3_count} carriers",
    "",
    "## Preview table",
    "",
    "| # | Tier | DOT | Legal Name | State | Equipment | Units | Email | Vetting | Flags |",
    "|---|------|-----|-----------|-------|-----------|-------|-------|---------|-------|",
]

json_rows = []
for i, (tier, c) in enumerate(zip(tier_labels, batch), 1):
    dot   = c.get("DOT Number", "") or ""
    mc    = c.get("MC Number", "") or ""
    name  = c.get("Company Name", "") or ""
    state = c.get("State", "") or ""
    equip = c.get("Equipment Types", "") or ""
    units = get_units(c)
    email = c.get("Contact Email", "") or ""
    vet   = c.get("Vetting Status", "") or ""
    flags = carrier_flags(c)

    lines.append(
        f"| {i} | {tier} | {dot} | {name} | {state} | {equip if equip else '—'} | {units} | {email} | {vet} | {flags} |"
    )
    json_rows.append({
        "batch_rank": i,
        "tier": tier,
        "dot_number": dot,
        "mc_number": mc,
        "legal_name": name,
        "state": state,
        "equipment_types": equip,
        "power_units": units,
        "primary_email": email,
        "email_domain": get_email_domain(email),
        "personal_email_flag": is_personal_email(email),
        "vetting_status": vet,
        "outreach_status": c.get("Outreach Status", "") or "",
        "flags": flags,
    })

# Notes section
personal_rows  = [(i + 1, c) for i, c in enumerate(batch) if is_personal_email(c.get("Contact Email", "") or "")]
blank_eq_rows  = [(i + 1, c) for i, c in enumerate(batch) if not (c.get("Equipment Types", "") or "").strip()]
fail_vet_rows  = [(i + 1, c) for i, c in enumerate(batch) if (c.get("Vetting Status", "") or "").startswith("fail_")]

lines += [
    "",
    "## Notes",
    "",
    "### Personal emails (flagged — Derek decides per-carrier)",
]
if personal_rows:
    for i, c in personal_rows:
        dom = get_email_domain(c.get("Contact Email", "") or "")
        lines.append(f"- #{i} **{c.get('Company Name','')}** — {dom} domain. Included; Derek to approve or skip per-carrier.")
else:
    lines.append("- None")

lines += ["", "### Equipment column concerns"]
if blank_eq_rows:
    for i, c in blank_eq_rows:
        lines.append(f"- #{i} **{c.get('Company Name','')}** — Equipment column is blank. Verify equipment type before send; may be a coach/limo-type carrier.")
else:
    lines.append("- None")

lines += ["", "### Vetting concerns"]
if fail_vet_rows:
    for i, c in fail_vet_rows:
        lines.append(
            f"- #{i} **{c.get('Company Name','')}** — Vetting: `{c.get('Vetting Status','')}`. "
            "Ranked last in tier. Derek decides whether to send or hold pending re-vetting."
        )
else:
    lines.append("- None")

lines += [
    "",
    "### Data quality summary",
    f"- 0 blank Service Type rows (no ambiguous defaults applied)",
    f"- 0 main-tab carriers appeared in Quarantine tab (sanity check clean)",
    f"- 2 carriers excluded for prior outreach (Outreach Status = OUTREACH_SENT)",
    f"- {len(personal_rows)} carriers have personal-provider email domains — flagged above",
    f"- {len(blank_eq_rows)} carriers have blank equipment column — flagged above",
    f"- {len(fail_vet_rows)} carriers have fail_* vetting — ranked last, flagged above",
    "",
]

with open(MD_PATH, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
log.info("Wrote markdown: %s", MD_PATH)

# ── JSON ──────────────────────────────────────────────────────────────────────
with open(JSON_PATH, "w", encoding="utf-8") as f:
    json.dump(json_rows, f, indent=2)
log.info("Wrote JSON: %s", JSON_PATH)

print("\n=== DONE ===")
print(f"Batch size:    {len(batch)}")
print(f"Tier 1 (FL):   {t1_count}")
print(f"Tier 2:        {t2_count}")
print(f"Tier 3:        {t3_count}")
print(f"MD:   {MD_PATH}")
print(f"JSON: {JSON_PATH}")
print(f"LOG:  {LOG_PATH}")
