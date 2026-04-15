"""
Address Cross-Reference Script — Pass 1 (flatbed + dry van)
Reads destination sheet gid=1779615674 and cross-references against Carrier DB.
Outputs .md and .json reports.

READ-ONLY on both sheets. No writes.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

# ── Setup paths ──────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv
load_dotenv(REPO_ROOT / ".env")

LOG_PATH = REPO_ROOT / "scripts/logs/address_crossref_20260415.log"
MD_PATH  = REPO_ROOT / "scripts/logs/address_crossref_20260415.md"
JSON_PATH= REPO_ROOT / "scripts/logs/address_crossref_20260415.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("crossref")

# ── Source / Carrier sheet IDs ───────────────────────────────────────────────
SOURCE_SHEET_ID   = "1hj77abQJxrSlZb5eKgQ-iU9LiiijXgYrCSYcRqPgZeA"
SOURCE_GID        = "1779615674"
CARRIER_SHEET_ID  = "1B5nzDCisdpG29bI0MjLtD8dIE2rsjdXg3Qncj-E58WE"
QUARANTINE_TAB    = "Carrier Quarantine"

# ── State adjacency map (contiguous US, simplified) ─────────────────────────
ADJACENCY: dict[str, list[str]] = {
    "AL": ["FL","GA","MS","TN"],
    "AK": [],
    "AZ": ["CA","CO","NM","NV","UT"],
    "AR": ["LA","MO","MS","OK","TN","TX"],
    "CA": ["AZ","NV","OR"],
    "CO": ["AZ","KS","NE","NM","OK","UT","WY"],
    "CT": ["MA","NY","RI"],
    "DE": ["MD","NJ","PA"],
    "FL": ["AL","GA"],
    "GA": ["AL","FL","NC","SC","TN"],
    "HI": [],
    "ID": ["MT","NV","OR","UT","WA","WY"],
    "IL": ["IN","IA","KY","MI","MO","WI"],
    "IN": ["IL","KY","MI","OH"],
    "IA": ["IL","MN","MO","NE","SD","WI"],
    "KS": ["CO","MO","NE","OK"],
    "KY": ["IL","IN","MO","OH","TN","VA","WV"],
    "LA": ["AR","MS","TX"],
    "ME": ["NH"],
    "MD": ["DE","PA","VA","WV"],
    "MA": ["CT","NH","NY","RI","VT"],
    "MI": ["IN","OH","WI"],
    "MN": ["IA","ND","SD","WI"],
    "MS": ["AL","AR","LA","TN"],
    "MO": ["AR","IL","IA","KS","KY","NE","OK","TN"],
    "MT": ["ID","ND","SD","WY"],
    "NE": ["CO","IA","KS","MO","SD","WY"],
    "NV": ["AZ","CA","ID","OR","UT"],
    "NH": ["MA","ME","VT"],
    "NJ": ["DE","NY","PA"],
    "NM": ["AZ","CO","OK","TX"],
    "NY": ["CT","MA","NJ","PA","VT"],
    "NC": ["GA","SC","TN","VA"],
    "ND": ["MN","MT","SD"],
    "OH": ["IN","KY","MI","PA","WV"],
    "OK": ["AR","CO","KS","MO","NM","TX"],
    "OR": ["CA","ID","NV","WA"],
    "PA": ["DE","MD","NJ","NY","OH","WV"],
    "RI": ["CT","MA"],
    "SC": ["GA","NC"],
    "SD": ["IA","MN","MT","ND","NE","WY"],
    "TN": ["AL","AR","GA","KY","MO","MS","NC","VA"],
    "TX": ["AR","LA","NM","OK"],
    "UT": ["AZ","CO","ID","NV","NM","WY"],
    "VT": ["MA","NH","NY"],
    "VA": ["KY","MD","NC","TN","WV"],
    "WA": ["ID","OR"],
    "WV": ["KY","MD","OH","PA","VA"],
    "WI": ["IL","IA","MI","MN"],
    "WY": ["CO","ID","MT","NE","SD","UT"],
    "DC": ["MD","VA"],
}


def get_adjacent(state: str) -> list[str]:
    return ADJACENCY.get(state.upper(), [])


# ── Sheets auth ──────────────────────────────────────────────────────────────
def get_svc():
    from app.google_auth import get_sheets_service
    return get_sheets_service().spreadsheets()


def read_range(sheet_id: str, range_: str) -> list[list[str]]:
    resp = get_svc().values().get(spreadsheetId=sheet_id, range=range_).execute()
    return resp.get("values", [])


# ── Fetch source sheet tab by gid ────────────────────────────────────────────
def get_tab_name_by_gid(sheet_id: str, gid: str) -> str | None:
    meta = get_svc().get(spreadsheetId=sheet_id).execute()
    for s in meta.get("sheets", []):
        if str(s["properties"]["sheetId"]) == str(gid):
            return s["properties"]["title"]
    return None


# ── Equipment filter ─────────────────────────────────────────────────────────
PASS_EQUIP = {"flatbed", "dry_van", "dry van", "flat bed", "flatbed/dry van", "dry van/flatbed"}
DENY_EQUIP = {"reefer", "refrigerated", "tanker", "tank", "liquid", "auto", "car hauler",
              "auto transport", "fuel", "box truck", "box_truck", "hotshot", "hot shot", "cargo van"}

def equip_passes(raw: str) -> tuple[bool, list[str]]:
    """Returns (passes, matched_types). passes=True if flatbed OR dry_van found, no reefer/tanker-only."""
    if not raw:
        return False, []
    lower = raw.lower()
    matched = []
    # Check positive
    has_flatbed = any(x in lower for x in ["flatbed", "flat bed", "flat-bed"])
    has_dry_van = any(x in lower for x in ["dry van", "dry_van", "dryvan"])
    # Check hard deny (reefer-ONLY or tanker-anything)
    has_reefer = any(x in lower for x in ["reefer", "refrigerated"])
    has_tanker = any(x in lower for x in ["tanker", "tank truck"])
    has_box    = any(x in lower for x in ["box truck", "box_truck"])
    has_hotshot= any(x in lower for x in ["hotshot", "hot shot", "hot-shot", "power only"])
    if has_flatbed:
        matched.append("flatbed")
    if has_dry_van:
        matched.append("dry_van")
    if not matched:
        return False, []
    # If reefer or tanker is ALSO there but it's not the ONLY equipment, still allow
    # unless tanker (zero tolerance per memory)
    if has_tanker:
        return False, []
    # reefer-ONLY = deny; mixed flatbed+reefer = allow
    if has_reefer and not has_flatbed and not has_dry_van:
        return False, []
    return True, matched


# ── Vetting filter ────────────────────────────────────────────────────────────
FAIL_PREFIXES = ("fail_",)

def vetting_passes(status: str) -> bool:
    if not status:
        return True  # blank = no vetting done = needs_review, include
    s = status.lower().strip()
    return not any(s.startswith(p) for p in FAIL_PREFIXES)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=== Address Cross-Reference Pass 1 started ===")

    # 1. Fetch source sheet tab name
    log.info("Resolving source sheet tab for gid=%s ...", SOURCE_GID)
    try:
        tab_name = get_tab_name_by_gid(SOURCE_SHEET_ID, SOURCE_GID)
    except Exception as e:
        log.error("AUTH FAILURE reading source sheet: %s", e)
        print(f"\n\nAUTH FAILURE: {e}\nSTOPPING.\n")
        sys.exit(1)

    if not tab_name:
        log.error("Could not find tab with gid=%s in source sheet", SOURCE_GID)
        sys.exit(1)

    log.info("Source tab name: '%s'", tab_name)

    # 2. Read all rows from source tab
    try:
        source_rows = read_range(SOURCE_SHEET_ID, f"'{tab_name}'!A:Z")
    except Exception as e:
        log.error("AUTH FAILURE reading source tab rows: %s", e)
        print(f"\n\nAUTH FAILURE: {e}\nSTOPPING.\n")
        sys.exit(1)

    if not source_rows:
        log.error("Source sheet returned 0 rows.")
        sys.exit(1)

    source_headers = [h.strip() for h in source_rows[0]]
    source_data_rows = source_rows[1:]
    log.info("Source schema: %s", source_headers)
    log.info("Source data rows (excl header): %d", len(source_data_rows))

    # 3. Parse source rows — detect column names flexibly
    def find_col(candidates: list[str], headers: list[str]) -> int | None:
        for c in candidates:
            for i, h in enumerate(headers):
                if c.lower() in h.lower():
                    return i
        return None

    col_location = find_col(["location", "name", "company", "destination", "business"], source_headers)
    col_address  = find_col(["address", "addr", "street"], source_headers)
    col_city     = find_col(["city"], source_headers)
    col_state    = find_col(["state", "st"], source_headers)
    col_zip      = find_col(["zip", "postal"], source_headers)

    schema_desc = (
        f"col[{col_location}]={source_headers[col_location] if col_location is not None else 'NOT FOUND'}(Location), "
        f"col[{col_address}]={source_headers[col_address] if col_address is not None else 'NOT FOUND'}(Address), "
        f"col[{col_city}]={source_headers[col_city] if col_city is not None else 'NOT FOUND'}(City), "
        f"col[{col_state}]={source_headers[col_state] if col_state is not None else 'NOT FOUND'}(State), "
        f"col[{col_zip}]={source_headers[col_zip] if col_zip is not None else 'NOT FOUND'}(Zip)"
    )
    log.info("Schema detected: %s", schema_desc)

    def get_cell(row: list[str], idx: int | None) -> str:
        if idx is None or idx >= len(row):
            return ""
        return row[idx].strip()

    # Parse destinations
    destinations = []
    dq_issues = []
    for i, row in enumerate(source_data_rows, start=2):
        loc   = get_cell(row, col_location)
        addr  = get_cell(row, col_address)
        city  = get_cell(row, col_city)
        state = get_cell(row, col_state).upper()
        zip_  = get_cell(row, col_zip)

        # Skip blank rows
        if not any([loc, addr, city, state, zip_]):
            log.info("Row %d: blank, skipping", i)
            continue

        # Data quality checks
        issues = []
        if not state:
            issues.append("missing state")
        elif len(state) != 2:
            issues.append(f"malformed state '{state}'")
        if not city and not addr:
            issues.append("missing city and address")

        label = loc or city or addr or f"Row {i}"
        if issues:
            dq_issues.append({"row": i, "label": label, "issues": issues})
            log.warning("Row %d DQ: %s — %s", i, label, "; ".join(issues))

        destinations.append({
            "row": i,
            "label": label,
            "address": addr,
            "city": city,
            "state": state,
            "zip": zip_,
            "dq_issues": issues,
        })

    log.info("Parsed %d destinations (%d with DQ issues)", len(destinations), len(dq_issues))

    # 4. Read carrier DB
    log.info("Reading carrier DB ...")
    try:
        carrier_rows = read_range(CARRIER_SHEET_ID, "'Carrier Database'!A:AR")
    except Exception as e:
        log.error("AUTH FAILURE reading carrier DB: %s", e)
        sys.exit(1)

    if not carrier_rows:
        log.error("Carrier DB returned 0 rows")
        sys.exit(1)

    c_headers = [h.strip() for h in carrier_rows[0]]
    log.info("Carrier DB headers: %s", c_headers)

    def find_c_col(candidates: list[str]) -> int | None:
        for c in candidates:
            for i, h in enumerate(c_headers):
                if h.lower() == c.lower():
                    return i
            # partial match fallback
            for i, h in enumerate(c_headers):
                if c.lower() in h.lower():
                    return i
        return None

    ci_dot      = find_c_col(["DOT Number", "DOT"])
    ci_name     = find_c_col(["Company Name", "Legal_Name"])
    ci_city     = find_c_col(["City"])
    ci_state    = find_c_col(["State"])
    ci_equip    = find_c_col(["Equipment Types", "Equipment_Type", "Equipment"])
    ci_fleet    = find_c_col(["Fleet Size", "Power_Units"])
    ci_email    = find_c_col(["Contact Email", "Primary_Email"])
    ci_phone    = find_c_col(["Contact Phone", "Primary_Phone"])
    ci_svc_type = find_c_col(["Service Type", "Service_Type"])
    ci_vetting  = find_c_col(["Vetting Status", "Vetting_Status"])

    log.info("Carrier col indices: DOT=%s Name=%s City=%s State=%s Equip=%s Fleet=%s Email=%s Phone=%s SvcType=%s Vetting=%s",
             ci_dot, ci_name, ci_city, ci_state, ci_equip, ci_fleet, ci_email, ci_phone, ci_svc_type, ci_vetting)

    def gc(row: list, idx) -> str:
        if idx is None or idx >= len(row):
            return ""
        return str(row[idx]).strip()

    # 5. Read quarantine tab to build exclusion set
    try:
        q_rows = read_range(CARRIER_SHEET_ID, f"'{QUARANTINE_TAB}'!A:Z")
        q_headers = [h.strip() for h in q_rows[0]] if q_rows else []
        q_dot_idx = next((i for i, h in enumerate(q_headers) if "dot" in h.lower()), None)
        quarantine_dots = set()
        for r in q_rows[1:]:
            if q_dot_idx is not None and q_dot_idx < len(r) and r[q_dot_idx].strip():
                quarantine_dots.add(r[q_dot_idx].strip())
        log.info("Quarantine tab: %d carriers excluded", len(quarantine_dots))
    except Exception as e:
        log.warning("Could not read quarantine tab: %s — proceeding without quarantine check", e)
        quarantine_dots = set()

    # 6. Filter carriers
    total_carriers = len(carrier_rows) - 1
    log.info("Total carriers in DB: %d", total_carriers)

    general_pool = []
    equip_pool   = []
    final_pool   = []

    # Filter Step 1: Service_Type == General
    for row in carrier_rows[1:]:
        svc = gc(row, ci_svc_type).lower().strip()
        if not svc or svc == "general":
            general_pool.append(row)

    log.info("After Service_Type=General filter: %d", len(general_pool))

    # Filter Step 2: equipment includes flatbed OR dry_van
    for row in general_pool:
        equip_raw = gc(row, ci_equip)
        passes, types = equip_passes(equip_raw)
        if passes:
            equip_pool.append((row, types))

    log.info("After equipment (flatbed OR dry_van) filter: %d", len(equip_pool))

    # Filter Step 3: vetting not fail_*
    vet_pool = []
    for row, types in equip_pool:
        vs = gc(row, ci_vetting)
        if vetting_passes(vs):
            vet_pool.append((row, types))

    log.info("After vetting filter: %d", len(vet_pool))

    # Filter Step 4: not in quarantine
    for row, types in vet_pool:
        dot = gc(row, ci_dot)
        if dot not in quarantine_dots:
            final_pool.append((row, types))

    log.info("Final pool after quarantine exclusion: %d", len(final_pool))

    # Build carrier dicts
    def carrier_dict(row, types) -> dict:
        return {
            "dot":      gc(row, ci_dot),
            "name":     gc(row, ci_name),
            "city":     gc(row, ci_city),
            "state":    gc(row, ci_state).upper(),
            "equipment": gc(row, ci_equip),
            "matched_types": types,
            "fleet":    gc(row, ci_fleet),
            "email":    gc(row, ci_email),
            "phone":    gc(row, ci_phone),
            "vetting":  gc(row, ci_vetting),
        }

    carriers = [carrier_dict(r, t) for r, t in final_pool]

    # Check pass 2 candidates (box truck + hot shot) — count only, don't include
    pass2_count = 0
    for row in carrier_rows[1:]:
        equip_raw = gc(row, ci_equip).lower()
        if any(x in equip_raw for x in ["box truck", "box_truck", "hotshot", "hot shot"]):
            svc = gc(row, ci_svc_type).lower().strip()
            if not svc or svc == "general":
                vs = gc(row, ci_vetting)
                if vetting_passes(vs):
                    dot = gc(row, ci_dot)
                    if dot not in quarantine_dots:
                        pass2_count += 1
    log.info("Pass 2 additional candidates (box truck / hot shot): ~%d", pass2_count)

    # 7. Cross-reference destinations
    results = []
    stats = {"in_state_covered": 0, "nearby_only": 0, "zero_match": 0}

    for dest in destinations:
        dstate = dest["state"]
        in_state = [c for c in carriers if c["state"] == dstate]
        adjacent = get_adjacent(dstate)
        nearby   = [c for c in carriers if c["state"] in adjacent and c["state"] != dstate]

        if in_state:
            stats["in_state_covered"] += 1
        elif nearby:
            stats["nearby_only"] += 1
        else:
            stats["zero_match"] += 1

        results.append({
            "destination": dest,
            "in_state_count": len(in_state),
            "nearby_count":   len(nearby),
            "in_state":  in_state,
            "nearby":    nearby,
            "adjacent_states": adjacent,
        })

        log.info("Dest '%s' (%s): in-state=%d, nearby=%d",
                 dest["label"], dstate, len(in_state), len(nearby))

    # Sort for top/bottom stats
    results_sorted_by_in_state = sorted(results, key=lambda x: x["in_state_count"], reverse=True)
    top3 = results_sorted_by_in_state[:3]
    bottom3 = sorted(results, key=lambda x: x["in_state_count"] + x["nearby_count"])[:3]

    # 8. Write JSON output
    json_out = {
        "meta": {
            "date": "2026-04-15",
            "pass": 1,
            "source_sheet_id": SOURCE_SHEET_ID,
            "source_tab": tab_name,
            "source_gid": SOURCE_GID,
            "locations_count": len(destinations),
            "schema": schema_desc,
            "filter_funnel": {
                "total_in_db": total_carriers,
                "after_service_type_general": len(general_pool),
                "after_equipment_filter": len(equip_pool),
                "after_vetting_filter": len(vet_pool),
                "final_pool": len(final_pool),
            },
            "coverage": {
                "in_state_covered": stats["in_state_covered"],
                "nearby_only": stats["nearby_only"],
                "zero_match": stats["zero_match"],
            },
            "pass2_additional_candidates": pass2_count,
            "dq_issues": dq_issues,
        },
        "destinations": [
            {
                "label": r["destination"]["label"],
                "state": r["destination"]["state"],
                "city": r["destination"]["city"],
                "address": r["destination"]["address"],
                "zip": r["destination"]["zip"],
                "dq_issues": r["destination"]["dq_issues"],
                "in_state_count": r["in_state_count"],
                "nearby_count": r["nearby_count"],
                "adjacent_states": r["adjacent_states"],
                "in_state_carriers": r["in_state"],
                "nearby_carriers": r["nearby"],
            }
            for r in results
        ],
    }

    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(json_out, f, indent=2)
    log.info("JSON written to %s", JSON_PATH)

    # 9. Write Markdown output
    def carrier_table_rows(cs: list[dict]) -> str:
        if not cs:
            return "_None_\n"
        lines = ["| DOT | Name | City | Equipment | Units | Email | Vetting |",
                 "|---|---|---|---|---|---|---|"]
        for c in cs:
            lines.append(f"| {c['dot']} | {c['name']} | {c['city']} | {c['equipment'][:40]} | {c['fleet']} | {c['email']} | {c['vetting']} |")
        return "\n".join(lines) + "\n"

    md_lines = [
        "# Destination → Carrier Cross-Reference (Pass 1: flatbed + dry van)",
        "",
        "## Source",
        f"- Sheet ID: `{SOURCE_SHEET_ID}`",
        f"- Tab: **{tab_name}** (gid={SOURCE_GID})",
        f"- Locations pulled: **{len(destinations)}**",
        f"- Schema detected: `{schema_desc}`",
        "",
        "## Filter Funnel",
        f"- Total carriers in DB: **{total_carriers}**",
        f"- Passes Service_Type = General (blank or 'General'): **{len(general_pool)}**",
        f"- Passes equipment (flatbed OR dry_van): **{len(equip_pool)}**",
        f"- Passes vetting (not fail_*): **{len(vet_pool)}**",
        f"- Not in quarantine: **{len(final_pool)}** ← Final pool",
        "",
        "## Coverage Summary",
        f"- Destinations with AT LEAST ONE in-state match: **{stats['in_state_covered']}**",
        f"- Destinations with ONLY nearby-state matches: **{stats['nearby_only']}**",
        f"- Destinations with ZERO matches: **{stats['zero_match']}**",
        "",
        "## Data Quality Issues",
    ]

    if dq_issues:
        for dq in dq_issues:
            md_lines.append(f"- Row {dq['row']} ({dq['label']}): {', '.join(dq['issues'])}")
    else:
        md_lines.append("_None detected._")

    md_lines += ["", "---", "", "## Per-Location Matches", ""]

    for r in results:
        dest = r["destination"]
        md_lines.append(f"### {dest['label']}")
        if dest["city"] or dest["state"]:
            md_lines.append(f"**Location:** {dest['city']}, {dest['state']} {dest['zip']}".strip())
        if dest["address"]:
            md_lines.append(f"**Address:** {dest['address']}")
        md_lines.append("")

        md_lines.append(f"**In-state ({dest['state']}):** {r['in_state_count']} carrier(s)")
        md_lines.append(carrier_table_rows(r["in_state"]))

        nearby_label = ", ".join(r["adjacent_states"]) if r["adjacent_states"] else "none"
        md_lines.append(f"**Nearby-state ({nearby_label}):** {r['nearby_count']} carrier(s)")
        md_lines.append(carrier_table_rows(r["nearby"]))
        md_lines.append("---")
        md_lines.append("")

    with open(MD_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))
    log.info("Markdown written to %s", MD_PATH)

    # ── Print summary for Sasha ───────────────────────────────────────────────
    print("\n" + "="*60)
    print("CROSS-REFERENCE SUMMARY")
    print("="*60)
    print(f"Source tab: '{tab_name}' | Locations: {len(destinations)}")
    print(f"Schema: {schema_desc}")
    print(f"\nFilter funnel:")
    print(f"  Total in DB:        {total_carriers}")
    print(f"  Service_Type=Gen:   {len(general_pool)}")
    print(f"  Equipment pass:     {len(equip_pool)}")
    print(f"  Vetting pass:       {len(vet_pool)}")
    print(f"  Final pool:         {len(final_pool)}")
    print(f"\nCoverage:")
    print(f"  In-state covered:   {stats['in_state_covered']}")
    print(f"  Nearby-state only:  {stats['nearby_only']}")
    print(f"  Zero match:         {stats['zero_match']}")
    print(f"\nTop 3 best-covered:")
    for r in top3:
        print(f"  {r['destination']['label']} ({r['destination']['state']}): {r['in_state_count']} in-state")
    print(f"\nTop 3 worst-covered:")
    for r in bottom3:
        print(f"  {r['destination']['label']} ({r['destination']['state']}): {r['in_state_count']} in-state, {r['nearby_count']} nearby")
    print(f"\nDQ issues: {len(dq_issues)}")
    print(f"Pass 2 additional candidates (box truck/hot shot): ~{pass2_count}")
    print(f"\nOutputs:")
    print(f"  MD:   {MD_PATH}")
    print(f"  JSON: {JSON_PATH}")
    print(f"  LOG:  {LOG_PATH}")
    print("="*60)


if __name__ == "__main__":
    main()
