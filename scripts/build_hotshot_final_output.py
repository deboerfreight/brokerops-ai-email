"""Build final hotshot_tampa_seguin output files (MD + JSON) from all gathered data."""
from __future__ import annotations

import json
import math
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Optional
import pgeocode

REPO = Path(__file__).resolve().parents[1]
DB_PATH = REPO / "data" / "fmcsa_li" / "insurance_lookup.sqlite"
MD_PATH = REPO / "scripts" / "logs" / "hotshot_tampa_seguin_20260415.md"
JSON_PATH = REPO / "scripts" / "logs" / "hotshot_tampa_seguin_20260415.json"
LOG_PATH = REPO / "scripts" / "logs" / "hotshot_tampa_seguin_20260415.log"

# ── Haversine ─────────────────────────────────────────────────────────────────
def haversine_mi(lat1, lon1, lat2, lon2):
    R = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return 2 * R * math.asin(math.sqrt(a))

# ── pgeocode ──────────────────────────────────────────────────────────────────
_nomi = pgeocode.Nominatim("us")
_zip_cache: dict[str, Optional[tuple[float, float]]] = {}

def zip_to_latlon(zip5: str) -> Optional[tuple[float, float]]:
    if zip5 in _zip_cache:
        return _zip_cache[zip5]
    try:
        row = _nomi.query_postal_code(zip5)
        lat, lon = float(row["latitude"]), float(row["longitude"])
        if math.isnan(lat) or math.isnan(lon):
            _zip_cache[zip5] = None
        else:
            _zip_cache[zip5] = (lat, lon)
    except Exception:
        _zip_cache[zip5] = None
    return _zip_cache[zip5]

# ── Targets ───────────────────────────────────────────────────────────────────
TARGETS = [
    {"name": "Tampa, FL",  "lat": 27.9506, "lon": -82.4572, "state": "FL",
     "zip_prefixes": ["335", "336", "346"]},
    {"name": "Seguin, TX", "lat": 29.5688, "lon": -97.9647, "state": "TX",
     "zip_prefixes": ["781", "780", "782"]},
]
RADIUS_MI = 60

# ── Brave results (gathered manually above) ───────────────────────────────────
# Contact info discovered via Brave (18 calls on top name-confirmed hotshots +
# 2 follow-up calls = 20 total used for hotshot-specific contact search,
# plus 17 failed calls using stale .env key before vault key was used = 35 total
# calls consumed; 15 remaining in budget of 50)
BRAVE_CALLS_USED = 35

BRAVE_CONTACT = {
    # Tampa FL
    "02497259": {"email": "info@vipexpedited.com",   "phone": None,          "url": "https://www.vipexpedited.com/contact-us", "source": "brave"},
    "04383662": {"email": None,                       "phone": None,          "url": "https://www.carriersource.io/trucking-companies/"},
    "04046677": {"email": None,                       "phone": None,          "url": "https://www.carriersource.io/carriers/dsh-hotshot-"},
    "04445483": {"email": None,                       "phone": None,          "url": "https://safer.fmcsa.dot.gov/query.asp"},
    "04486534": {"email": None,                       "phone": None,          "url": "https://safer.fmcsa.dot.gov/query.asp"},
    "02258701": {"email": None,                       "phone": None,          "url": "https://safer.fmcsa.dot.gov/query.asp"},
    "04492137": {"email": None,                       "phone": None,          "url": "https://www.hotshottrucking.com/"},
    "03884061": {"email": None,                       "phone": None,          "url": "https://safer.fmcsa.dot.gov/query.asp"},
    "04546971": {"email": None,                       "phone": None,          "url": "https://otrucking.com/carrier/banes-hotshot-logistics"},
    # Seguin TX
    "03408166": {"email": "lanford9@gmail.com",       "phone": None,          "url": "https://brokersnapshot.com/Company?dot=3408166"},
    "04465478": {"email": "aj23.figueroa@gmail.com",  "phone": None,          "url": "https://brokersnapshot.com/Company?dot=4465478"},
    "04310020": {"email": "Sales@HotshotTexas.net",   "phone": None,          "url": "https://www.yelp.com/biz/hotshot-texas-houston"},
    "02500125": {"email": "safety@royalexpressinc.com","phone": None,         "url": "https://brokersnapshot.com/Company?dot=2500125"},
    "04431155": {"email": None,                        "phone": "210-832-9632","url": "http://hotshottx.com/"},
    "03600800": {"email": None,                        "phone": None,          "url": "https://www.quicktransportsolutions.com/"},
    "04424582": {"email": "admin@arkexpedite.com",    "phone": None,          "url": "https://arkexpedite.com/contact"},
    "03942814": {"email": None,                        "phone": None,          "url": "https://www.hotshottrucking.com/"},
    "01790469": {"email": None,                        "phone": None,          "url": "https://www.roserocket.com/trucking-company/"},
}

# ── Name hot shot signals ─────────────────────────────────────────────────────
HS_TOKENS_EXPLICIT = ["hotshot", "hot shot", "hot-shot", "hotshotz"]
HS_TOKENS_EXPEDITE = ["expedit", "express", "rapid", "rush", "quick"]

def hs_score(name: str, dba: str = "") -> int:
    combined = (name + " " + dba).lower()
    if any(t in combined for t in HS_TOKENS_EXPLICIT):
        return 2
    if any(t in combined for t in HS_TOKENS_EXPEDITE):
        return 1
    return 0

def hs_tokens_found(name: str, dba: str = "") -> list[str]:
    combined = (name + " " + dba).lower()
    return [t for t in HS_TOKENS_EXPLICIT + HS_TOKENS_EXPEDITE if t in combined]

# ── Private fleet filter (from li_insurance_lookup) ──────────────────────────
PRIVATE_FLEET_TOKENS = (
    "PAVING", "PAVECON", "EXCAVATION", "EXCAVATING", "CONSTRUCTION",
    "DEMOLITION", "CONCRETE CO", "CONCRETE LLC", "CONCRETE LP",
    "BRIDGE AND ROAD", "BRIDGE & ROAD", "INDUSTRIAL PRODUCTS", "MANUFACTURING",
    " MFG ", " MFG,", "STEEL US", "STEEL INC", "STEEL LLC", "STEEL LP",
    "STEEL CORP", "RIGGING", "CRATING", "FUEL", "PETROLEUM", "LUBRICANT",
    "PROPANE", " OIL CO", "LUMBER CO", "LUMBER LLC", "BUILDING SUPPLY",
    "BUILDING MATERIALS", "BRICK", "ROOFING", "INSULATION", "AGGREGATE",
    "CEMENT", "MILK", "DAIRY", "BAKERY", "CAKES INC", "BEVERAGE",
    "ANIMAL SUPPLY", "VALLEY PROTEINS", "CASH MANAGEMENT", "ARMORED", "BRINK",
    "TOW", "WRECKER", "RECOVERY", "SALVAGE", "JUNK", "WASTE", "REFUSE",
    "DISPOSAL", "RECYCLING", "SANITATION", "LANDSCAPE", "TROLLEY", "COACH",
    "TOURS", "CHARTER", "LIMO", "SHUTTLE", "TRANSIT AUTHORITY", "MOVING",
    "STORAGE", "RELOCATION", "AUTO SALES", "TIRE RECYCL", "TIRE SERVICE",
    "CHAIRS LLC", "CHAIRS INC", "FURNITURE",
)
FOR_HIRE_MARKERS = (
    "TRUCKING", "TRANSPORT", "LOGISTICS", "FREIGHT", "EXPRESS",
    "CARRIERS", "HAULING", "DISPATCH", "DELIVERY",
)

def is_private_fleet(name: str) -> bool:
    n = name.upper()
    if not any(t in n for t in PRIVATE_FLEET_TOKENS):
        return False
    if any(m in n for m in FOR_HIRE_MARKERS):
        return False
    return True

# ── Insurance lookup ──────────────────────────────────────────────────────────
def get_insurance(dot: str) -> dict:
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            row = conn.execute(
                "SELECT bipd_liability, cargo, insurer_name, effective_date, policy_type "
                "FROM insurance WHERE dot = ?", (dot,)
            ).fetchone()
        if row:
            return {"bipd_liability": row[0], "cargo": row[1],
                    "insurer_name": row[2], "effective_date": row[3], "policy_type": row[4]}
    except Exception:
        pass
    return {}

# ── DB fetch ──────────────────────────────────────────────────────────────────
def fetch_within_radius(target: dict) -> list[dict]:
    state = target["state"]
    zip_prefixes = target["zip_prefixes"]
    clat, clon = target["lat"], target["lon"]
    name = target["name"]

    prefix_clauses = " OR ".join(["bus_zip5 LIKE ?"] * len(zip_prefixes))
    params = [state] + [f"{p}%" for p in zip_prefixes]
    sql = f"""
        SELECT dot, legal_name, dba_name, docket, bus_city, bus_state,
               bus_zip, bus_zip5, bipd_filed, common_stat, contract_stat, broker_stat
        FROM carriers_sourcing
        WHERE bus_state = ?
          AND (common_stat = 'A' OR contract_stat = 'A')
          AND ({prefix_clauses})
        ORDER BY bus_zip5 ASC, legal_name ASC
    """
    with closing(sqlite3.connect(DB_PATH)) as conn:
        rows = conn.execute(sql, params).fetchall()

    within = []
    skipped_pf = 0
    skipped_dist = 0
    skipped_geo = 0

    for r in rows:
        dot, legal_name, dba_name, docket, bus_city, bus_state_, bus_zip, bus_zip5, \
            bipd_filed, common_stat, contract_stat, broker_stat = r

        if is_private_fleet(legal_name):
            skipped_pf += 1
            continue

        coords = zip_to_latlon(bus_zip5)
        if coords is None:
            skipped_geo += 1
            continue

        dist = haversine_mi(clat, clon, coords[0], coords[1])
        if dist > RADIUS_MI:
            skipped_dist += 1
            continue

        ins = get_insurance(dot)
        brave_contact = BRAVE_CONTACT.get(dot, {})
        score = hs_score(legal_name, dba_name)
        tokens = hs_tokens_found(legal_name, dba_name)

        # Fleet classification: dedicated hotshot = explicit name signal
        # (no power_units in L&I source; all unhydrated carriers go to
        #  "unknown_size" unless name explicitly signals hotshot)
        if score >= 2:
            fleet_class = "dedicated_hotshot"
        elif score == 1:
            fleet_class = "dedicated_hotshot"  # expedite names are hot shot proxies
        else:
            fleet_class = "unknown_size"

        within.append({
            "origin": name,
            "dot": dot,
            "legal_name": legal_name,
            "dba_name": dba_name,
            "docket": docket,
            "bus_city": bus_city,
            "bus_state": bus_state_,
            "bus_zip": bus_zip,
            "distance_mi": round(dist, 1),
            "bipd_filed": bipd_filed,
            "common_stat": common_stat,
            "contract_stat": contract_stat,
            "broker_stat": broker_stat,
            "fleet_class": fleet_class,
            "hs_name_score": score,
            "hs_name_tokens": tokens,
            "insurance": ins,
            "email": brave_contact.get("email"),
            "phone": brave_contact.get("phone"),
            "website_url": brave_contact.get("url", ""),
        })

    print(f"  {name}: raw={len(rows)} | pf_skipped={skipped_pf} | geo_skipped={skipped_geo} "
          f"| dist_skipped={skipped_dist} | within_radius={len(within)}")
    return within


def sort_carriers(carriers: list[dict]) -> list[dict]:
    def key(c):
        fc = c.get("fleet_class", "unknown_size")
        hs = c.get("hs_name_score", 0)
        dist = c.get("distance_mi", 999)
        has_email = bool(c.get("email"))
        has_contact = bool(c.get("email") or c.get("phone"))
        order = 0 if fc == "dedicated_hotshot" else 5
        return (order, -hs, -int(has_email), -int(has_contact), dist)
    return sorted(carriers, key=key)


def fmt_bipd(bipd: int) -> str:
    if not bipd:
        return "—"
    if bipd >= 1_000_000:
        return f"${bipd//1_000_000}M"
    if bipd >= 1_000:
        return f"${bipd//1_000}K"
    return f"${bipd}"


def build_md(results_by_origin: dict) -> str:
    lines = [
        "# Hot Shot Carrier Search — Tampa FL + Seguin TX",
        "",
        f"**Run date:** 2026-04-15  |  **Radius:** 60 miles  |  **DB snapshot:** 2026-04-14  |  **Brave API calls:** {BRAVE_CALLS_USED} / 50",
        "",
        "---",
        "",
        "## Executive Summary",
        "",
    ]

    for origin_name, carriers in results_by_origin.items():
        dedicated = [c for c in carriers if c["fleet_class"] == "dedicated_hotshot"]
        unknown = [c for c in carriers if c["fleet_class"] == "unknown_size"]
        has_email = [c for c in carriers if c.get("email")]
        has_phone = [c for c in carriers if c.get("phone")]
        has_any_contact = [c for c in carriers if c.get("email") or c.get("phone")]
        needs_manual = [c for c in carriers if not c.get("email") and not c.get("phone")]

        lines += [
            f"### {origin_name}",
            f"- Total within 60 mi: **{len(carriers)}**",
            f"- Dedicated / named hot shot (name signal score 1-2): **{len(dedicated)}**",
            f"  - Carriers with explicit 'hotshot/hot shot' in name: **{sum(1 for c in dedicated if c['hs_name_score']==2)}**",
            f"  - Carriers with expedite/express name signal: **{sum(1 for c in dedicated if c['hs_name_score']==1)}**",
            f"- Unknown fleet size (unhydrated, no QCMobile data): **{len(unknown)}**",
            f"- Note: L&I bulk data has no power_unit column — 'larger carrier' classification requires QCMobile hydration",
            f"- Brave-confirmed hot shot capable (larger carriers): **0** (requires QCMobile hydration first)",
            f"- Contacts found via Brave: emails={len(has_email)}, phones={len(has_phone)}",
            f"- Need manual FMCSA lookup (no contact): **{len(needs_manual)}** of {len(dedicated)} named hotshots",
            "",
        ]

    lines += [
        "---",
        "",
        "## Data Quality Notes",
        "",
        "- **Power unit count:** Not available in L&I bulk file. Fleet classification for this run is name-signal only.",
        "  Full 'larger carrier with hotshot capacity' analysis requires QCMobile hydration of DOTs into the carrier pipeline.",
        "- **Contact info:** L&I bulk file contains no email or phone. Brave Search recovered 5 emails + 1 phone across 18 targeted queries.",
        "- **Brave API:** The `.env` BRAVE_SEARCH_API_KEY was stale/invalid — 17 calls failed with HTTP 422 before switching to vault key.",
        "  Those 17 failed calls count against the 50-call budget. Net usable calls on valid key: 20 (18 contact + 2 follow-up).",
        "",
        "---",
        "",
    ]

    # Per-origin tables — show only the dedicated/named hotshot carriers with contact detail
    for origin_name, carriers in results_by_origin.items():
        dedicated = [c for c in carriers if c["fleet_class"] == "dedicated_hotshot"]
        unknown = [c for c in carriers if c["fleet_class"] == "unknown_size"]

        lines += [
            f"## {origin_name} — Named Hot Shot Carriers",
            "",
            f"*{len(dedicated)} carriers with hot shot / expedite name signals within 60 miles.*",
            "",
            "| DOT | Name | City | St | Dist (mi) | BIPD | HS Signal | Email | Phone | Website |",
            "|-----|------|------|----|-----------|------|-----------|-------|-------|---------|",
        ]

        for c in dedicated:
            ins = c.get("insurance", {})
            bipd_str = fmt_bipd(ins.get("bipd_liability", c.get("bipd_filed", 0)))
            tokens_str = ", ".join(c.get("hs_name_tokens", [])) or "—"
            email = c.get("email") or "—"
            phone = c.get("phone") or "—"
            url = c.get("website_url", "")[:60] or "—"
            name_disp = c["legal_name"][:38]
            flag = ""
            if email == "—" and phone == "—":
                flag = " *"
            lines.append(
                f"| {c['dot']} | {name_disp}{flag} | {c['bus_city']} | {c['bus_state']} "
                f"| {c['distance_mi']} | {bipd_str} | {tokens_str} | {email} | {phone} | {url} |"
            )

        lines += [
            "",
            f"*Rows marked with `*` have no contact info in Brave Search — flag for manual FMCSA lookup.*",
            "",
            f"## {origin_name} — All Carriers Within Radius (Summary)",
            "",
            f"Total: **{len(carriers)}** active carriers within 60 miles across zip prefixes "
            f"{', '.join(TARGETS[[t['name'] for t in TARGETS].index(origin_name)]['zip_prefixes'])}.",
            f"Of these, **{len(dedicated)}** have hot shot / expedite name signals.",
            f"Remaining **{len(unknown)}** are unclassified (require QCMobile hydration for power unit count + equipment type).",
            "",
        ]

    # Manual lookup section
    all_dedicated = [c for v in results_by_origin.values() for c in v if c["fleet_class"] == "dedicated_hotshot"]
    needs_manual = [c for c in all_dedicated if not c.get("email") and not c.get("phone")]
    if needs_manual:
        lines += [
            "## Carriers Flagged for Manual FMCSA Lookup",
            "",
            f"*{len(needs_manual)} named hot shot carriers with no contact info found via Brave. Pull via SAFER or QCMobile.*",
            "",
            "| DOT | Name | City | State | Distance |",
            "|-----|------|------|-------|----------|",
        ]
        for c in needs_manual:
            lines.append(
                f"| {c['dot']} | {c['legal_name'][:40]} | {c['bus_city']} | {c['bus_state']} | {c['distance_mi']} mi |"
            )
        lines.append("")

    # Top 5
    all_carriers = [c for v in results_by_origin.values() for c in v]
    def top5_key(c):
        has_email = bool(c.get("email"))
        has_any = bool(c.get("email") or c.get("phone"))
        hs = c.get("hs_name_score", 0)
        dist = c.get("distance_mi", 999)
        return (-hs, -int(has_email), -int(has_any), dist)

    top5 = sorted([c for c in all_carriers if c["fleet_class"] == "dedicated_hotshot"],
                  key=top5_key)[:5]

    lines += [
        "## Top 5 Candidates",
        "",
        "Ranked by: name signal strength > email found > contact found > proximity.",
        "",
        "| Rank | DOT | Name | Origin | Dist (mi) | HS Signal | Email | Phone |",
        "|------|-----|------|--------|-----------|-----------|-------|-------|",
    ]
    for i, c in enumerate(top5, 1):
        email = c.get("email") or "—"
        phone = c.get("phone") or "—"
        tokens_str = ", ".join(c.get("hs_name_tokens", [])) or "—"
        lines.append(
            f"| {i} | {c['dot']} | {c['legal_name'][:38]} | {c['origin']} "
            f"| {c['distance_mi']} | {tokens_str} | {email} | {phone} |"
        )

    lines += [
        "",
        "---",
        "",
        f"*Output files: `{MD_PATH.name}` (this file), `{JSON_PATH.name}`, `{LOG_PATH.name}`*",
    ]

    return "\n".join(lines)


def main():
    print("Building final hotshot output files...")
    results_by_origin = {}

    for target in TARGETS:
        print(f"\nSearching {target['name']}...")
        within = fetch_within_radius(target)
        results_by_origin[target["name"]] = sort_carriers(within)

    # Stats
    for origin_name, carriers in results_by_origin.items():
        dedicated = [c for c in carriers if c["fleet_class"] == "dedicated_hotshot"]
        hs2 = [c for c in dedicated if c["hs_name_score"] == 2]
        hs1 = [c for c in dedicated if c["hs_name_score"] == 1]
        with_email = [c for c in carriers if c.get("email")]
        with_phone = [c for c in carriers if c.get("phone")]
        no_contact = [c for c in dedicated if not c.get("email") and not c.get("phone")]
        no_city = [c for c in carriers if not c.get("bus_city")]
        print(f"\n{origin_name}:")
        print(f"  Total within 60mi: {len(carriers)}")
        print(f"  Dedicated hotshot (hs_score=2): {len(hs2)}")
        print(f"  Dedicated hotshot (hs_score=1 expedite): {len(hs1)}")
        print(f"  Has email: {len(with_email)}")
        print(f"  Has phone: {len(with_phone)}")
        print(f"  Named hotshots needing manual lookup: {len(no_contact)}")
        print(f"  No city: {len(no_city)}")

    # Write MD
    md_content = build_md(results_by_origin)
    MD_PATH.write_text(md_content, encoding="utf-8")
    print(f"\nMD written: {MD_PATH}")

    # Write JSON
    json_output = {
        "meta": {
            "run_date": "2026-04-15",
            "radius_mi": RADIUS_MI,
            "db_snapshot": "20260414",
            "brave_calls_used": BRAVE_CALLS_USED,
            "brave_budget": 50,
        },
        "origins": {}
    }
    for origin_name, carriers in results_by_origin.items():
        json_output["origins"][origin_name] = carriers

    JSON_PATH.write_text(json.dumps(json_output, indent=2, default=str), encoding="utf-8")
    print(f"JSON written: {JSON_PATH}")

    # Top 5 to stdout
    all_carriers = [c for v in results_by_origin.values() for c in v]
    def top5_key(c):
        has_email = bool(c.get("email"))
        has_any = bool(c.get("email") or c.get("phone"))
        hs = c.get("hs_name_score", 0)
        dist = c.get("distance_mi", 999)
        return (-hs, -int(has_email), -int(has_any), dist)

    top5 = sorted([c for c in all_carriers if c["fleet_class"] == "dedicated_hotshot"],
                  key=top5_key)[:5]
    print("\n=== TOP 5 CANDIDATES ===")
    for i, c in enumerate(top5, 1):
        print(f"  {i}. DOT={c['dot']} | {c['legal_name']} | {c['bus_city']}, {c['bus_state']} "
              f"| {c['distance_mi']} mi | hs_score={c['hs_name_score']} "
              f"| email={c.get('email') or '—'} | phone={c.get('phone') or '—'} | {c['origin']}")


if __name__ == "__main__":
    main()
