"""Hot shot carrier search: Tampa FL + Seguin TX, 60-mile radius.
Run: python scripts/hotshot_search_20260415.py
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import sqlite3
import sys
import time
from contextlib import closing
from pathlib import Path
from typing import Optional

# ── Setup paths ───────────────────────────────────────────────────────────────
REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

LOG_PATH = REPO / "scripts" / "logs" / "hotshot_tampa_seguin_20260415.log"
MD_PATH  = REPO / "scripts" / "logs" / "hotshot_tampa_seguin_20260415.md"
JSON_PATH = REPO / "scripts" / "logs" / "hotshot_tampa_seguin_20260415.json"
DB_PATH  = REPO / "data" / "fmcsa_li" / "insurance_lookup.sqlite"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("hotshot_search")

# ── Targets ───────────────────────────────────────────────────────────────────
TARGETS = [
    {"name": "Tampa, FL",   "lat": 27.9506, "lon": -82.4572, "state": "FL",
     "zip_prefixes": ["335", "336", "346"]},
    {"name": "Seguin, TX",  "lat": 29.5688, "lon": -97.9647, "state": "TX",
     "zip_prefixes": ["781", "780", "782"]},  # 78155 is Seguin; 782xx covers SA metro
]
RADIUS_MI = 60

# ── Haversine ─────────────────────────────────────────────────────────────────
def haversine_mi(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3958.8  # Earth radius miles
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

# ── pgeocode zip→lat/lon ──────────────────────────────────────────────────────
import pgeocode
_nomi = pgeocode.Nominatim("us")
_zip_cache: dict[str, Optional[tuple[float, float]]] = {}

def zip_to_latlon(zip5: str) -> Optional[tuple[float, float]]:
    if zip5 in _zip_cache:
        return _zip_cache[zip5]
    try:
        row = _nomi.query_postal_code(zip5)
        lat = float(row["latitude"])
        lon = float(row["longitude"])
        if math.isnan(lat) or math.isnan(lon):
            _zip_cache[zip5] = None
        else:
            _zip_cache[zip5] = (lat, lon)
    except Exception:
        _zip_cache[zip5] = None
    return _zip_cache[zip5]

# ── Hot shot name signals ─────────────────────────────────────────────────────
HOTSHOT_NAME_TOKENS = [
    "hotshot", "hot shot", "hot-shot", "expedit", "hs transport",
    "hs trucking", "hs hauling", "quick haul", "quick delivery",
    "rapid transport", "rapid freight", "same day", "rush transport",
    "express transport", "express freight", "express hauling",
]

def name_hotshot_score(name: str) -> int:
    """0 = no signal, 1 = general express/rapid, 2 = explicit hotshot"""
    name_lower = name.lower()
    if any(t in name_lower for t in ["hotshot", "hot shot", "hot-shot", "hs transport",
                                      "hs trucking", "expediting", "expedited haul"]):
        return 2
    if any(t in name_lower for t in ["expedit", "express", "rapid", "rush", "quick"]):
        return 1
    return 0

# ── Brave Search ──────────────────────────────────────────────────────────────
# Load env
_env_path = REPO / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

BRAVE_KEY = os.environ.get("BRAVE_SEARCH_API_KEY", "").strip()
BRAVE_URL = "https://api.search.brave.com/res/v1/web/search"
_EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}")
brave_call_count = 0
BRAVE_BUDGET = 50

def _brave_search(query: str) -> dict:
    """Fire one Brave query; return raw result dict or {}. Respects rate limit."""
    global brave_call_count
    if not BRAVE_KEY:
        log.warning("BRAVE_SEARCH_API_KEY not set — Brave Search skipped")
        return {}
    if brave_call_count >= BRAVE_BUDGET:
        log.warning("Brave budget exhausted (%d calls)", brave_call_count)
        return {}
    import httpx
    try:
        resp = httpx.get(
            BRAVE_URL,
            params={"q": query, "count": 10},
            headers={"Accept": "application/json",
                     "Accept-Encoding": "gzip",
                     "X-Subscription-Token": BRAVE_KEY},
            timeout=15,
        )
        brave_call_count += 1
        if resp.status_code == 429:
            log.warning("Brave 429 on %r — sleeping 6s and retrying", query)
            time.sleep(6)
            resp = httpx.get(
                BRAVE_URL,
                params={"q": query, "count": 10},
                headers={"Accept": "application/json",
                         "Accept-Encoding": "gzip",
                         "X-Subscription-Token": BRAVE_KEY},
                timeout=15,
            )
            brave_call_count += 1
        if resp.status_code in (401, 403):
            log.error("Brave auth error %d", resp.status_code)
            return {}
        resp.raise_for_status()
        time.sleep(6)  # 1 req/sec budget; 6s to be safe as instructed
        return resp.json()
    except Exception as exc:
        log.warning("Brave error for %r: %s", query, exc)
        time.sleep(6)
        return {}

def brave_hotshot_check(carrier_name: str) -> dict:
    """Check if a larger carrier has hot shot capacity. Returns evidence dict."""
    if brave_call_count >= BRAVE_BUDGET:
        return {"hotshot_capable": None, "evidence": "budget_exhausted", "url": ""}

    results: list[dict] = []
    for suffix in ["hot shot", "hotshot capacity"]:
        query = f'"{carrier_name}" {suffix}'
        data = _brave_search(query)
        web_results = (data.get("web") or {}).get("results", [])
        results.extend(web_results)
        if brave_call_count >= BRAVE_BUDGET:
            break

    if not results:
        return {"hotshot_capable": False, "evidence": "no_results", "url": ""}

    # Scan for hotshot mentions
    for item in results:
        text = ((item.get("description") or "") + " " + (item.get("title") or "") +
                " " + (item.get("url") or "")).lower()
        if any(t in text for t in ["hot shot", "hotshot", "hotshot capacity", "expedited"]):
            return {"hotshot_capable": True,
                    "evidence": item.get("description", "")[:200],
                    "url": item.get("url", "")}

    return {"hotshot_capable": False, "evidence": "searched_no_hotshot_mention", "url": ""}

# ── DB query ──────────────────────────────────────────────────────────────────
def fetch_candidates(state: str, zip_prefixes: list[str]) -> list[dict]:
    """Pull all carriers from carriers_sourcing matching state + zip prefixes.
    No min_bipd, no authority filter — we'll apply distance + power-unit filters ourselves.
    """
    if not DB_PATH.exists():
        log.error("DB not found at %s — STOPPING", DB_PATH)
        sys.exit(1)

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

    results = []
    for r in rows:
        results.append({
            "dot": r[0], "legal_name": r[1], "dba_name": r[2], "docket": r[3],
            "bus_city": r[4], "bus_state": r[5], "bus_zip": r[6], "bus_zip5": r[7],
            "bipd_filed": r[8], "common_stat": r[9], "contract_stat": r[10], "broker_stat": r[11],
        })
    return results

def get_insurance_for_dot(dot: str) -> dict:
    """Look up insurance record."""
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

# ── QCMobile power-unit lookup ─────────────────────────────────────────────────
# We'll try to pull power_units from the existing carrier DB if available
_carrier_db_path = REPO / "data" / "carriers.db"

def get_power_units(dot: str) -> Optional[int]:
    """Try to get power unit count from local carriers DB."""
    if not _carrier_db_path.exists():
        return None
    try:
        with closing(sqlite3.connect(_carrier_db_path)) as conn:
            # Try various column names
            for col in ["power_units", "Power_Units", "total_power_units"]:
                try:
                    row = conn.execute(
                        f"SELECT {col} FROM carriers WHERE dot_number = ? OR dot_number = ?",
                        (dot, dot.lstrip("0"))
                    ).fetchone()
                    if row and row[0] is not None:
                        return int(row[0])
                except Exception:
                    continue
    except Exception:
        pass
    return None

# ── Main search ───────────────────────────────────────────────────────────────
def search_origin(target: dict) -> list[dict]:
    name = target["name"]
    clat, clon = target["lat"], target["lon"]
    state = target["state"]
    zip_prefixes = target["zip_prefixes"]

    log.info("=== Searching %s (radius %d mi) ===", name, RADIUS_MI)

    candidates = fetch_candidates(state, zip_prefixes)
    log.info("Raw DB candidates for %s: %d", name, len(candidates))

    within = []
    skipped_no_geo = 0
    skipped_distance = 0

    for c in candidates:
        zip5 = c["bus_zip5"]
        coords = zip_to_latlon(zip5)
        if coords is None:
            skipped_no_geo += 1
            log.debug("No lat/lon for zip %s (carrier %s %s) — skipping", zip5, c["dot"], c["legal_name"])
            continue
        dist = haversine_mi(clat, clon, coords[0], coords[1])
        if dist > RADIUS_MI:
            skipped_distance += 1
            continue
        c["distance_mi"] = round(dist, 1)
        within.append(c)

    log.info("%s: %d within %d mi (skipped %d no-geo, %d out-of-range)",
             name, len(within), RADIUS_MI, skipped_no_geo, skipped_distance)
    return within

def classify_carrier(c: dict) -> dict:
    """Add hotshot signals and classification to a carrier dict."""
    legal_name = c.get("legal_name", "")
    dba_name = c.get("dba_name", "")
    combined_name = f"{legal_name} {dba_name}".strip()

    # Name-based hotshot score
    hs_score = max(name_hotshot_score(legal_name), name_hotshot_score(dba_name))

    # Power units from carriers.db if available
    power_units = get_power_units(c["dot"])
    c["power_units"] = power_units

    # Insurance
    ins = get_insurance_for_dot(c["dot"])
    c["insurance"] = ins

    # Classification
    # "Dedicated hot shot": power_units 1-5 OR name signals HS
    # "Larger carrier": power_units >= 20
    if power_units is not None:
        if power_units <= 5:
            fleet_class = "dedicated_hotshot"
        elif power_units >= 20:
            fleet_class = "larger_carrier"
        else:
            fleet_class = "mid_fleet"  # 6-19 units
    else:
        # No power unit data — use name signal
        if hs_score >= 2:
            fleet_class = "dedicated_hotshot"
        elif hs_score >= 1:
            fleet_class = "dedicated_hotshot"  # expedite names are likely small
        else:
            fleet_class = "unknown_size"

    c["fleet_class"] = fleet_class
    c["hs_name_score"] = hs_score
    c["hs_name_tokens"] = [t for t in HOTSHOT_NAME_TOKENS
                            if t in combined_name.lower()]
    return c

def run_brave_for_larger(carriers: list[dict]) -> None:
    """Run Brave hot shot check on larger carriers (in-place update)."""
    larger = [c for c in carriers if c["fleet_class"] == "larger_carrier"]
    log.info("Running Brave hot shot check on %d larger carriers", len(larger))
    for c in larger:
        if brave_call_count >= BRAVE_BUDGET:
            log.warning("Brave budget hit — skipping remaining larger carriers")
            c["brave_result"] = {"hotshot_capable": None, "evidence": "budget_exhausted", "url": ""}
            continue
        result = brave_hotshot_check(c["legal_name"])
        c["brave_result"] = result
        log.info("Brave [%s]: hotshot_capable=%s", c["legal_name"], result.get("hotshot_capable"))

def sort_carriers(carriers: list[dict]) -> list[dict]:
    """Sort: dedicated_hotshot first (by hs_name_score desc, then dist),
    then mid_fleet, then larger confirmed, then larger possible, then unknown."""
    def sort_key(c):
        fc = c.get("fleet_class", "unknown_size")
        brave = c.get("brave_result", {})
        hc = brave.get("hotshot_capable")

        if fc == "dedicated_hotshot":
            order = 0
        elif fc == "mid_fleet":
            order = 1
        elif fc == "larger_carrier" and hc is True:
            order = 2
        elif fc == "larger_carrier" and hc is None:
            order = 3
        elif fc == "larger_carrier" and hc is False:
            order = 4
        else:
            order = 5

        hs_score = c.get("hs_name_score", 0)
        dist = c.get("distance_mi", 999)
        pu = c.get("power_units") or 99
        return (order, -hs_score, dist, pu)

    return sorted(carriers, key=sort_key)

# ── Contact info stub ─────────────────────────────────────────────────────────
def get_contact_info(dot: str) -> dict:
    """Try to pull email/phone from existing carriers DB."""
    if not _carrier_db_path.exists():
        return {"email": None, "phone": None}
    try:
        with closing(sqlite3.connect(_carrier_db_path)) as conn:
            # Try common schema variations
            try:
                row = conn.execute(
                    "SELECT email, phone FROM carriers WHERE dot_number = ? OR dot_number = ?",
                    (dot, dot.lstrip("0"))
                ).fetchone()
                if row:
                    return {"email": row[0] or None, "phone": row[1] or None}
            except Exception:
                pass
            # Try alternate column names
            try:
                row = conn.execute(
                    "SELECT primary_email, primary_phone FROM carriers WHERE dot_number = ? OR dot_number = ?",
                    (dot, dot.lstrip("0"))
                ).fetchone()
                if row:
                    return {"email": row[0] or None, "phone": row[1] or None}
            except Exception:
                pass
    except Exception:
        pass
    return {"email": None, "phone": None}

# ── Carriers.db schema check ─────────────────────────────────────────────────
def inspect_carriers_db() -> list[str]:
    if not _carrier_db_path.exists():
        return []
    try:
        with closing(sqlite3.connect(_carrier_db_path)) as conn:
            tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            cols = []
            for (t,) in tables:
                try:
                    pragma = conn.execute(f"PRAGMA table_info({t})").fetchall()
                    cols.append(f"Table {t}: " + ", ".join(r[1] for r in pragma))
                except Exception:
                    cols.append(f"Table {t}: error reading schema")
            return cols
    except Exception as e:
        return [f"Error: {e}"]

# ── Report generation ─────────────────────────────────────────────────────────
def carrier_row_md(c: dict, idx: int) -> str:
    ins = c.get("insurance", {})
    bipd = ins.get("bipd_liability", 0)
    bipd_str = f"${bipd:,}" if bipd else "—"
    brave = c.get("brave_result", {})
    brave_str = ""
    if brave:
        hc = brave.get("hotshot_capable")
        ev = brave.get("evidence", "")[:80]
        url = brave.get("url", "")
        if hc is True:
            brave_str = f"YES — {ev} [{url}]"
        elif hc is False:
            brave_str = "No hotshot mention found"
        elif hc is None:
            brave_str = brave.get("evidence", "")

    contact = c.get("contact", {})
    email = contact.get("email") or "—"
    phone = contact.get("phone") or "—"
    pu = c.get("power_units")
    pu_str = str(pu) if pu is not None else "—"
    hs_tokens = ", ".join(c.get("hs_name_tokens", [])) or "—"
    fleet_class = c.get("fleet_class", "")
    flag = ""
    if email == "—" and phone == "—":
        flag = " ⚑ MANUAL LOOKUP"
    elif email == "—":
        flag = " ⚑ EMAIL NEEDED"

    return (
        f"| {c['dot']} | {c['legal_name'][:40]} | {c['bus_city']} | {c['bus_state']} "
        f"| {c['distance_mi']} | {pu_str} | BIPD:{bipd_str} / {hs_tokens} "
        f"| {brave_str} | {email}{flag} | {phone} |"
    )

def generate_md(results_by_origin: dict) -> str:
    lines = [
        "# Hot Shot Carrier Search — Tampa FL + Seguin TX",
        f"**Run date:** 2026-04-15  |  **Radius:** 60 miles  |  **DB snapshot:** 2026-04-14",
        "",
    ]

    # Executive summary
    lines.append("## Executive Summary")
    lines.append("")
    for origin_name, carriers in results_by_origin.items():
        dedicated = [c for c in carriers if c["fleet_class"] == "dedicated_hotshot"]
        mid = [c for c in carriers if c["fleet_class"] == "mid_fleet"]
        larger_confirmed = [c for c in carriers if c["fleet_class"] == "larger_carrier"
                            and c.get("brave_result", {}).get("hotshot_capable") is True]
        larger_possible = [c for c in carriers if c["fleet_class"] == "larger_carrier"
                           and c.get("brave_result", {}).get("hotshot_capable") is None]
        larger_no = [c for c in carriers if c["fleet_class"] == "larger_carrier"
                     and c.get("brave_result", {}).get("hotshot_capable") is False]
        unknown = [c for c in carriers if c["fleet_class"] == "unknown_size"]

        lines.append(f"### {origin_name}")
        lines.append(f"- Total within 60 mi: **{len(carriers)}**")
        lines.append(f"- Dedicated hot shot (1-5 units or name signal): **{len(dedicated)}**")
        lines.append(f"- Mid-fleet (6-19 units): **{len(mid)}**")
        lines.append(f"- Larger carriers — Brave-confirmed hot shot capable: **{len(larger_confirmed)}**")
        lines.append(f"- Larger carriers — possible hot shot (budget/no search): **{len(larger_possible)}**")
        lines.append(f"- Larger carriers — no hot shot evidence: **{len(larger_no)}**")
        lines.append(f"- Unknown fleet size (no QCMobile data): **{len(unknown)}**")
        lines.append("")

    lines.append(f"**Total Brave API calls made:** {brave_call_count}")
    lines.append("")

    # Per-origin tables
    for origin_name, carriers in results_by_origin.items():
        lines.append(f"## {origin_name} — Carrier Table")
        lines.append("")
        lines.append("| DOT | Name | City | State | Dist (mi) | Power Units | Equipment/Tags | Brave Evidence | Email | Phone |")
        lines.append("|-----|------|------|-------|-----------|-------------|----------------|----------------|-------|-------|")
        prev_class = None
        for c in carriers:
            fc = c.get("fleet_class", "")
            brave = c.get("brave_result", {})
            hc = brave.get("hotshot_capable")
            # Section header rows
            if fc != prev_class:
                section_label = {
                    "dedicated_hotshot": "--- DEDICATED HOT SHOT ---",
                    "mid_fleet": "--- MID FLEET (6-19 units) ---",
                    "larger_carrier": (
                        "--- LARGER CARRIERS: BRAVE-CONFIRMED ---" if hc is True
                        else "--- LARGER CARRIERS: POSSIBLE ---" if hc is None
                        else "--- LARGER CARRIERS: NO EVIDENCE ---"
                    ),
                    "unknown_size": "--- UNKNOWN FLEET SIZE ---",
                }.get(fc, f"--- {fc.upper()} ---")
                lines.append(f"| **{section_label}** | | | | | | | | | |")
                prev_class = fc
            lines.append(carrier_row_md(c, 0))
        lines.append("")

    # Manual lookup flags
    all_carriers = [c for carriers in results_by_origin.values() for c in carriers]
    needs_manual = [c for c in all_carriers
                    if not (c.get("contact", {}).get("email") or c.get("contact", {}).get("phone"))]
    if needs_manual:
        lines.append("## Carriers Needing Manual FMCSA Lookup")
        lines.append(f"*{len(needs_manual)} carriers have no contact info in local DB.*")
        lines.append("")
        lines.append("| DOT | Name | City | State | Distance |")
        lines.append("|-----|------|------|-------|----------|")
        for c in needs_manual[:50]:  # cap at 50 in report
            lines.append(f"| {c['dot']} | {c['legal_name'][:40]} | {c['bus_city']} | {c['bus_state']} | {c['distance_mi']} mi |")
        lines.append("")

    return "\n".join(lines)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("Starting hot shot search — Tampa FL + Seguin TX")
    log.info("DB path: %s", DB_PATH)

    # Check carriers.db schema
    db_schema = inspect_carriers_db()
    if db_schema:
        log.info("carriers.db schema: %s", db_schema)
    else:
        log.info("carriers.db not found at %s — contact info will be empty", _carrier_db_path)

    results_by_origin: dict[str, list[dict]] = {}

    for target in TARGETS:
        origin_name = target["name"]
        within = search_origin(target)

        # Classify each carrier
        for c in within:
            classify_carrier(c)

        # Run Brave on larger carriers
        run_brave_for_larger(within)

        # Pull contact info
        for c in within:
            c["contact"] = get_contact_info(c["dot"])

        # Sort
        sorted_carriers = sort_carriers(within)
        results_by_origin[origin_name] = sorted_carriers

        # Stats
        dedicated = [c for c in sorted_carriers if c["fleet_class"] == "dedicated_hotshot"]
        larger_confirmed = [c for c in sorted_carriers
                            if c["fleet_class"] == "larger_carrier"
                            and c.get("brave_result", {}).get("hotshot_capable") is True]
        no_email = [c for c in sorted_carriers if not c.get("contact", {}).get("email")]
        no_phone = [c for c in sorted_carriers if not c.get("contact", {}).get("phone")]
        no_city  = [c for c in sorted_carriers if not c.get("bus_city")]

        log.info(
            "%s — total=%d dedicated_hs=%d brave_confirmed=%d "
            "no_email=%d no_phone=%d no_city=%d",
            origin_name, len(sorted_carriers), len(dedicated), len(larger_confirmed),
            len(no_email), len(no_phone), len(no_city)
        )

    # Write MD
    md_content = generate_md(results_by_origin)
    MD_PATH.write_text(md_content, encoding="utf-8")
    log.info("MD written: %s", MD_PATH)

    # Write JSON
    json_output = {}
    for origin_name, carriers in results_by_origin.items():
        json_output[origin_name] = carriers
    JSON_PATH.write_text(json.dumps(json_output, indent=2, default=str), encoding="utf-8")
    log.info("JSON written: %s", JSON_PATH)

    # Summary stats for Sasha
    log.info("=== FINAL SUMMARY ===")
    for origin_name, carriers in results_by_origin.items():
        dedicated = [c for c in carriers if c["fleet_class"] == "dedicated_hotshot"]
        mid = [c for c in carriers if c["fleet_class"] == "mid_fleet"]
        larger_confirmed = [c for c in carriers
                            if c["fleet_class"] == "larger_carrier"
                            and c.get("brave_result", {}).get("hotshot_capable") is True]
        larger_possible = [c for c in carriers
                           if c["fleet_class"] == "larger_carrier"
                           and c.get("brave_result", {}).get("hotshot_capable") is None]
        larger_no = [c for c in carriers
                     if c["fleet_class"] == "larger_carrier"
                     and c.get("brave_result", {}).get("hotshot_capable") is False]
        unknown = [c for c in carriers if c["fleet_class"] == "unknown_size"]
        no_email = [c for c in carriers if not c.get("contact", {}).get("email")]
        no_phone = [c for c in carriers if not c.get("contact", {}).get("phone")]
        log.info(
            "  %s: total=%d | dedicated_hs=%d | mid=%d | larger(confirmed=%d possible=%d no=%d) | unknown=%d | no_email=%d no_phone=%d",
            origin_name, len(carriers), len(dedicated), len(mid),
            len(larger_confirmed), len(larger_possible), len(larger_no),
            len(unknown), len(no_email), len(no_phone)
        )
    log.info("Total Brave API calls: %d / %d budget", brave_call_count, BRAVE_BUDGET)

    # Top 5 candidates across both origins
    all_carriers = []
    for origin_name, carriers in results_by_origin.items():
        for c in carriers:
            c["_origin"] = origin_name
        all_carriers.extend(carriers)

    # Score for top 5: prioritize dedicated hs with name signal + small distance
    def top5_score(c):
        fc = c.get("fleet_class", "unknown_size")
        hs = c.get("hs_name_score", 0)
        dist = c.get("distance_mi", 999)
        has_contact = bool(c.get("contact", {}).get("email") or c.get("contact", {}).get("phone"))
        # Lower is better
        class_rank = {"dedicated_hotshot": 0, "mid_fleet": 1, "larger_carrier": 2, "unknown_size": 3}.get(fc, 4)
        return (class_rank, -hs, -int(has_contact), dist)

    top5 = sorted(all_carriers, key=top5_score)[:5]
    log.info("=== TOP 5 CANDIDATES ===")
    for i, c in enumerate(top5, 1):
        contact = c.get("contact", {})
        log.info(
            "  %d. DOT=%s | %s | %s, %s | %.1f mi | fleet=%s | hs_score=%d | email=%s | phone=%s | origin=%s",
            i, c["dot"], c["legal_name"], c["bus_city"], c["bus_state"],
            c["distance_mi"], c["fleet_class"], c["hs_name_score"],
            contact.get("email") or "—", contact.get("phone") or "—",
            c.get("_origin", "")
        )

    return results_by_origin

if __name__ == "__main__":
    results = main()
    print(f"\nDone. Brave calls used: {brave_call_count}")
    print(f"MD:   {MD_PATH}")
    print(f"JSON: {JSON_PATH}")
    print(f"Log:  {LOG_PATH}")
