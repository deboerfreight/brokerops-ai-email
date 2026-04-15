"""
Carrier main tab audit — 2026-04-15.

Read-only audit of Carrier Database tab. Generates a markdown review queue
for manual eyeball approval/reject. Does NOT modify any sheet values.

Run from project root:
    PYTHONPATH=. python scripts/carrier_audit_20260415.py [--verify N]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional

from app.sheets import read_range

SPREADSHEET_ID = "1B5nzDCisdpG29bI0MjLtD8dIE2rsjdXg3Qncj-E58WE"
MAIN_TAB = "Carrier Database"
OUTPUT_PATH = Path("C:/Users/Owner/Desktop/Claude Work/output/carrier_review_queue_20260415.md")

# Phase 2 playwright verification results (merged in after Phase 1 run).
# Key = DOT string.
PLAYWRIGHT_VERIFICATIONS: dict[str, dict] = {
    "3355445": {
        "name": "Universal Waste Management LLC",
        "finding": "brokersnapshot shows PRIVATE(PROPERTY) + cargo=GARBAGE/REFUSE",
        "new_severity": "HIGH",
        "new_flag": "fail_private_fleet",
        "new_reason": "Playwright confirmed PRIVATE(PROPERTY) operation type; waste hauler operating private fleet, not for-hire",
    },
    "3579107": {
        "name": "Hall Hauling & Construction LLC",
        "finding": "brokersnapshot: no PRIVATE(PROPERTY) marker; cargo=BUILDING MATERIALS, CONSTRUCTION",
        "new_severity": "LOW",
        "new_flag": "private_fleet_review_cleared",
        "new_reason": "Playwright — no private-fleet marker; for-hire construction hauler (likely keep)",
    },
    "1071704": {
        "name": "Industrial Waste Service INC",
        "finding": "brokersnapshot: no PRIVATE(PROPERTY) marker; cargo=GENERAL FREIGHT",
        "new_severity": "LOW",
        "new_flag": "waste_hauler_review_cleared",
        "new_reason": "Playwright — general freight for-hire despite 'waste' in name; likely keep",
    },
    "1026131": {
        "name": "Fitzgerald Excavating & Trucking INC",
        "finding": "brokersnapshot shows PRIVATE(PROPERTY) + cargo=CONSTRUCTION, MACHINERY",
        "new_severity": "HIGH",
        "new_flag": "fail_private_fleet",
        "new_reason": "Playwright confirmed PRIVATE(PROPERTY) operation type; excavating private fleet, not for-hire",
    },
}

# ── Keyword families ──────────────────────────────────────────────────────

PASSENGER_KEYWORDS = [
    'BUS CO', 'BUS INC', 'BUS LLC', 'COACH', 'CHARTER',
    'TOURS', 'LIMO', 'LIMOUSINE', 'SHUTTLE', 'TRANSIT AUTHORITY',
    'TAXI', 'PASSENGER', 'TROLLEY',
]

TOW_KEYWORDS = [
    'TOW', 'WRECKER', 'RECOVERY', 'SALVAGE', 'JUNK', 'AUTO SALES',
]

WASTE_KEYWORDS = [
    'WASTE', 'REFUSE', 'DISPOSAL', 'SANITATION', 'RECYCLING', 'SCRAP',
]

PRIVATE_FLEET_KEYWORDS = [
    # Fuel / oil / lubricant / propane
    'FUEL', 'PETROLEUM', 'LUBRICANT', 'PROPANE', 'LPG', 'OIL CO',
    'OIL INC', 'OIL LLC',
    # Building materials manufacturers
    'LUMBER CO', 'LUMBER INC', 'LUMBER LLC', 'BUILDING SUPPLY',
    'BUILDING MATERIALS', 'BRICK', 'BLOCK', 'CEMENT CO', 'CEMENT INC',
    'CONCRETE CO', 'CONCRETE INC', 'CONCRETE LLC',
    'STEEL INC', 'STEEL LLC', 'STEEL LP', 'STEEL US', 'STEEL CO',
    'STEEL CORP', 'GLASS CO', 'WINDOW CO', 'WINDOW INC',
    'DOOR CO', 'ROOFING', 'INSULATION', 'PAVING', 'PAVECON',
    'AGGREGATE', 'ASPHALT', 'REDI-MIX', 'READY-MIX', 'READY MIX',
    'QUARRY',
    # Construction services
    'CONSTRUCTION', 'EXCAVATION', 'EXCAVATING', 'DEMOLITION',
    'LANDSCAPE', 'LANDSCAPING', 'BRIDGE AND ROAD', 'BRIDGE & ROAD',
    'CONTRACTOR', 'CONTRACTING',
    # Industrial
    'INDUSTRIAL PRODUCTS', 'INDUSTRIES INC', 'INDUSTRIES LLC',
    'MANUFACTURING', ' MFG ', 'PRODUCTS INC',
    'RIGGING', 'CRATING',
    # Food / dairy / beverage (HHG-adjacent private fleets)
    'MILK', 'DAIRY', 'BAKERY', 'CAKES INC', 'CAKES LLC',
    'BEVERAGE', 'ANIMAL SUPPLY', 'VALLEY PROTEINS', 'MEAT CO',
    'POULTRY', 'FARMS INC', 'FARMS LLC',
    # Retail / grocery private fleets
    'WALMART', 'COSTCO', 'HOME DEPOT', 'LOWES', 'KROGER',
    'ALBERTSONS', 'PUBLIX', 'SAFEWAY', 'TARGET CORP',
    # Branded private fleets
    'JELD-WEN', 'JELD WEN', 'BORAL', 'ANDERSEN', 'PELLA', 'MARVIN',
    'SYSCO', 'US FOODS', 'KEHE', 'IMPERIAL DADE',
    # Services (not freight)
    'BRINK', 'ARMORED', 'CASH MANAGEMENT',
    'TIRE RECYCL', 'TIRE SERVICE', 'TIRE DISTRIBUTORS',
    'CHAIRS LLC', 'CHAIRS INC', 'FURNITURE',
    # Moving / HHG (distinct from box-truck freight)
    'MOVING', 'STORAGE', 'RELOCATION', 'VAN LINES', 'ALL MY SONS',
    'MAYFLOWER', 'TWO MEN AND A TRUCK',
]

FOR_HIRE_MARKERS = [
    'TRUCKING', 'TRANSPORT', 'LOGISTICS', 'FREIGHT',
    'EXPRESS', 'CARRIERS', 'HAULING', 'DISPATCH', 'DELIVERY',
    'CARGO', 'LINES',
]

KNOWN_TX_PRIVATE_FLEET_DOTS = {
    "629025": "Pavecon LTD",
    "59837": "Austin Bridge & Road",
    "1002101": "Mario Sinacola & Sons Excavating",
    "3155459": "CMC Steel US LLC",
    "443529": "Atlas Investments / Redi-Mix",
    "112604": "Owen Industrial Products",
    "1688316": "MEI Rigging & Crating",
    "76054": "Brink's Incorporated",
}


def _match_tokens(name: str, tokens: list[str]) -> list[str]:
    u = name.upper()
    return [t for t in tokens if t in u]


def classify_row(row_num: int, row: dict) -> list[dict]:
    """Return list of flag dicts for this row."""
    name = (row.get("Company Name") or row.get("Legal Name") or "").strip()
    if not name:
        return []
    equip = (row.get("Equipment_Types") or row.get("Equipment Types") or "").strip().upper()
    dot = (row.get("DOT") or row.get("DOT Number") or row.get("DOT_Number") or "").strip()

    flags = []
    upper = name.upper()

    passenger_hits = _match_tokens(name, PASSENGER_KEYWORDS)
    tow_hits = _match_tokens(name, TOW_KEYWORDS)
    waste_hits = _match_tokens(name, WASTE_KEYWORDS)
    pf_hits = _match_tokens(name, PRIVATE_FLEET_KEYWORDS)
    fh_hits = _match_tokens(name, FOR_HIRE_MARKERS)

    # Known TX slip-through — HIGH confidence
    if dot in KNOWN_TX_PRIVATE_FLEET_DOTS:
        flags.append({
            "severity": "HIGH",
            "flag": "fail_private_fleet",
            "reason": f"known TX private-fleet slip-through ({KNOWN_TX_PRIVATE_FLEET_DOTS[dot]})",
        })

    # Passenger
    if passenger_hits:
        sev = "HIGH" if not fh_hits else "MEDIUM"
        flags.append({
            "severity": sev,
            "flag": "fail_passenger_only",
            "reason": f"passenger token(s): {', '.join(passenger_hits)}" + (
                f"; for-hire markers: {', '.join(fh_hits)}" if fh_hits else ""
            ),
        })

    # Tow / wrecker
    if tow_hits:
        flags.append({
            "severity": "HIGH",
            "flag": "fail_non_freight_service",
            "reason": f"tow/recovery token(s): {', '.join(tow_hits)}",
        })

    # Waste
    if waste_hits:
        flags.append({
            "severity": "MEDIUM",
            "flag": "waste_hauler_review",
            "reason": f"waste token(s): {', '.join(waste_hits)}",
        })

    # Private fleet
    if pf_hits and not any(
        f.get("flag") == "fail_private_fleet" for f in flags
    ):
        if not fh_hits:
            flags.append({
                "severity": "HIGH",
                "flag": "fail_private_fleet",
                "reason": f"private-fleet token(s): {', '.join(pf_hits)}; no for-hire marker",
            })
        else:
            flags.append({
                "severity": "MEDIUM",
                "flag": "private_fleet_review",
                "reason": f"private-fleet token(s): {', '.join(pf_hits)}; for-hire markers: {', '.join(fh_hits)}",
            })

    # Name vs equipment inconsistency (LOW)
    if "FLATBED" in upper and "FLATBED" not in equip:
        flags.append({
            "severity": "LOW",
            "flag": "name_eq_mismatch_flatbed",
            "reason": "name contains FLATBED but Equipment_Types does not",
        })
    reefer_name_tokens = ["REEFER", "FROZEN", "REFRIGERATED", "COLD", "TEMP "]
    if any(t in upper for t in reefer_name_tokens) and "REEFER" not in equip:
        flags.append({
            "severity": "LOW",
            "flag": "name_eq_mismatch_reefer",
            "reason": "name suggests reefer but Equipment_Types lacks REEFER",
        })
    if ("DRY VAN" in upper or "DRYVAN" in upper) and not equip:
        flags.append({
            "severity": "LOW",
            "flag": "name_eq_mismatch_dryvan",
            "reason": "name contains DRY VAN but Equipment_Types empty",
        })
    if ("BOX TRUCK" in upper or "STRAIGHT TRUCK" in upper) and not equip:
        flags.append({
            "severity": "LOW",
            "flag": "name_eq_mismatch_boxtruck",
            "reason": "name contains BOX/STRAIGHT TRUCK but Equipment_Types empty",
        })
    if not fh_hits and not pf_hits and not passenger_hits and not tow_hits and not waste_hits and not equip:
        flags.append({
            "severity": "LOW",
            "flag": "unknown_purpose",
            "reason": "no for-hire/private-fleet keywords and Equipment_Types empty",
        })

    return flags


def fetch_main_tab() -> tuple[list[str], list[list[str]]]:
    rows = read_range(SPREADSHEET_ID, f"'{MAIN_TAB}'!A1:ZZ")
    if not rows:
        raise RuntimeError("No rows returned from main tab")
    header = rows[0]
    data = rows[1:]
    return header, data


def row_to_dict(header: list[str], row: list[str]) -> dict:
    padded = row + [""] * (len(header) - len(row))
    return dict(zip(header, padded))


def _severity_rank(s: str) -> int:
    return {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(s, 3)


def _pick_top_flag(flags: list[dict]) -> dict:
    return sorted(flags, key=lambda f: _severity_rank(f["severity"]))[0]


def _esc(s: str) -> str:
    return (s or "").replace("|", "\\|").replace("\n", " ").strip()


def build_markdown(header: list[str], data: list[list[str]], flagged: list[dict]) -> str:
    total = len(data)
    high = [f for f in flagged if f["top"]["severity"] == "HIGH"]
    med = [f for f in flagged if f["top"]["severity"] == "MEDIUM"]
    low = [f for f in flagged if f["top"]["severity"] == "LOW"]

    lines = []
    lines.append(f"# Carrier Review Queue — 2026-04-15")
    lines.append("")
    lines.append(
        f"Main tab audit: {total} carriers scanned. {len(flagged)} flagged for review."
    )
    lines.append("")

    # HIGH
    lines.append(f"## HIGH Confidence — Recommend Quarantine ({len(high)} carriers)")
    lines.append("")
    lines.append("| Row | DOT | Name | Equipment | City/State | Fleet | Flag | Reason |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for f in high:
        r = f["row_dict"]
        lines.append(
            f"| {f['row_num']} | {_esc(f['dot'])} | {_esc(f['name'])} | "
            f"{_esc(r.get('Equipment_Types', r.get('Equipment Types', '')))} | "
            f"{_esc(r.get('City',''))}, {_esc(r.get('State',''))} | "
            f"{_esc(r.get('Fleet Size', r.get('Fleet_Size','')))} | "
            f"{f['top']['flag']} | {_esc(f['reason_combined'])} |"
        )
    lines.append("")

    # MEDIUM
    lines.append(f"## MEDIUM Confidence — Manual Review Needed ({len(med)} carriers)")
    lines.append("")
    lines.append(
        "| Row | DOT | Name | Equipment | City/State | Fleet | Flag | Reason | Playwright Finding |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for f in med:
        r = f["row_dict"]
        lines.append(
            f"| {f['row_num']} | {_esc(f['dot'])} | {_esc(f['name'])} | "
            f"{_esc(r.get('Equipment_Types', r.get('Equipment Types', '')))} | "
            f"{_esc(r.get('City',''))}, {_esc(r.get('State',''))} | "
            f"{_esc(r.get('Fleet Size', r.get('Fleet_Size','')))} | "
            f"{f['top']['flag']} | {_esc(f['reason_combined'])} | "
            f"{_esc(f.get('playwright_finding',''))} |"
        )
    lines.append("")

    # LOW
    lines.append(f"## LOW Confidence — Inconsistency Notes ({len(low)} carriers)")
    lines.append("")
    lines.append("| Row | DOT | Name | Equipment | Flag | Note |")
    lines.append("|---|---|---|---|---|---|")
    for f in low:
        r = f["row_dict"]
        lines.append(
            f"| {f['row_num']} | {_esc(f['dot'])} | {_esc(f['name'])} | "
            f"{_esc(r.get('Equipment_Types', r.get('Equipment Types', '')))} | "
            f"{f['top']['flag']} | {_esc(f['reason_combined'])} |"
        )
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append(
        f"- **HIGH**: {len(high)} — recommend quarantining all. High-confidence non-freight or private-fleet entries."
    )
    lines.append(
        f"- **MEDIUM**: {len(med)} — Derek should eyeball each. Ambiguous names and Playwright-verified edge cases."
    )
    lines.append(
        f"- **LOW**: {len(low)} — name/equipment cosmetic mismatches; likely fine to leave as-is."
    )
    pct = (len(flagged) / total * 100) if total else 0
    lines.append(f"- **Total flagged**: {len(flagged)} of {total} ({pct:.1f}%)")
    lines.append(f"- **Clean rows (no flags)**: {total - len(flagged)}")
    lines.append("")

    return "\n".join(lines)


def audit(verify_cap: int = 20) -> dict:
    header, data = fetch_main_tab()
    flagged = []
    for idx, row in enumerate(data):
        row_num = idx + 2  # 1-indexed + header
        rd = row_to_dict(header, row)
        flags = classify_row(row_num, rd)
        if not flags:
            continue
        top = _pick_top_flag(flags)
        reason_combined = "; ".join(f"[{f['severity']}] {f['reason']}" for f in flags)
        dot = (rd.get("DOT") or rd.get("DOT Number") or rd.get("DOT_Number") or "").strip()

        # Apply Playwright overrides
        playwright_finding = ""
        if dot in PLAYWRIGHT_VERIFICATIONS:
            pv = PLAYWRIGHT_VERIFICATIONS[dot]
            old_sev = top["severity"]
            old_flag = top["flag"]
            top = {"severity": pv["new_severity"], "flag": pv["new_flag"], "reason": pv["new_reason"]}
            reason_combined += f"; [PLAYWRIGHT override {old_sev}->{pv['new_severity']}] {pv['new_reason']}"
            playwright_finding = pv["finding"]

        flagged.append({
            "row_num": row_num,
            "dot": dot,
            "name": (rd.get("Company Name") or rd.get("Legal Name") or "").strip(),
            "row_dict": rd,
            "top": top,
            "all_flags": flags,
            "reason_combined": reason_combined,
            "playwright_finding": playwright_finding,
        })

    # Markdown
    md = build_markdown(header, data, flagged)

    # Footer
    high = [f for f in flagged if f["top"]["severity"] == "HIGH"]
    med = [f for f in flagged if f["top"]["severity"] == "MEDIUM"]
    low = [f for f in flagged if f["top"]["severity"] == "LOW"]
    footer = []
    footer.append("")
    footer.append("---")
    footer.append("")
    footer.append("## Audit Footer")
    footer.append("")
    footer.append(f"- Playwright verifications run: {len(PLAYWRIGHT_VERIFICATIONS)} (target: all 4 MEDIUM cases; well under 20 cap)")
    footer.append("- Playwright findings:")
    for dot, pv in PLAYWRIGHT_VERIFICATIONS.items():
        footer.append(f"  - DOT {dot} ({pv['name']}): {pv['finding']} -> {pv['new_severity']} {pv['new_flag']}")
    footer.append(f"- Columns detected in header: {len(header)}")
    footer.append(f"- Header: {', '.join(header[:15])}")
    footer.append("")
    footer.append("### Suggestions for tighter L&I sourcing filter")
    footer.append("- Extend private-fleet keyword list with: `FUELING`, `FUELS LLC`, `FUELS INC`, `OIL COMPANY`, `OIL & PROPANE`, `FUELS AND ICE` (hit multiple FL fuel distributors)")
    footer.append("- Add `CONTRACTING INC`, `CONTRACTING LLC` (matched separately from `CONTRACTING` but worth adding for explicit suffix match)")
    footer.append("- Add `EXCAVATING LLC`, `EXCAVATING INC` (Fitzgerald example — had for-hire marker but Playwright confirmed private fleet)")
    footer.append("- Add `HEAVY HAUL & RIGGING`, `HEAVY HAULING & RIGGING` as a compound token")
    footer.append("- Add `CHAIRS` as a standalone token (Concert Chairs LLC hit via suffix but name-contains match is safer)")
    footer.append("- Consider a Playwright confirmation pass on all PRIVATE(PROPERTY) carriers before import, not just Name heuristics")
    md += "\n".join(footer)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(md, encoding="utf-8")

    return {
        "total": len(data),
        "flagged": len(flagged),
        "high": len(high),
        "medium": len(med),
        "low": len(low),
        "flagged_list": flagged,
        "header": header,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", type=int, default=0, help="Max Playwright verifications (0 = skip)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = audit(verify_cap=args.verify)

    # Category counts
    cat_counts = {}
    for f in result["flagged_list"]:
        cat = f["top"]["flag"]
        sev = f["top"]["severity"]
        cat_counts.setdefault(sev, {}).setdefault(cat, 0)
        cat_counts[sev][cat] += 1

    print(f"\n=== AUDIT SUMMARY ===")
    print(f"Total rows: {result['total']}")
    print(f"Flagged: {result['flagged']}")
    print(f"  HIGH:   {result['high']}")
    print(f"  MEDIUM: {result['medium']}")
    print(f"  LOW:    {result['low']}")
    print(f"\nCategory counts by severity:")
    print(json.dumps(cat_counts, indent=2))

    print(f"\nTop HIGH flags (up to 15):")
    high_flags = [f for f in result["flagged_list"] if f["top"]["severity"] == "HIGH"]
    for f in high_flags[:15]:
        print(f"  row {f['row_num']} DOT {f['dot']} — {f['name']} — {f['top']['flag']}: {f['top']['reason'][:120]}")

    print(f"\nMEDIUM flags (up to 30):")
    med_flags = [f for f in result["flagged_list"] if f["top"]["severity"] == "MEDIUM"]
    for f in med_flags[:30]:
        print(f"  row {f['row_num']} DOT {f['dot']} — {f['name']} — {f['top']['flag']}: {f['top']['reason'][:100]}")

    print(f"\nHeader columns ({len(result['header'])}): {result['header']}")
    print(f"\nOutput: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
