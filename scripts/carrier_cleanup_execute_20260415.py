"""
BrokerOps AI — Carrier Cleanup Execution, 2026-04-15
=====================================================
Authorized changes per Sasha/Derek cleanup plan.

Tasks:
  1. Add "Service Type" column to main Carrier Database tab (default: General)
  2. Apply category tags (Heavy Haul, Auto Transport, Fuel) to specific DOTs
  3. Move 44 carriers to Carrier Quarantine with reason non_target_service_type:<subreason>
     (does NOT delete rows from main tab — reversible)

Idempotent: safe to re-run.
Run from project root:
    PYTHONPATH=. python scripts/carrier_cleanup_execute_20260415.py [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import io
from datetime import datetime, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from app.config import get_settings
from app.google_auth import get_sheets_service

LOGS_DIR = Path("C:/Users/Owner/brokerops-ai/scripts/logs")
LOG_OUT  = LOGS_DIR / "carrier_cleanup_execution_20260415.log"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_OUT), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("carrier_cleanup_execute")

# ── Constants ─────────────────────────────────────────────────────────────────

MAIN_TAB     = "Carrier Database"
QUARANTINE_TAB = "Carrier Quarantine"
QUARANTINE_REASON_COL = "Quarantine Reason"

# Spreadsheet ID from instructions
SHEET_ID = "1B5nzDCisdpG29bI0MjLtD8dIE2rsjdXg3Qncj-E58WE"
QUARANTINE_SHEET_ID_NUM = 1204167765  # per instructions

SERVICE_TYPE_HEADER = "Service Type"
SERVICE_TYPE_DEFAULT = "General"

# ── Service Type tag assignments ──────────────────────────────────────────────
# DOTs that need non-General service type tags
# Heavy Haul: 4 heavy_haul_rigging flagged in audit (including Tampa rescue) + Tampa itself
HEAVY_HAUL_DOTS = {
    "167645",   # Anderson Heavy Haul & Rigging
    "2558618",  # Extreme Steel Crane
    "1688316",  # MEI Rigging & Crating LLC
    "2921580",  # Tampa Heavy Haul Transport (rescued)
}

AUTO_TRANSPORT_DOTS = {
    "2209779",  # Yes Auto Transport INC
    "2827927",  # SDK Auto Transport INC
    "3040478",  # Lap Auto Transport INC
}

FUEL_DOTS = {
    "260315",   # W H Thomas Oil Co INC
    "354670",   # Palmetto Propane Fuels and Ice INC
    "1047888",  # Barnes Oil & Propane INC
    "2379580",  # Colonial Fuel and Lubricant Services
}

# ── Quarantine assignments ────────────────────────────────────────────────────
# DOT → quarantine subreason
QUARANTINE_MAP: dict[str, str] = {
    # Towing / wrecker / recovery (21)
    "3306087": "non_target_service_type:towing",   # Action Towing LLC
    "3186652": "non_target_service_type:towing",   # Virginia Towing Services LLC
    "3335283": "non_target_service_type:towing",   # Virginia Fast Towing LLC
    "801906":  "non_target_service_type:towing",   # Steele's Towing & Equipment LLC
    "3125719": "non_target_service_type:towing",   # Baker Bros Towing
    "3024177": "non_target_service_type:towing",   # Tillman Towing LLC
    "3320196": "non_target_service_type:towing",   # Total Towing
    "1972717": "non_target_service_type:towing",   # Joe's Tow INC
    "3858400": "non_target_service_type:towing",   # King Towing and Recovery LLC
    "610934":  "non_target_service_type:towing",   # William Fulp Wrecker Service INC
    "1001180": "non_target_service_type:towing",   # Kimble's Towing LLC
    "2100804": "non_target_service_type:towing",   # Yarbrough Wrecker Service
    "2186875": "non_target_service_type:towing",   # Jay's Auto Repair and Towing LLC
    "841319":  "non_target_service_type:towing",   # Chancey's Wrecker Service INC
    "1145975": "non_target_service_type:towing",   # Terry's Towing INC
    "2906088": "non_target_service_type:towing",   # Smith's Towing LLC
    "2953511": "non_target_service_type:towing",   # Perfect Choice Towing & Recovery LLC
    "2570406": "non_target_service_type:towing",   # Lucky 13 Towing and Recovery LLC
    "1578182": "non_target_service_type:towing",   # Mims Wrecker Service
    "1662319": "non_target_service_type:towing",   # Wheeler Wrecker Service INC
    "1316597": "non_target_service_type:towing",   # Miracle Towing & Recovery

    # Passenger bus / tours / charter / transit (10)
    "3529027": "non_target_service_type:passenger",  # Jbd Transit LLC
    "3678704": "non_target_service_type:passenger",  # Halo Transit LLC
    "2142253": "non_target_service_type:passenger",  # Big Bus Tours Miami
    "3005176": "non_target_service_type:passenger",  # Rice Tours LLC
    "2379403": "non_target_service_type:passenger",  # Garvin Tours
    "1622357": "non_target_service_type:passenger",  # New Generation Charter & Tours INC
    "1005365": "non_target_service_type:passenger",  # Camelot Bus Charters Tours
    "1281776": "non_target_service_type:passenger",  # Old Town Trolley Tours
    "1777594": "non_target_service_type:passenger",  # Travel by Bus LLC
    "30144":   "non_target_service_type:passenger",  # Bulk Transit Corporation

    # Moving / HHG (4 confirmed from audit; 5th expected in sheet but not in audit JSON)
    "292830":  "non_target_service_type:hhg_moving", # Danny Branham Mobile Home Movers
    "923537":  "non_target_service_type:hhg_moving", # All My Sons Moving & Storage Of Dallas LLC
    "2453189": "non_target_service_type:hhg_moving", # Daryl Flood Moving & Storage
    "3530356": "non_target_service_type:hhg_moving", # College Hunks Hauling Junk & Moving

    # Excavating / private fleet (2 removes from audit: Smith + Fitzgerald)
    "2508773": "non_target_service_type:excavating_private_fleet",  # Smith Excavating LLC
    "1026131": "non_target_service_type:excavating_private_fleet",  # Fitzgerald Excavating & Trucking INC

    # Waste / disposal (2)
    "3355445": "non_target_service_type:waste",  # Universal Waste Management LLC
    "1071704": "non_target_service_type:waste",  # Industrial Waste Service INC

    # Logging / timber (2)
    "3325439": "non_target_service_type:logging",  # G and H Land and Timber Investments LLC
    "2779218": "non_target_service_type:logging",  # Haddock Timber INC

    # Vehicle recovery (standalone — not already counted in towing)
    "1726329": "non_target_service_type:vehicle_recovery",  # Extreme Recovery LLC

    # NOTE: Heavy Haul carriers (167645, 2558618, 1688316) are marked "remove" in the audit
    # but per Derek's cleanup plan they STAY in main tab with Heavy Haul service type.
    # Auto Transport carriers (2209779, 2827927, 3040478) similarly stay with Auto Transport tag.
    # Stewart's Grading (2393652) rescued → stays as General.
    # Tampa Heavy Haul (2921580) rescued → stays as Heavy Haul.
}

# DOTs that should be SKIPPED for quarantine regardless of audit action
# (rescued carriers that stay in main tab)
RESCUE_DOTS = {
    "2393652",  # Stewart's Grading & Hauling — rescued as General
    "2921580",  # Tampa Heavy Haul Transport — rescued as Heavy Haul
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_sheet_data(svc, spreadsheet_id: str, tab: str, end_col: str = "AZ") -> tuple[list[str], list[list[str]]]:
    """Read a tab and return (header, data_rows)."""
    resp = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab}'!A1:{end_col}",
    ).execute()
    rows = resp.get("values", [])
    if not rows:
        return [], []
    header = rows[0]
    data = rows[1:]
    return header, data


def col_letter(n: int) -> str:
    """Convert 0-indexed column number to spreadsheet letter (A, B, ..., Z, AA, ...)."""
    result = ""
    n += 1  # 1-indexed
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


# ── Task 1: Add Service Type column ──────────────────────────────────────────

def add_service_type_column(svc, spreadsheet_id: str, dry_run: bool = False) -> int:
    """Add Service Type column to Carrier Database tab. Returns column index (0-based)."""
    header, data = get_sheet_data(svc, spreadsheet_id, MAIN_TAB)

    if SERVICE_TYPE_HEADER in header:
        col_idx = header.index(SERVICE_TYPE_HEADER)
        logger.info("Service Type column already exists at col %s (index %d) — skipping add",
                    col_letter(col_idx), col_idx)
        return col_idx

    # Append to right
    col_idx = len(header)
    col_ltr = col_letter(col_idx)
    num_data_rows = len(data)

    logger.info("Adding Service Type column at col %s (index %d), %d data rows",
                col_ltr, col_idx, num_data_rows)

    if dry_run:
        logger.info("[DRY-RUN] Would write header '%s' at %s!%s1", SERVICE_TYPE_HEADER, MAIN_TAB, col_ltr)
        logger.info("[DRY-RUN] Would write %d 'General' default values", num_data_rows)
        return col_idx

    # Write header
    svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{MAIN_TAB}'!{col_ltr}1",
        valueInputOption="USER_ENTERED",
        body={"values": [[SERVICE_TYPE_HEADER]]},
    ).execute()
    logger.info("Wrote header '%s' at %s!%s1", SERVICE_TYPE_HEADER, MAIN_TAB, col_ltr)

    # Write defaults for existing rows
    if num_data_rows > 0:
        defaults = [[SERVICE_TYPE_DEFAULT]] * num_data_rows
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{MAIN_TAB}'!{col_ltr}2:{col_ltr}{num_data_rows + 1}",
            valueInputOption="USER_ENTERED",
            body={"values": defaults},
        ).execute()
        logger.info("Wrote %d default 'General' values to %s!%s2:%s%d",
                    num_data_rows, MAIN_TAB, col_ltr, col_ltr, num_data_rows + 1)

    return col_idx


# ── Task 2: Apply category tags ───────────────────────────────────────────────

def apply_service_type_tags(
    svc, spreadsheet_id: str, service_type_col_idx: int, dry_run: bool = False
) -> dict[str, int]:
    """Apply Heavy Haul, Auto Transport, Fuel tags to specific DOTs."""
    header, data = get_sheet_data(svc, spreadsheet_id, MAIN_TAB)

    # Find DOT column
    dot_col = None
    for possible in ["DOT Number", "DOT_Number", "DOT"]:
        if possible in header:
            dot_col = header.index(possible)
            break
    if dot_col is None:
        logger.error("Could not find DOT column in header: %s", header[:10])
        return {}

    col_ltr = col_letter(service_type_col_idx)

    # Build DOT → service type map
    all_tag_dots: dict[str, str] = {}
    for dot in HEAVY_HAUL_DOTS:
        all_tag_dots[dot] = "Heavy Haul"
    for dot in AUTO_TRANSPORT_DOTS:
        all_tag_dots[dot] = "Auto Transport"
    for dot in FUEL_DOTS:
        all_tag_dots[dot] = "Fuel"

    applied: dict[str, int] = {"Heavy Haul": 0, "Auto Transport": 0, "Fuel": 0, "not_found": 0}
    updates: list[tuple[int, str]] = []  # (1-indexed row, service_type)

    for row_idx, row in enumerate(data, start=2):
        if dot_col >= len(row):
            continue
        dot = str(row[dot_col]).strip()
        if dot in all_tag_dots:
            tag = all_tag_dots[dot]
            updates.append((row_idx, tag))

    found_dots = set()
    for row_idx, tag in updates:
        row = data[row_idx - 2]
        dot = str(row[dot_col]).strip() if dot_col < len(row) else "?"
        found_dots.add(dot)
        logger.info("Tag %s → row %d (DOT %s) as '%s'", col_ltr, row_idx, dot, tag)
        if not dry_run:
            svc.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{MAIN_TAB}'!{col_ltr}{row_idx}",
                valueInputOption="USER_ENTERED",
                body={"values": [[tag]]},
            ).execute()
        service_type_counts = {"Heavy Haul": 0, "Auto Transport": 0, "Fuel": 0}
        for d, t in all_tag_dots.items():
            if d in found_dots:
                service_type_counts[t] = service_type_counts.get(t, 0) + 1
        applied = {
            "Heavy Haul": sum(1 for d, t in all_tag_dots.items() if t == "Heavy Haul" and d in found_dots),
            "Auto Transport": sum(1 for d, t in all_tag_dots.items() if t == "Auto Transport" and d in found_dots),
            "Fuel": sum(1 for d, t in all_tag_dots.items() if t == "Fuel" and d in found_dots),
        }

    missing_dots = set(all_tag_dots.keys()) - found_dots
    if missing_dots:
        logger.warning("DOTs in tag list but NOT FOUND in main tab: %s", sorted(missing_dots))
        applied["not_found"] = len(missing_dots)

    logger.info("Tags applied: Heavy Haul=%d, Auto Transport=%d, Fuel=%d, not_found=%d",
                applied.get("Heavy Haul", 0), applied.get("Auto Transport", 0),
                applied.get("Fuel", 0), applied.get("not_found", 0))
    return applied


# ── Task 3: Quarantine moves ──────────────────────────────────────────────────

def move_to_quarantine(svc, spreadsheet_id: str, dry_run: bool = False) -> dict[str, int]:
    """Copy target rows to Quarantine tab with reason, leaving main tab intact."""
    header, data = get_sheet_data(svc, spreadsheet_id, MAIN_TAB)

    # Find DOT column
    dot_col = None
    for possible in ["DOT Number", "DOT_Number", "DOT"]:
        if possible in header:
            dot_col = header.index(possible)
            break
    if dot_col is None:
        logger.error("Could not find DOT column in Carrier Database header")
        return {}

    # Read existing quarantine to avoid duplicates
    q_resp = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{QUARANTINE_TAB}'!A1:B",
    ).execute()
    q_rows = q_resp.get("values", [])
    q_header = q_rows[0] if q_rows else []
    q_dot_col = None
    for possible in ["DOT Number", "DOT_Number", "DOT"]:
        if possible in q_header:
            q_dot_col = q_header.index(possible)
            break
    existing_q_dots: set[str] = set()
    if q_dot_col is not None:
        for r in q_rows[1:]:
            if q_dot_col < len(r):
                existing_q_dots.add(str(r[q_dot_col]).strip())

    logger.info("Quarantine tab has %d existing DOTs", len(existing_q_dots))

    # Read full quarantine header to know column layout
    q_full_resp = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{QUARANTINE_TAB}'!A1:AK1",
    ).execute()
    q_full_header = q_full_resp.get("values", [[]])[0]
    if not q_full_header:
        # Quarantine tab may not have extra cols set up yet — use main header + extras
        q_full_header = list(header) + ["Quarantine Reason", "Quarantined At", "Original Row Number", "Last Re-checked"]
        logger.info("Quarantine header not yet set up — will use main+extras schema")

    stats = {"moved": 0, "already_quarantined": 0, "not_found": 0, "skipped_rescue": 0}
    found_dots: set[str] = set()

    for row_idx, row in enumerate(data, start=2):
        if dot_col >= len(row):
            continue
        dot = str(row[dot_col]).strip()

        if dot not in QUARANTINE_MAP:
            continue

        found_dots.add(dot)
        reason = QUARANTINE_MAP[dot]

        if dot in RESCUE_DOTS:
            logger.info("  [SKIP] DOT %s is in rescue list — NOT quarantining", dot)
            stats["skipped_rescue"] += 1
            continue

        if dot in existing_q_dots:
            logger.info("  [=] DOT %s already in quarantine — updating reason to: %s", dot, reason)
            if not dry_run:
                # Update reason in-place (find the row)
                q_search = svc.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{QUARANTINE_TAB}'!A:AK",
                ).execute()
                q_all_rows = q_search.get("values", [])
                q_h = q_all_rows[0] if q_all_rows else []
                q_reason_col = q_h.index("Quarantine Reason") if "Quarantine Reason" in q_h else None
                q_dot_c = None
                for possible in ["DOT Number", "DOT_Number", "DOT"]:
                    if possible in q_h:
                        q_dot_c = q_h.index(possible)
                        break
                if q_reason_col is not None and q_dot_c is not None:
                    for qi, qr in enumerate(q_all_rows[1:], start=2):
                        if q_dot_c < len(qr) and str(qr[q_dot_c]).strip() == dot:
                            col_r_ltr = col_letter(q_reason_col)
                            svc.spreadsheets().values().update(
                                spreadsheetId=spreadsheet_id,
                                range=f"'{QUARANTINE_TAB}'!{col_r_ltr}{qi}",
                                valueInputOption="USER_ENTERED",
                                body={"values": [[reason]]},
                            ).execute()
                            break
            stats["already_quarantined"] += 1
            continue

        # Build quarantine row payload aligned to q_full_header
        row_dict = {}
        for col_i, col_name in enumerate(header):
            val = row[col_i] if col_i < len(row) else ""
            row_dict[col_name] = val
            # Also store underscore alias
            row_dict[col_name.replace(" ", "_")] = val

        payload: list[str] = []
        for col_name in q_full_header:
            if col_name == "Quarantine Reason":
                payload.append(reason)
            elif col_name == "Quarantined At":
                payload.append(_now_iso())
            elif col_name == "Original Row Number":
                payload.append(str(row_idx))
            elif col_name == "Last Re-checked":
                payload.append(_now_iso())
            else:
                val = row_dict.get(col_name, row_dict.get(col_name.replace(" ", "_"), ""))
                payload.append("" if val is None else str(val))

        if dry_run:
            logger.info("  [DRY-RUN] Would quarantine DOT %s (%s) reason=%s",
                        dot,
                        row_dict.get("Company Name", row_dict.get("Legal_Name", "?")),
                        reason)
        else:
            svc.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"'{QUARANTINE_TAB}'!A:AK",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": [payload]},
            ).execute()
            logger.info("  [+] Quarantined DOT %s reason=%s (from main tab row %d)",
                        dot, reason, row_idx)

        stats["moved"] += 1

    missing = set(QUARANTINE_MAP.keys()) - found_dots
    if missing:
        logger.warning("DOTs in quarantine map but NOT FOUND in main tab: %s", sorted(missing))
        stats["not_found"] = len(missing)

    logger.info("Quarantine summary: moved=%d already_quarantined=%d not_found=%d",
                stats["moved"], stats["already_quarantined"], stats["not_found"])
    return stats


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="BrokerOps carrier cleanup execution")
    parser.add_argument("--dry-run", action="store_true", help="Log only, no sheet writes")
    args = parser.parse_args()

    dry_run = args.dry_run
    if dry_run:
        logger.info("=== DRY-RUN MODE — no sheet writes will occur ===")

    settings = get_settings()
    sheet_id = SHEET_ID  # from instructions; overrides env var

    logger.info("Starting carrier cleanup execution — sheet: %s", sheet_id)
    logger.info("Timestamp: %s", _now_iso())

    svc = get_sheets_service()

    # Task 1: Add Service Type column
    logger.info("=== TASK 1: Add Service Type column ===")
    service_type_col_idx = add_service_type_column(svc, sheet_id, dry_run=dry_run)

    # Task 2: Apply category tags
    logger.info("=== TASK 2: Apply category tags ===")
    tag_results = apply_service_type_tags(svc, sheet_id, service_type_col_idx, dry_run=dry_run)
    logger.info("Tag results: %s", tag_results)

    # Task 3: Move to quarantine
    logger.info("=== TASK 3: Move carriers to Quarantine ===")
    q_stats = move_to_quarantine(svc, sheet_id, dry_run=dry_run)

    # Summary
    logger.info("")
    logger.info("=== EXECUTION SUMMARY ===")
    logger.info("Task 1 — Service Type column: added at col index %d, default=General", service_type_col_idx)
    logger.info("Task 2 — Tags applied: %s", tag_results)
    logger.info("Task 3 — Quarantine: moved=%d already_in_q=%d not_found_in_sheet=%d",
                q_stats.get("moved", 0), q_stats.get("already_quarantined", 0), q_stats.get("not_found", 0))

    expected_quarantine = len(QUARANTINE_MAP)
    logger.info("         Expected to quarantine: %d DOTs (from map)", expected_quarantine)
    logger.info("         Note: 5th HHG row (moving company not in audit JSON) may add 1 more if found in sheet")
    logger.info("         Note: Heavy Haul carriers (167645, 2558618, 1688316) STAY in main tab with Heavy Haul tag per plan")


if __name__ == "__main__":
    main()
