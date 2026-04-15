"""
BrokerOps AI — Add missing outreach-tracking columns to Carrier Database.

Idempotent: reads the live header row first, appends only columns that are
actually missing. Run manually via bash; do not import from app/.

Usage:
    python scripts/add_missing_outreach_columns.py

Logs output to scripts/logs/approval_flow_hardening_20260415.log
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# ── Bootstrap ─────────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

_LOG_FILE = _REPO_ROOT / "scripts" / "logs" / "approval_flow_hardening_20260415.log"
_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(_LOG_FILE)),
    ],
)
logger = logging.getLogger("add_missing_outreach_columns")

# Load .env so we can get CARRIER_MASTER_SHEET_ID
try:
    from dotenv import load_dotenv
    load_dotenv(_REPO_ROOT / ".env")
except ImportError:
    pass

SHEET_ID = os.environ.get("CARRIER_MASTER_SHEET_ID", "")
if not SHEET_ID:
    logger.error("CARRIER_MASTER_SHEET_ID not set in environment")
    sys.exit(1)

CARRIER_DB_TAB = "'Carrier Database'"

# The full set of extra columns we expect in order after CARRIER_MASTER_COLUMNS.
# The code in app/sheets.py expects exactly these names in the header row.
EXPECTED_EXTRA_COLUMNS = [
    "Classification",
    "Vetting Status",
    "Service Type",
    "Website",
    "Outreach_Status",
    "Outreach_E1_SentAt",
    "Outreach_E2_SentAt",
    "Outreach_E3_SentAt",
    "Outreach_Thread_Id",
    "Onboarding_Status",
    "Onboarding_Docs_Received",
    "Outreach_OOO_Return_Date",
    "Onboarding_E4_ScheduledFor",
    "Onboarding_E4_SentAt",
]


def main() -> None:
    from app.sheets import read_range, write_range

    logger.info("Reading live header row from sheet %s ...", SHEET_ID)
    rows = read_range(SHEET_ID, f"{CARRIER_DB_TAB}!A1:ZZ1")
    if not rows:
        logger.error("Could not read header row — empty response")
        sys.exit(1)

    live_headers: list[str] = rows[0]
    logger.info("Live header row has %d columns", len(live_headers))
    logger.info("Headers: %s", live_headers)

    # Identify drift
    present = set(live_headers)
    missing = [col for col in EXPECTED_EXTRA_COLUMNS if col not in present]
    extra_in_sheet = [h for h in live_headers if h not in set(
        # Known base columns up through AI
        ["Carrier ID", "Status", "Company Name", "MC Number", "DOT Number",
         "Contact Name", "Contact Email", "Contact Phone",
         "Dispatcher Name", "Dispatcher Email", "Dispatcher Phone",
         "Address", "City", "State", "ZIP",
         "Equipment Types", "Fleet Size",
         "Insurance Liability", "Insurance Cargo", "Insurance Expiry",
         "Authority Status", "Authority Date", "Safety Rating",
         "Has GPS", "GPS Provider",
         "Compliance Status", "Last Compliance Check",
         "Score", "Outreach Status", "Onboarded Date", "Notes",
         "Classification", "Vetting Status", "Service Type", "Website",
        ] + EXPECTED_EXTRA_COLUMNS
    )]

    logger.info("=== SCHEMA DRIFT AUDIT ===")
    for col in EXPECTED_EXTRA_COLUMNS:
        status = "PRESENT" if col in present else "MISSING"
        logger.info("  %-35s  %s", col, status)

    if extra_in_sheet:
        logger.info("Extra columns in sheet not in expected list: %s", extra_in_sheet)

    if not missing:
        logger.info("No missing columns — sheet schema is fully aligned. Nothing to do.")
        return

    logger.info("Missing columns to append: %s", missing)

    # Expand the sheet grid if needed so we can write beyond current max columns
    total_needed = len(live_headers) + len(missing)
    logger.info("Expanding sheet grid to %d columns (current max unknown; requesting %d)", total_needed, total_needed)
    _expand_sheet_columns(SHEET_ID, total_needed + 5)  # +5 buffer

    # Find next empty column — append after last populated header
    next_col_idx = len(live_headers)  # 0-based index of first empty col
    appended_cells = []
    for i, col_name in enumerate(missing):
        col_letter = _col_index_to_letter(next_col_idx + i)
        cell = f"{CARRIER_DB_TAB}!{col_letter}1"
        logger.info("Writing '%s' to cell %s", col_name, cell)
        write_range(SHEET_ID, cell, [[col_name]])
        appended_cells.append((col_letter, col_name))

    logger.info("Appended %d missing columns: %s", len(appended_cells), appended_cells)

    # Verify
    verify_rows = read_range(SHEET_ID, f"{CARRIER_DB_TAB}!A1:ZZ1")
    if verify_rows:
        new_headers = verify_rows[0]
        logger.info("Post-fix header count: %d", len(new_headers))
        still_missing = [col for col in EXPECTED_EXTRA_COLUMNS if col not in new_headers]
        if still_missing:
            logger.error("Still missing after fix: %s", still_missing)
            sys.exit(1)
        else:
            logger.info("Verification passed — all expected columns now present.")
    else:
        logger.warning("Could not re-read header row for verification")


def _expand_sheet_columns(sheet_id: str, min_columns: int) -> None:
    """Expand the first sheet tab to at least min_columns columns via batchUpdate."""
    import google.auth
    import googleapiclient.discovery

    # Get creds the same way app/ does
    from app.google_auth import get_sheets_service
    svc = get_sheets_service()

    # Get current sheet metadata to find sheet_id (gid) and current column count
    meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheets = meta.get("sheets", [])
    target_sheet = None
    for s in sheets:
        if "Carrier Database" in s["properties"]["title"]:
            target_sheet = s
            break
    if target_sheet is None:
        target_sheet = sheets[0]  # fallback to first sheet

    gid = target_sheet["properties"]["sheetId"]
    current_cols = target_sheet["properties"]["gridProperties"].get("columnCount", 26)
    logger.info("Sheet gid=%d current_cols=%d min_needed=%d", gid, current_cols, min_columns)

    if current_cols >= min_columns:
        logger.info("Grid already large enough (%d >= %d)", current_cols, min_columns)
        return

    body = {
        "requests": [{
            "updateSheetProperties": {
                "properties": {
                    "sheetId": gid,
                    "gridProperties": {"columnCount": min_columns},
                },
                "fields": "gridProperties.columnCount",
            }
        }]
    }
    svc.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=body).execute()
    logger.info("Expanded grid to %d columns", min_columns)


def _col_index_to_letter(idx: int) -> str:
    """Convert 0-based column index to spreadsheet letter (A, B, ..., Z, AA, ...)."""
    result = ""
    n = idx + 1
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


if __name__ == "__main__":
    main()
