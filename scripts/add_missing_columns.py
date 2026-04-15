"""
Stream 4 — Schema drift migration (DRY-RUN BY DEFAULT).

Appends missing columns to the Carrier_Master 'Carrier Database' tab.
Six fields are read by workflow code but have no corresponding column:
  - W9_On_File
  - Claims_Count
  - Last_Load_Date
  - Preferred_Lanes
  - Outreach_Method
  - OOS_Active

Columns are appended to the RIGHT of existing columns. Mid-sheet insertion
is avoided because a grep of the codebase for `values[N]`-style index-based
readers came up empty, but appending is still the safer path.

Usage:
  python scripts/add_missing_columns.py           # dry-run
  python scripts/add_missing_columns.py --apply   # actually modify sheet

Also prints the proposed updates to _FIELD_MAP and _READ_ALIAS_MAP in
app/sheets.py. Those edits must be applied by a human after the columns
land in the sheet — this script will not touch source code.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.config import get_settings  # noqa: E402
from app.google_auth import get_sheets_service  # noqa: E402
from app.sheets import CARRIER_MASTER_COLUMNS, CARRIER_DB_TAB  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("add_missing_columns")


# Mapping of internal-field-name → human-readable sheet header.
# Ordering here determines left-to-right append order.
PROPOSED_COLUMNS: list[tuple[str, str]] = [
    ("W9_On_File",      "W9 On File"),
    ("Claims_Count",    "Claims Count"),
    ("Last_Load_Date",  "Last Load Date"),
    ("Preferred_Lanes", "Preferred Lanes"),
    ("Outreach_Method", "Outreach Method"),
    ("OOS_Active",      "OOS Active"),
]


def _fetch_current_headers(sheet_id: str) -> list[str]:
    svc = get_sheets_service().spreadsheets()
    resp = svc.values().get(
        spreadsheetId=sheet_id,
        range=f"{CARRIER_DB_TAB}!1:1",
    ).execute()
    values = resp.get("values", [])
    return values[0] if values else []


def _next_column_letter(n_existing: int) -> str:
    """Return the A1 column letter immediately after `n_existing` columns.
    Handles up to ZZ (728 cols)."""
    # n_existing = 0 → A, 1 → B, 26 → AA, ...
    result = ""
    n = n_existing
    while True:
        result = chr(ord("A") + (n % 26)) + result
        n = n // 26 - 1
        if n < 0:
            break
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Actually write changes")
    args = parser.parse_args()

    settings = get_settings()
    sheet_id = settings.CARRIER_MASTER_SHEET_ID

    logger.info("Fetching current headers from %s / %s", sheet_id, CARRIER_DB_TAB)
    current = _fetch_current_headers(sheet_id)
    logger.info("Current header count: %d", len(current))
    logger.info("Current headers: %s", current)

    # Figure out which proposed columns are already present
    existing_set = {h.strip() for h in current}
    to_add: list[tuple[str, str]] = []
    skipped: list[tuple[str, str]] = []
    for internal, header in PROPOSED_COLUMNS:
        if header in existing_set:
            skipped.append((internal, header))
        else:
            to_add.append((internal, header))

    logger.info("Columns to add: %d", len(to_add))
    for i, (internal, header) in enumerate(to_add, start=1):
        col_idx = len(current) + i - 1  # 0-based after existing
        col_letter = _next_column_letter(col_idx)
        logger.info("  %s -> col %s  (internal field: %s)", header, col_letter, internal)
    if skipped:
        logger.info("Columns already present (skipped): %s", [h for _, h in skipped])

    # Print the expected source-code edits (for a human to apply)
    print("\n=== Proposed edits to app/sheets.py (manual follow-up after --apply) ===")
    print("\n# Append these entries to _FIELD_MAP (write path):")
    for internal, header in to_add:
        print(f'    "{internal}": "{header}",')

    print("\n# Append these entries to _READ_ALIAS_MAP (read path):")
    for internal, header in to_add:
        print(f'    "{header}": "{internal}",')

    print("\n# Append these entries to CARRIER_MASTER_COLUMNS (for insert_carrier row shape):")
    for internal, header in to_add:
        print(f'    "{header}",')

    if not to_add:
        logger.info("Nothing to do — all proposed columns already present.")
        return 0

    # Build the row of new headers to append
    new_header_row = [header for _, header in to_add]
    start_col = _next_column_letter(len(current))
    end_col = _next_column_letter(len(current) + len(to_add) - 1)
    target_range = f"{CARRIER_DB_TAB}!{start_col}1:{end_col}1"

    if not args.apply:
        logger.info("DRY RUN — would write %s to %s", new_header_row, target_range)
        logger.info("Re-run with --apply to execute.")
        return 0

    logger.info("APPLYING — writing headers to %s", target_range)
    svc = get_sheets_service().spreadsheets()

    # 1. Expand grid if needed (count columns)
    meta = svc.get(spreadsheetId=sheet_id).execute()
    tab_name = CARRIER_DB_TAB.strip("'")
    for sheet in meta.get("sheets", []):
        if sheet["properties"]["title"] == tab_name:
            current_cols = sheet["properties"]["gridProperties"].get("columnCount", 26)
            needed_cols = len(current) + len(to_add)
            if needed_cols > current_cols:
                add_cols = needed_cols - current_cols
                logger.info("Expanding '%s' grid by %d columns", tab_name, add_cols)
                svc.batchUpdate(
                    spreadsheetId=sheet_id,
                    body={"requests": [{
                        "appendDimension": {
                            "sheetId": sheet["properties"]["sheetId"],
                            "dimension": "COLUMNS",
                            "length": add_cols,
                        }
                    }]},
                ).execute()
            break

    # 2. Write headers
    svc.values().update(
        spreadsheetId=sheet_id,
        range=target_range,
        valueInputOption="USER_ENTERED",
        body={"values": [new_header_row]},
    ).execute()
    logger.info("Wrote %d headers to %s", len(new_header_row), target_range)
    logger.info("DONE. Now update app/sheets.py per the proposed edits above.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
