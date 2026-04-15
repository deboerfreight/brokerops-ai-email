"""
BrokerOps AI — Main Tab Dedupe, 2026-04-15
==========================================
Removes duplicate rows from Carrier Database that were copied (not moved)
to Carrier Quarantine during the 2026-04-15 morning cleanup pass.

Safety guarantees:
  - Never deletes a row unless the SAME DOT exists in Quarantine.
  - Never deletes rows tagged Heavy Haul, Auto Transport, or Fuel.
  - Reads Quarantine as source-of-truth; cross-checks against cleanup log DOTs.
  - Deletes in descending row order so indices don't shift.
  - Verifies post-run state: no target DOTs remain in main, all 42 still in Quarantine.
  - Logs every delete to main_tab_dedupe_20260415.log.

Run from project root:
    PYTHONPATH=. python scripts/main_tab_dedupe_20260415.py [--dry-run]
"""
from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from app.google_auth import get_sheets_service

# ── Constants ─────────────────────────────────────────────────────────────────

SHEET_ID       = "1B5nzDCisdpG29bI0MjLtD8dIE2rsjdXg3Qncj-E58WE"
MAIN_TAB       = "Carrier Database"
QUARANTINE_TAB = "Carrier Quarantine"

# Retained-category service types — NEVER delete these from main.
RETAINED_SERVICE_TYPES = {"Heavy Haul", "Auto Transport", "Fuel"}

LOGS_DIR = Path("C:/Users/Owner/brokerops-ai/scripts/logs")
LOG_OUT  = LOGS_DIR / "main_tab_dedupe_20260415.log"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# DOTs confirmed moved to Quarantine per carrier_cleanup_execution_20260415.log
LOG_DOTS: set[str] = {
    "3355445", "3325439", "2779218", "3529027", "2508773", "1071704",
    "292830",  "3678704", "1026131", "2142253", "3306087", "3186652",
    "3335283", "801906",  "923537",  "2453189", "3530356", "3125719",
    "3024177", "1726329", "3005176", "2379403", "3320196", "1972717",
    "3858400", "610934",  "1001180", "2100804", "2186875", "841319",
    "1145975", "2906088", "2953511", "2570406", "1578182", "1622357",
    "1662319", "1005365", "1316597", "1281776", "1777594", "30144",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_OUT), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("main_tab_dedupe")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_tab_data(svc, tab: str) -> tuple[list[str], list[list[str]]]:
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"'{tab}'!A1:AK",
    ).execute()
    rows = resp.get("values", [])
    if not rows:
        return [], []
    return rows[0], rows[1:]


def _find_col(header: list[str], candidates: list[str]) -> int | None:
    for name in candidates:
        if name in header:
            return header.index(name)
    return None


def _get_main_sheet_id(svc) -> int:
    """Return the numeric sheetId for the Carrier Database tab."""
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    for sheet in meta.get("sheets", []):
        if sheet["properties"]["title"] == MAIN_TAB:
            return sheet["properties"]["sheetId"]
    raise RuntimeError(f"Tab '{MAIN_TAB}' not found in spreadsheet metadata")


def _has_basic_filter(svc) -> bool:
    """Return True if Carrier Database tab has a basicFilter set."""
    meta = svc.spreadsheets().get(
        spreadsheetId=SHEET_ID,
        fields="sheets.properties,sheets.basicFilter",
    ).execute()
    for sheet in meta.get("sheets", []):
        if sheet["properties"]["title"] == MAIN_TAB:
            return "basicFilter" in sheet and bool(sheet["basicFilter"])
    return False


def _reapply_basic_filter(svc, main_sheet_id: int, num_cols: int, num_rows: int) -> None:
    """Re-apply a basic filter over the full data range."""
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={
            "requests": [{
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": main_sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": num_rows + 1,   # +1 for header
                            "startColumnIndex": 0,
                            "endColumnIndex": num_cols,
                        }
                    }
                }
            }]
        },
    ).execute()
    logger.info("Re-applied basic filter over %d cols × %d data rows", num_cols, num_rows)


# ── Core logic ────────────────────────────────────────────────────────────────

def build_quarantine_dot_set(svc) -> set[str]:
    """Return all DOTs currently in the Quarantine tab."""
    q_header, q_data = _get_tab_data(svc, QUARANTINE_TAB)
    dot_col = _find_col(q_header, ["DOT Number", "DOT_Number", "DOT"])
    if dot_col is None:
        raise RuntimeError("Cannot find DOT column in Quarantine tab header")
    return {str(r[dot_col]).strip() for r in q_data if dot_col < len(r) and r[dot_col].strip()}


def main():
    parser = argparse.ArgumentParser(description="Remove main-tab copies of quarantined carriers")
    parser.add_argument("--dry-run", action="store_true", help="Log only, no sheet writes")
    args = parser.parse_args()
    dry_run = args.dry_run

    logger.info("=== main_tab_dedupe_20260415 START %s%s ===", _now_iso(),
                " [DRY-RUN]" if dry_run else "")

    svc = get_sheets_service()

    # ── Step 1: Build and cross-check DOT target list ──────────────────────────
    logger.info("Step 1: Cross-checking log DOTs vs Quarantine tab")

    quarantine_dots = build_quarantine_dot_set(svc)
    logger.info("Log DOTs: %d | Quarantine tab DOTs: %d", len(LOG_DOTS), len(quarantine_dots))

    log_not_in_q   = LOG_DOTS - quarantine_dots
    q_extra        = quarantine_dots - LOG_DOTS  # Quarantine has more than our 42 — fine
    agreed_targets = LOG_DOTS & quarantine_dots

    if log_not_in_q:
        logger.error(
            "STOP: %d DOTs from cleanup log are NOT in Quarantine: %s",
            len(log_not_in_q), sorted(log_not_in_q)
        )
        logger.error("Cannot safely delete these — Quarantine reversibility check failed. Aborting.")
        sys.exit(1)

    logger.info("All %d log DOTs confirmed in Quarantine. Agreed target set: %d",
                len(LOG_DOTS), len(agreed_targets))
    if q_extra:
        logger.info("Quarantine has %d additional DOTs beyond the 42 (OK, unrelated to this task): %s",
                    len(q_extra), sorted(q_extra))

    # ── Step 2: Read main tab ──────────────────────────────────────────────────
    logger.info("Step 2: Reading main Carrier Database tab")
    main_header, main_data = _get_tab_data(svc, MAIN_TAB)

    dot_col          = _find_col(main_header, ["DOT Number", "DOT_Number", "DOT"])
    service_type_col = _find_col(main_header, ["Service Type"])

    if dot_col is None:
        logger.error("Cannot find DOT column in main tab header. Aborting.")
        sys.exit(1)

    logger.info("Main tab: %d data rows | DOT col index: %d | Service Type col index: %s",
                len(main_data), dot_col,
                service_type_col if service_type_col is not None else "NOT FOUND")

    # ── Step 3: Identify rows to delete ───────────────────────────────────────
    logger.info("Step 3: Identifying target rows (DOT in agreed_targets, not retained service type)")

    # row_index here is 1-based sheet row (row 1 = header, row 2 = first data row)
    rows_to_delete: list[int] = []   # sheet row indices (1-based)
    skipped_orphan: list[str] = []
    skipped_retained: list[tuple[str, str]] = []

    for i, row in enumerate(main_data, start=2):  # row 2 is first data row
        if dot_col >= len(row):
            continue
        dot = str(row[dot_col]).strip()

        if dot not in agreed_targets:
            continue

        # Check retained service type
        if service_type_col is not None and service_type_col < len(row):
            svc_type = str(row[service_type_col]).strip()
        else:
            svc_type = ""

        if svc_type in RETAINED_SERVICE_TYPES:
            logger.warning(
                "SKIP DOT %s at row %d — Service Type '%s' is retained category",
                dot, i, svc_type
            )
            skipped_retained.append((dot, svc_type))
            continue

        rows_to_delete.append(i)
        logger.info("Targeted for delete: row %d DOT %s (Service Type: '%s')", i, dot, svc_type or "General")

    logger.info(
        "Rows targeted: %d | Skipped retained type: %d | Expected: 42",
        len(rows_to_delete), len(skipped_retained)
    )

    # Sanity check — if count is wildly off, stop before touching anything
    if len(rows_to_delete) < 30 and not dry_run:
        logger.error(
            "Only %d rows found for deletion (expected ~42). "
            "This is suspiciously low — aborting to avoid data loss. Run --dry-run to investigate.",
            len(rows_to_delete)
        )
        sys.exit(1)

    if len(rows_to_delete) == 0:
        logger.info("No rows to delete. Main tab may already be clean. Exiting.")
        return

    # ── Step 4: Batch delete in descending row order ───────────────────────────
    main_sheet_id = _get_main_sheet_id(svc)

    # Sort descending so earlier deletes don't shift later indices
    rows_desc = sorted(rows_to_delete, reverse=True)

    logger.info("Step 4: Deleting %d rows (descending order) from main tab", len(rows_desc))

    if dry_run:
        for r in rows_desc:
            logger.info("[DRY-RUN] Would delete sheet row %d", r)
        logger.info("[DRY-RUN] No changes written.")
    else:
        # Build one batchUpdate with all deleteDimension requests
        requests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": main_sheet_id,
                        "dimension": "ROWS",
                        # Sheets API uses 0-based startIndex, endIndex is exclusive
                        "startIndex": r - 1,   # convert 1-based to 0-based
                        "endIndex": r,
                    }
                }
            }
            for r in rows_desc
        ]

        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": requests},
        ).execute()

        logger.info("batchUpdate complete — %d rows deleted", len(requests))

        for r in rows_desc:
            logger.info("DELETED sheet row %d", r)

    # ── Step 5: Verify ────────────────────────────────────────────────────────
    logger.info("Step 5: Post-run verification")

    if not dry_run:
        post_header, post_data = _get_tab_data(svc, MAIN_TAB)
        post_dot_col = _find_col(post_header, ["DOT Number", "DOT_Number", "DOT"])
        post_main_dots: set[str] = set()
        if post_dot_col is not None:
            post_main_dots = {
                str(r[post_dot_col]).strip()
                for r in post_data
                if post_dot_col < len(r) and r[post_dot_col].strip()
            }

        surviving_targets = agreed_targets & post_main_dots
        if surviving_targets:
            logger.error(
                "VERIFY FAIL: %d target DOTs still present in main tab after delete: %s",
                len(surviving_targets), sorted(surviving_targets)
            )
        else:
            logger.info("VERIFY PASS: None of the %d target DOTs remain in main tab", len(agreed_targets))

        # Confirm Quarantine still has all 42
        post_q_dots = build_quarantine_dot_set(svc)
        missing_from_q = agreed_targets - post_q_dots
        if missing_from_q:
            logger.error(
                "VERIFY FAIL: %d target DOTs missing from Quarantine after run: %s",
                len(missing_from_q), sorted(missing_from_q)
            )
        else:
            logger.info("VERIFY PASS: All %d target DOTs still present in Quarantine tab", len(agreed_targets))

        post_row_count = len(post_data)
        logger.info("Post-run main tab data row count: %d", post_row_count)

        # Check / restore basic filter
        filter_active = _has_basic_filter(svc)
        if not filter_active:
            logger.warning("Basic filter not active after deletion — re-applying")
            _reapply_basic_filter(svc, main_sheet_id, len(post_header), post_row_count)
            filter_active = _has_basic_filter(svc)
        logger.info("Basic filter active on main tab: %s", filter_active)
    else:
        logger.info("[DRY-RUN] Skipping post-run verification (no writes made)")

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=== DEDUPE SUMMARY ===")
    logger.info("DOT list source: log parse + quarantine intersection — agree: %s",
                "YES" if not log_not_in_q else "NO")
    logger.info("Targeted: %d | Deleted: %d | Skipped retained type: %d | Orphans (not in Q): %d",
                len(rows_to_delete) + len(skipped_retained),
                0 if dry_run else len(rows_to_delete),
                len(skipped_retained),
                len(skipped_orphan))
    if skipped_retained:
        logger.info("Retained skips: %s", skipped_retained)
    logger.info("=== main_tab_dedupe_20260415 END %s ===", _now_iso())


if __name__ == "__main__":
    main()
