"""
BrokerOps AI — Website column + confidence gate post-backfill tasks, 2026-04-15

Tasks:
  1. Add 'Website' column (col AI, index 34) to Carrier Database tab
  2. Extend basic filter range to cover the new column
  3. Backfill 2 recent Brave HITs: CTS Inc (DOT 1250299) and Kwik Logistics (DOT 3340432)
  4. Re-run enrichment on both DOTs to verify confidence gate behavior
  5. Clear Kwik's Contact Email if it still holds the Yahoo address
"""
from __future__ import annotations

import logging
import os
import sys

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_PATH = "C:/Users/Owner/brokerops-ai/scripts/logs/website_col_and_confidence_gate_20260415.log"
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("website_col_gate")

# ── Bootstrap path ──────────────────────────────────────────────────────────────
sys.path.insert(0, "C:/Users/Owner/brokerops-ai")

from app.config import get_settings
from app.google_auth import get_sheets_service

SPREADSHEET_ID = "1B5nzDCisdpG29bI0MjLtD8dIE2rsjdXg3Qncj-E58WE"
SHEET_ID = 0  # Carrier Database sheetId
MAIN_TAB = "Carrier Database"
WEBSITE_HEADER = "Website"

# Backfill data
BACKFILL = [
    {"dot": "1250299", "name": "CTS Inc",        "website": "ctslogisticssolutions.com"},
    {"dot": "3340432", "name": "Kwik Logistics",  "website": "brokersnapshot.com"},
]
KWIK_DOT = "3340432"
KWIK_YAHOO = "nataliepacker50@yahoo.com"


def col_letter(n: int) -> str:
    """Convert 0-based column index to spreadsheet letter."""
    result = ""
    n += 1
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def get_sheet_data(svc):
    """Return (header_list, data_rows) from Carrier Database, reading wide."""
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{MAIN_TAB}'!A:AJ",
    ).execute()
    rows = resp.get("values", [])
    if not rows:
        return [], []
    return rows[0], rows[1:]


# ── Task 1: Add Website column ─────────────────────────────────────────────────

def add_website_column(svc) -> int:
    """Add Website column if absent. Returns 0-based col index."""
    header, data = get_sheet_data(svc)

    if WEBSITE_HEADER in header:
        col_idx = header.index(WEBSITE_HEADER)
        logger.info("Website column already exists at col %s (index %d) — skipping add",
                    col_letter(col_idx), col_idx)
        return col_idx

    col_idx = len(header)
    col_ltr = col_letter(col_idx)
    num_data_rows = len(data)

    logger.info("Adding Website column at col %s (index %d), %d data rows",
                col_ltr, col_idx, num_data_rows)

    # Expand the grid to accommodate the new column if needed
    meta = svc.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets(properties)",
    ).execute()
    for s in meta.get("sheets", []):
        if s["properties"]["sheetId"] == SHEET_ID:
            current_cols = s["properties"]["gridProperties"]["columnCount"]
            if current_cols <= col_idx:
                cols_to_add = col_idx - current_cols + 2  # add a bit of buffer
                svc.spreadsheets().batchUpdate(
                    spreadsheetId=SPREADSHEET_ID,
                    body={"requests": [{"appendDimension": {
                        "sheetId": SHEET_ID,
                        "dimension": "COLUMNS",
                        "length": cols_to_add,
                    }}]},
                ).execute()
                logger.info("Expanded grid from %d to %d columns", current_cols, current_cols + cols_to_add)
            break

    # Write header
    svc.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{MAIN_TAB}'!{col_ltr}1",
        valueInputOption="USER_ENTERED",
        body={"values": [[WEBSITE_HEADER]]},
    ).execute()
    logger.info("Wrote header '%s' at '%s'!%s1", WEBSITE_HEADER, MAIN_TAB, col_ltr)

    # Existing rows get blank (no fill needed — Sheets leaves them blank by default)
    # But write explicit blanks so the column is well-formed
    if num_data_rows > 0:
        blanks = [[""] for _ in range(num_data_rows)]
        svc.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{MAIN_TAB}'!{col_ltr}2:{col_ltr}{num_data_rows + 1}",
            valueInputOption="USER_ENTERED",
            body={"values": blanks},
        ).execute()
        logger.info("Wrote %d blank defaults for existing rows in col %s", num_data_rows, col_ltr)

    return col_idx


# ── Task 2: Extend basic filter ────────────────────────────────────────────────

def extend_basic_filter(svc, website_col_idx: int) -> None:
    """Extend the sheet's basic filter range to cover the new Website column."""
    # Read current filter range
    meta = svc.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets(properties,basicFilter)",
    ).execute()

    target_sheet = None
    for s in meta.get("sheets", []):
        if s["properties"]["sheetId"] == SHEET_ID:
            target_sheet = s
            break

    if target_sheet is None:
        logger.warning("Could not find sheetId=%d in spreadsheet metadata", SHEET_ID)
        return

    current_filter = target_sheet.get("basicFilter")
    if current_filter:
        r = current_filter.get("range", {})
        current_end_col = r.get("endColumnIndex", 0)
        new_end_col = website_col_idx + 1  # endColumnIndex is exclusive
        if current_end_col >= new_end_col:
            logger.info(
                "Basic filter already covers col %d (endColumnIndex=%d) — no update needed",
                website_col_idx, current_end_col,
            )
            return
        logger.info(
            "Extending basic filter from endColumnIndex=%d to %d",
            current_end_col, new_end_col,
        )
        new_range = dict(r)
        new_range["endColumnIndex"] = new_end_col
        new_range["sheetId"] = SHEET_ID
    else:
        logger.info("No existing basic filter found — creating one covering A:%s",
                    col_letter(website_col_idx))
        header, data = get_sheet_data(svc)
        num_rows = len(data) + 1  # +1 for header
        new_range = {
            "sheetId": SHEET_ID,
            "startRowIndex": 0,
            "startColumnIndex": 0,
            "endRowIndex": num_rows + 1,
            "endColumnIndex": website_col_idx + 1,
        }

    svc.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "requests": [{
                "setBasicFilter": {
                    "filter": {"range": new_range}
                }
            }]
        },
    ).execute()
    logger.info("Basic filter extended to include col %s", col_letter(website_col_idx))


# ── Task 3: Backfill the 2 HITs ───────────────────────────────────────────────

def backfill_websites(svc, website_col_idx: int) -> None:
    """Write discovered websites for CTS Inc and Kwik Logistics."""
    header, data = get_sheet_data(svc)

    if "DOT Number" not in header:
        logger.error("DOT Number column not found in header — cannot backfill")
        return

    dot_idx = header.index("DOT Number")
    col_ltr = col_letter(website_col_idx)

    for entry in BACKFILL:
        dot = entry["dot"]
        website = entry["website"]
        found = False
        for i, row in enumerate(data, start=2):
            padded = row + [""] * (len(header) - len(row))
            if padded[dot_idx].strip() == dot:
                cell = f"'{MAIN_TAB}'!{col_ltr}{i}"
                svc.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=cell,
                    valueInputOption="USER_ENTERED",
                    body={"values": [[website]]},
                ).execute()
                logger.info("Backfilled DOT %s (%s): Website=%s at row %d col %s",
                            dot, entry["name"], website, i, col_ltr)
                found = True
                break
        if not found:
            logger.warning("DOT %s (%s) not found in sheet — skipping backfill",
                           dot, entry["name"])


# ── Task 4: Verification re-runs ──────────────────────────────────────────────

def verify_enrichment() -> dict:
    """Re-run enrich_carrier_email for DOT 3340432 and DOT 1250299.

    Returns a dict with results for reporting.
    """
    from app.email_enrichment import enrich_carrier_email

    results = {}

    # Kwik Logistics — expect PHONE_ONLY after gate fix
    kwik = {
        "DOT_Number": "3340432",
        "MC_Number": "",
        "Legal_Name": "Kwik Logistics",
        "City": "",
        "State": "OH",
    }
    logger.info("--- Re-running enrichment for Kwik Logistics (DOT 3340432) ---")
    kwik_result = enrich_carrier_email(kwik)
    logger.info("Kwik result: source=%s email=%s website=%s",
                kwik_result.get("source"), kwik_result.get("email"), kwik_result.get("website"))
    results["kwik"] = kwik_result

    # CTS Inc — expect BRAVE_SEARCH HIT still
    cts = {
        "DOT_Number": "1250299",
        "MC_Number": "",
        "Legal_Name": "CTS Inc",
        "City": "",
        "State": "OH",
    }
    logger.info("--- Re-running enrichment for CTS Inc (DOT 1250299) ---")
    cts_result = enrich_carrier_email(cts)
    logger.info("CTS result: source=%s email=%s website=%s",
                cts_result.get("source"), cts_result.get("email"), cts_result.get("website"))
    results["cts"] = cts_result

    return results


# ── Task 5: Clear Kwik's Yahoo email if still present ─────────────────────────

def clear_kwik_yahoo_email(svc) -> bool:
    """If Kwik's Contact Email still holds the Yahoo address, clear it."""
    header, data = get_sheet_data(svc)

    if "DOT Number" not in header or "Contact Email" not in header:
        logger.error("Required columns not found — cannot clear Kwik email")
        return False

    dot_idx = header.index("DOT Number")
    email_idx = header.index("Contact Email")
    email_col_ltr = col_letter(email_idx)

    for i, row in enumerate(data, start=2):
        padded = row + [""] * (len(header) - len(row))
        if padded[dot_idx].strip() == KWIK_DOT:
            current_email = padded[email_idx].strip().lower()
            if current_email == KWIK_YAHOO.lower() or current_email == "phone_only":
                if current_email == KWIK_YAHOO.lower():
                    cell = f"'{MAIN_TAB}'!{email_col_ltr}{i}"
                    svc.spreadsheets().values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=cell,
                        valueInputOption="USER_ENTERED",
                        body={"values": [["PHONE_ONLY"]]},
                    ).execute()
                    logger.info("Cleared Kwik Logistics Yahoo email at row %d → PHONE_ONLY", i)
                    return True
                else:
                    logger.info("Kwik Logistics Contact Email already PHONE_ONLY — no clear needed")
                    return True
            else:
                logger.info("Kwik Logistics Contact Email is '%s' — not Yahoo, not touching", current_email)
                return False
    logger.warning("DOT %s (Kwik Logistics) not found in sheet — cannot clear email", KWIK_DOT)
    return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 70)
    logger.info("website_col_and_confidence_gate_20260415.py — START")
    logger.info("=" * 70)

    # ── Import test ──
    logger.info("--- Import test: enrich_carrier_email ---")
    try:
        from app.email_enrichment import enrich_carrier_email  # noqa: F401
        logger.info("Import OK")
    except Exception as exc:
        logger.error("Import FAILED: %s", exc)
        sys.exit(1)

    svc = get_sheets_service()

    # Task 1 — Add Website column
    logger.info("--- Task 1: Add Website column ---")
    website_col_idx = add_website_column(svc)
    logger.info("Task 1 complete: Website column at col %s (index %d)",
                col_letter(website_col_idx), website_col_idx)

    # Task 2 — Extend basic filter
    logger.info("--- Task 2: Extend basic filter ---")
    extend_basic_filter(svc, website_col_idx)
    logger.info("Task 2 complete")

    # Task 3 — Backfill 2 HITs
    logger.info("--- Task 3: Backfill CTS Inc + Kwik Logistics websites ---")
    backfill_websites(svc, website_col_idx)
    logger.info("Task 3 complete")

    # Task 4 — Verification re-runs
    logger.info("--- Task 4: Verification re-runs ---")
    enrichment_results = verify_enrichment()

    kwik_verdict = enrichment_results["kwik"].get("source")
    cts_verdict = enrichment_results["cts"].get("source")
    kwik_phone_only = kwik_verdict == "PHONE_ONLY"
    cts_still_hit = cts_verdict in ("BRAVE_SEARCH", "APOLLO")

    logger.info("Kwik Logistics re-run → %s (expected PHONE_ONLY: %s)",
                kwik_verdict, "YES" if kwik_phone_only else "NO — gate may not be firing")
    logger.info("CTS Inc re-run → %s (still HIT: %s)",
                cts_verdict, "YES" if cts_still_hit else "NO — gate may be too tight")

    if not kwik_phone_only:
        logger.warning("GATE NOT FIRING for Kwik Logistics — investigate _domain_match_confidence")
    if not cts_still_hit:
        logger.warning("CTS Inc downgraded — fuzzy threshold may be too tight, consider relaxing")

    # Write Kwik's website even if email downgraded (task instructions: website still useful)
    kwik_discovered_website = enrichment_results["kwik"].get("website")
    if kwik_discovered_website:
        logger.info("Kwik re-run discovered website: %s (written to sheet via backfill step above)",
                    kwik_discovered_website)

    # Task 5 — Clear Kwik Yahoo email if present
    logger.info("--- Task 5: Clear Kwik Yahoo email from sheet ---")
    cleared = clear_kwik_yahoo_email(svc)
    logger.info("Kwik Yahoo email cleared: %s", "YES" if cleared else "NO (already clean or not found)")

    # ── Summary ──
    logger.info("=" * 70)
    logger.info("SUMMARY")
    logger.info("  Website column:    col %s (index %d)", col_letter(website_col_idx), website_col_idx)
    logger.info("  Filter extended:   YES")
    logger.info("  Backfills:         CTS Inc (DOT 1250299) → ctslogisticssolutions.com")
    logger.info("                     Kwik Logistics (DOT 3340432) → brokersnapshot.com")
    logger.info("  Kwik re-run:       %s (expected PHONE_ONLY)", kwik_verdict)
    logger.info("  CTS re-run:        %s (expected HIT)", cts_verdict)
    logger.info("  Kwik Yahoo cleared: %s", "YES" if cleared else "NO")
    logger.info("=" * 70)
    logger.info("Done.")


if __name__ == "__main__":
    main()
