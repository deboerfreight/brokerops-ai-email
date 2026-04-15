"""Vetting Pipeline Rebuild — Finish Migration (2026-04-14, recovery script)

Picks up after vetting_rebuild_migration_20260414.py hit a Sheets read-rate
limit mid-quarantine. The FMCSA refresh and col AG re-vet have already been
applied to the main tab; we just need to:

  1. Read the (already-quarantined) DOTs from Carrier Quarantine.
  2. Read the main tab once.
  3. For every row in the main tab whose Vetting Status != pass_basic AND
     whose DOT is NOT already in quarantine, build a quarantine entry.
  4. Append all new quarantine entries in ONE batched call.
  5. Delete every failing row from the main tab in REVERSE order, in ONE
     batchUpdate call.
  6. Verify final state.

Idempotent: safe to re-run. Existing quarantine entries are skipped.
"""
from __future__ import annotations

import io
import logging
import sys
from datetime import datetime, timezone

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from app.config import get_settings
from app.google_auth import get_sheets_service
from app.vetting.gate import vet_complete, PASS_BASIC
from app.vetting.quarantine import (
    QUARANTINE_TAB,
    ensure_quarantine_tab_exists,
    QUARANTINE_EXTRA_COLUMNS,
)

LOG_PATH = "scripts/logs/vetting_rebuild_migration_20260414.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("vetting_rebuild_finish")

MAIN_TAB = "Carrier Database"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main():
    log.info("=" * 60)
    log.info("VETTING REBUILD FINISH starting at %s", _now_iso())
    log.info("=" * 60)

    sheet_id = get_settings().CARRIER_MASTER_SHEET_ID
    svc = get_sheets_service()

    ensure_quarantine_tab_exists(svc, sheet_id)

    # Resolve sheet IDs for delete operations
    meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    main_sheet_id = None
    quarantine_sheet_id = None
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == MAIN_TAB:
            main_sheet_id = s["properties"]["sheetId"]
        if s["properties"]["title"] == QUARANTINE_TAB:
            quarantine_sheet_id = s["properties"]["sheetId"]
    if main_sheet_id is None or quarantine_sheet_id is None:
        log.error("Could not resolve sheet IDs (main=%s, quarantine=%s)",
                  main_sheet_id, quarantine_sheet_id)
        sys.exit(1)

    # ── Read both tabs once ──────────────────────────────────────────
    log.info("Reading main tab")
    main_resp = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"{MAIN_TAB}!A:AG",
    ).execute()
    main_rows = main_resp.get("values", [])
    if not main_rows:
        log.error("Main tab empty — abort")
        sys.exit(1)
    main_header = main_rows[0]
    main_data = main_rows[1:]
    log.info("Main tab: %d cols, %d data rows", len(main_header), len(main_data))

    log.info("Reading quarantine tab")
    q_resp = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"{QUARANTINE_TAB}!A:AK",
    ).execute()
    q_rows = q_resp.get("values", [])
    q_header = q_rows[0] if q_rows else (
        list(main_header) + list(QUARANTINE_EXTRA_COLUMNS)
    )
    q_data = q_rows[1:] if q_rows else []
    log.info("Quarantine tab: %d cols, %d existing rows", len(q_header), len(q_data))

    # Existing quarantine DOTs (idempotency)
    q_dot_idx = q_header.index("DOT Number")
    existing_q_dots = set()
    for r in q_data:
        if len(r) > q_dot_idx:
            d = str(r[q_dot_idx]).strip()
            if d:
                existing_q_dots.add(d)
    log.info("Existing quarantine DOTs: %d", len(existing_q_dots))

    # ── Vet every row, queue fails ──────────────────────────────────
    main_dot_idx = main_header.index("DOT Number")
    fails: list[tuple[int, list[str], object]] = []  # (row_num, raw_row, result)
    statuses_by_status: dict = {}
    for idx, raw in enumerate(main_data):
        row_num = idx + 2
        carrier = dict(zip(main_header, raw + [""] * (len(main_header) - len(raw))))
        result = vet_complete(carrier)
        statuses_by_status[result.status] = statuses_by_status.get(result.status, 0) + 1
        if result.status != PASS_BASIC:
            fails.append((row_num, raw, result))

    log.info("Vet results: %s", statuses_by_status)
    log.info("%d failing rows in main tab", len(fails))

    # ── Build new quarantine appends ────────────────────────────────
    new_q_payloads: list[list[str]] = []
    fail_row_nums: list[int] = []
    skipped_already_q = 0

    for row_num, raw, result in fails:
        dot = (raw[main_dot_idx] if len(raw) > main_dot_idx else "").strip()
        fail_row_nums.append(row_num)
        if dot and dot in existing_q_dots:
            skipped_already_q += 1
            continue

        # Build the payload to match q_header order
        # Pad raw to main_header length
        padded = raw + [""] * (len(main_header) - len(raw))
        carrier_dict = dict(zip(main_header, padded))

        payload = []
        for col in q_header:
            if col == "Quarantine Reason":
                payload.append(f"{result.status}: {result.reason}")
            elif col == "Quarantined At":
                payload.append(_now_iso())
            elif col == "Original Row Number":
                payload.append(str(row_num))
            elif col == "Last Re-checked":
                payload.append(_now_iso())
            else:
                payload.append(str(carrier_dict.get(col, "")))
        new_q_payloads.append(payload)

    log.info("New quarantine appends: %d (skipped %d already in quarantine)",
             len(new_q_payloads), skipped_already_q)

    # ── Append all new quarantine rows in ONE call ──────────────────
    if new_q_payloads:
        svc.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range=f"{QUARANTINE_TAB}!A:AK",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": new_q_payloads},
        ).execute()
        log.info("Appended %d new quarantine rows in one batch", len(new_q_payloads))

    # ── Delete failing rows from main tab in REVERSE order ──────────
    if fail_row_nums:
        sorted_desc = sorted(set(fail_row_nums), reverse=True)
        requests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": main_sheet_id,
                        "dimension": "ROWS",
                        "startIndex": rn - 1,
                        "endIndex": rn,
                    }
                }
            }
            for rn in sorted_desc
        ]
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": requests},
        ).execute()
        log.info("Deleted %d rows from main tab", len(sorted_desc))

    # ── Verify ──────────────────────────────────────────────────────
    final_main = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"{MAIN_TAB}!A:A",
    ).execute()
    final_main_rows = max(0, len(final_main.get("values", [])) - 1)

    final_q = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"{QUARANTINE_TAB}!A:A",
    ).execute()
    final_q_rows = max(0, len(final_q.get("values", [])) - 1)

    log.info("=" * 60)
    log.info("RECONCILIATION:")
    log.info("  vet results: %s", statuses_by_status)
    log.info("  failing rows: %d", len(fails))
    log.info("  new quarantine rows: %d", len(new_q_payloads))
    log.info("  skipped (already in quarantine): %d", skipped_already_q)
    log.info("  rows deleted from main: %d", len(set(fail_row_nums)))
    log.info("  final main tab data rows:    %d", final_main_rows)
    log.info("  final quarantine data rows:  %d", final_q_rows)
    log.info("  expected main = pass_basic count = %d",
             statuses_by_status.get(PASS_BASIC, 0))
    if final_main_rows == statuses_by_status.get(PASS_BASIC, 0):
        log.info("  ✓ main tab is 100%% pass_basic")
    else:
        log.error("  ✗ main tab count mismatch: %d != %d",
                  final_main_rows, statuses_by_status.get(PASS_BASIC, 0))
    log.info("=" * 60)


if __name__ == "__main__":
    main()
