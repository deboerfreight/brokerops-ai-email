"""Vetting Pipeline Rebuild — Migration script (2026-04-14)

Phase B + C of the rebuild:

  1. Ensure the Carrier Quarantine tab exists (idempotent)
  2. Re-fetch FMCSA for all 128 rows (rate limited 1 req/sec)
  3. Update main tab with fresh data — DOT (E), address (L-O), Fleet Size (Q),
     Insurance (R, S), Authority (U-W). NEVER touches Notes (AE),
     Classification (AF), or Vetting Status (AG, written separately).
  4. Append a timestamped fresh-fetch line to the Notes column (AE).
  5. Re-run the vetting sweep on the refreshed data (writes col AG).
  6. For every row with status != pass_basic, append to Carrier Quarantine
     and queue for deletion from the main tab.
  7. Delete failing rows from the main tab in REVERSE order (preserves indices).
  8. Verify final state: main_count + quarantine_count == original_count.

Idempotent: running twice has no double-side-effects (quarantine append is
DOT-keyed; main-tab updates are values-only). Logged to
scripts/logs/vetting_rebuild_migration_20260414.log.

DO NOT INTERRUPT the FMCSA re-fetch loop — the rate limiter is the only thing
keeping us off the API ban list.
"""
from __future__ import annotations

import io
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

# Force UTF-8 stdout for Windows console
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from app.config import get_settings
from app.google_auth import get_sheets_service
from app.vetting.data_sync import fetch_fresh_fmcsa
from app.vetting.gate import vet_complete, PASS_BASIC
from app.vetting.quarantine import (
    QUARANTINE_TAB,
    ensure_quarantine_tab_exists,
    append_to_quarantine,
)

# ── Logging ──────────────────────────────────────────────────────────────
LOG_PATH = "scripts/logs/vetting_rebuild_migration_20260414.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("vetting_rebuild")

MAIN_TAB = "Carrier Database"
FMCSA_RATE_LIMIT_SEC = 1.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_main(svc, sheet_id: str) -> tuple[list[str], list[list[str]]]:
    resp = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"{MAIN_TAB}!A:AG",
    ).execute()
    rows = resp.get("values", [])
    if not rows:
        return [], []
    return rows[0], rows[1:]


def _row_to_dict(header: list[str], row: list[str]) -> dict:
    padded = row + [""] * (len(header) - len(row))
    return dict(zip(header, padded))


def _col_letter(idx: int) -> str:
    """0-indexed column → A1 letter (handles 2-letter columns up to ZZ)."""
    if idx < 26:
        return chr(ord("A") + idx)
    return "A" + chr(ord("A") + idx - 26)


def _apply_notes_appendix(
    svc, sheet_id: str, header: list[str],
    data: list[list[str]], appendix: list[str],
) -> int:
    """Append the FMCSA-refresh appendix to the Notes column for each row."""
    if "Notes" not in header:
        return 0
    notes_idx = header.index("Notes")
    notes_col = _col_letter(notes_idx)
    body_data = []
    for idx, append_str in enumerate(appendix):
        if not append_str:
            continue
        row_num = idx + 2
        old_notes = (data[idx][notes_idx] if len(data[idx]) > notes_idx else "") or ""
        new_notes = (old_notes + append_str).strip(" |")
        body_data.append({
            "range": f"{MAIN_TAB}!{notes_col}{row_num}",
            "values": [[new_notes]],
        })
    if not body_data:
        return 0
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=sheet_id,
        body={"valueInputOption": "USER_ENTERED", "data": body_data},
    ).execute()
    return len(body_data)


def _resweep_and_collect_fails(svc, sheet_id: str) -> tuple[
    list[str],          # statuses parallel to current data rows
    dict,               # counts by status
    list[tuple[int, dict, Any]],  # (row_num, carrier dict, VettingResult) for fails
]:
    """Read the (now refreshed) main tab, vet every row, write col AG, and
    return the list of failing rows for migration to quarantine."""
    header, data = _read_main(svc, sheet_id)
    statuses: list[str] = []
    counts: dict = {}
    fails: list[tuple[int, dict, Any]] = []

    for idx, raw in enumerate(data):
        row_num = idx + 2
        carrier = _row_to_dict(header, raw)
        result = vet_complete(carrier)
        statuses.append(result.status)
        counts[result.status] = counts.get(result.status, 0) + 1
        if result.status != PASS_BASIC:
            fails.append((row_num, carrier, result))

    # Write col AG in one call
    if statuses:
        last_row = len(statuses) + 1
        svc.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"{MAIN_TAB}!AG2:AG{last_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [[s] for s in statuses]},
        ).execute()

    return statuses, counts, fails


def _delete_rows_reverse(svc, sheet_id: str, main_sheet_id: int, row_nums: list[int]) -> int:
    """Delete rows from the main tab in REVERSE order to avoid index shift."""
    if not row_nums:
        return 0
    sorted_desc = sorted(set(row_nums), reverse=True)
    requests = []
    for rn in sorted_desc:
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": main_sheet_id,
                    "dimension": "ROWS",
                    "startIndex": rn - 1,
                    "endIndex": rn,
                }
            }
        })
    svc.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests": requests},
    ).execute()
    return len(sorted_desc)


def main():
    log.info("=" * 60)
    log.info("VETTING REBUILD MIGRATION starting at %s", _now_iso())
    log.info("=" * 60)

    sheet_id = get_settings().CARRIER_MASTER_SHEET_ID
    svc = get_sheets_service()

    # Resolve the main-tab sheetId for deleteDimension calls
    meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    main_sheet_id = None
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == MAIN_TAB:
            main_sheet_id = s["properties"]["sheetId"]
            break
    if main_sheet_id is None:
        log.error("Main tab '%s' not found", MAIN_TAB)
        sys.exit(1)

    header, data_initial = _read_main(svc, sheet_id)
    initial_count = len(data_initial)
    log.info("Initial main tab: %d data rows, %d header cols", initial_count, len(header))

    # ── Step 1: ensure quarantine tab ────────────────────────────────
    log.info("Step 1: ensure_quarantine_tab_exists")
    qid = ensure_quarantine_tab_exists(svc, sheet_id)
    log.info("Quarantine tab ready (sheetId=%d)", qid)

    # ── Step 2 & 3: FMCSA re-fetch + main-tab updates ────────────────
    log.info("Step 2-3: FMCSA re-fetch (rate-limited at %ss/req)", FMCSA_RATE_LIMIT_SEC)
    updated, errors, batch_entries, notes_appendix, diffs = _build_fresh_updates_inner(
        header, data_initial,
    )

    log.info("FMCSA fetch complete: %d rows updated, %d errors, %d cell updates queued",
             updated, errors, len(batch_entries))
    log.info("Diffs by category: %s", diffs)

    if batch_entries:
        # Apply in chunks to avoid hitting body-size limits
        CHUNK = 200
        for i in range(0, len(batch_entries), CHUNK):
            chunk = batch_entries[i:i + CHUNK]
            svc.spreadsheets().values().batchUpdate(
                spreadsheetId=sheet_id,
                body={"valueInputOption": "USER_ENTERED", "data": chunk},
            ).execute()
        log.info("Applied %d cell updates to main tab", len(batch_entries))

    # ── Step 4: Notes appendix ───────────────────────────────────────
    notes_written = _apply_notes_appendix(
        svc, sheet_id, header, data_initial, notes_appendix,
    )
    log.info("Notes appendix written for %d rows", notes_written)

    # ── Step 5: Re-vet entire (refreshed) main tab + write col AG ────
    log.info("Step 5: sweep_carrier_database (no second FMCSA fetch)")
    statuses, counts, fails = _resweep_and_collect_fails(svc, sheet_id)
    log.info("Re-vet results: %s", counts)
    log.info("%d rows scheduled for quarantine", len(fails))

    # ── Step 6: append fails to Carrier Quarantine ───────────────────
    log.info("Step 6: appending %d failing rows to Carrier Quarantine", len(fails))
    quarantined = 0
    quarantine_errors = 0
    for row_num, carrier, result in fails:
        try:
            append_to_quarantine(svc, sheet_id, carrier, result, original_row_number=row_num)
            quarantined += 1
            log.info("Quarantined row %d DOT %s: %s", row_num,
                     carrier.get("DOT Number", "?"), result.status)
        except Exception as exc:
            log.error("Failed to quarantine row %d: %s", row_num, exc)
            quarantine_errors += 1

    if quarantine_errors:
        log.error("%d quarantine writes failed — STOPPING before main-tab deletion",
                  quarantine_errors)
        log.error("Manual recovery required. See log: %s", LOG_PATH)
        sys.exit(2)

    # ── Step 7: delete fails from main tab in REVERSE row order ──────
    log.info("Step 7: deleting %d rows from main tab (reverse order)", len(fails))
    fail_rows = [rn for (rn, _, _) in fails]
    deleted = _delete_rows_reverse(svc, sheet_id, main_sheet_id, fail_rows)
    log.info("Deleted %d rows from main tab", deleted)

    # ── Step 8: verify ───────────────────────────────────────────────
    log.info("Step 8: verifying final state")
    _, data_final = _read_main(svc, sheet_id)
    main_final = len(data_final)
    log.info("Main tab final count: %d", main_final)

    # Re-read quarantine count
    q_resp = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"{QUARANTINE_TAB}!A:A",
    ).execute()
    q_rows = q_resp.get("values", [])
    quarantine_final = max(0, len(q_rows) - 1)  # subtract header
    log.info("Quarantine tab final count: %d", quarantine_final)

    log.info("=" * 60)
    log.info("RECONCILIATION:")
    log.info("  initial main rows:     %d", initial_count)
    log.info("  fmcsa updates:         %d (rows updated), %d (errors)", updated, errors)
    log.info("  vet results after refresh: %s", counts)
    log.info("  rows quarantined:      %d", quarantined)
    log.info("  rows deleted from main: %d", deleted)
    log.info("  final main count:      %d", main_final)
    log.info("  final quarantine count:%d", quarantine_final)
    log.info("  expected: main_final == counts.get(pass_basic, 0) == %d",
             counts.get(PASS_BASIC, 0))
    if main_final == counts.get(PASS_BASIC, 0):
        log.info("  ✓ main tab is 100%% pass_basic")
    else:
        log.error("  ✗ main tab count mismatch: %d != %d (expected pass_basic count)",
                  main_final, counts.get(PASS_BASIC, 0))
    log.info("=" * 60)


def _build_fresh_updates_inner(header, data):
    cols = {h: i for i, h in enumerate(header)}
    updated = 0
    errors = 0
    diffs = {"fleet": 0, "liability": 0, "cargo": 0, "safety": 0,
             "authority": 0, "address": 0}
    batch_entries: list[dict] = []
    notes_appendix: list[str] = [""] * len(data)

    fmcsa_to_col = {
        "DOT_Number": "DOT Number",
        "City": "City",
        "State": "State",
        "Zip": "ZIP",
        "Power_Units": "Fleet Size",
        "Insurance_Liability": "Insurance Liability",
        "Insurance_Cargo": "Insurance Cargo",
        "Authority_Status": "Authority Status",
        "Authority_Date": "Authority Date",
        "Safety_Rating": "Safety Rating",
    }

    for idx, raw in enumerate(data):
        row_num = idx + 2
        carrier = _row_to_dict(header, raw)
        dot = (carrier.get("DOT Number") or "").strip()
        if not dot:
            log.warning("Row %d has no DOT — skipping FMCSA fetch", row_num)
            continue

        log.info("Row %d (%d/%d): FMCSA fetch DOT %s",
                 row_num, idx + 1, len(data), dot)
        try:
            fresh = fetch_fresh_fmcsa(dot)
        except Exception as exc:
            log.error("Row %d DOT %s fetch raised: %s", row_num, dot, exc)
            errors += 1
            time.sleep(FMCSA_RATE_LIMIT_SEC)
            continue
        if not fresh:
            log.warning("Row %d DOT %s: empty FMCSA result", row_num, dot)
            errors += 1
            time.sleep(FMCSA_RATE_LIMIT_SEC)
            continue

        changes: list[str] = []
        for fmcsa_key, sheet_col in fmcsa_to_col.items():
            if sheet_col not in cols:
                continue
            col_idx = cols[sheet_col]
            old_val = (raw[col_idx] if len(raw) > col_idx else "") or ""
            new_val = fresh.get(fmcsa_key, "")
            if new_val in (None, ""):
                continue
            new_str = str(new_val).strip()
            old_str = str(old_val).strip()
            if sheet_col in ("Fleet Size", "Insurance Liability", "Insurance Cargo"):
                try:
                    old_num = int(float(old_str.replace("$", "").replace(",", "") or 0))
                except (ValueError, TypeError):
                    old_num = -1
                try:
                    new_num = int(float(str(new_val).replace("$", "").replace(",", "") or 0))
                except (ValueError, TypeError):
                    new_num = -1
                if old_num == new_num:
                    continue
                changes.append(f"{sheet_col}: {old_str or '(blank)'}->{new_str}")
                if sheet_col == "Fleet Size":
                    diffs["fleet"] += 1
                elif sheet_col == "Insurance Liability":
                    diffs["liability"] += 1
                elif sheet_col == "Insurance Cargo":
                    diffs["cargo"] += 1
            else:
                if old_str.upper() == new_str.upper():
                    continue
                changes.append(f"{sheet_col}: {old_str or '(blank)'}->{new_str}")
                if sheet_col in ("City", "State", "ZIP"):
                    diffs["address"] += 1
                elif sheet_col == "Safety Rating":
                    diffs["safety"] += 1
                elif sheet_col == "Authority Status":
                    diffs["authority"] += 1

            col_letter = _col_letter(col_idx)
            batch_entries.append({
                "range": f"{MAIN_TAB}!{col_letter}{row_num}",
                "values": [[new_str]],
            })

        if changes:
            updated += 1
            notes_appendix[idx] = (
                f" | [{_now_iso()}] FMCSA refresh: " + "; ".join(changes)
            )
        time.sleep(FMCSA_RATE_LIMIT_SEC)

    return updated, errors, batch_entries, notes_appendix, diffs


if __name__ == "__main__":
    main()
