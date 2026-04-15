"""BrokerOps AI – Carrier Quarantine tab management.

The quarantine tab is the home for any carrier row that fails `vet_complete`.
Schema: same 33 cols as `Carrier Database` (A–AG) plus 4 metadata columns:

    AH  Quarantine Reason       (status code from VettingResult)
    AI  Quarantined At          (ISO8601 timestamp)
    AJ  Original Row Number     (row number in main tab when quarantined)
    AK  Last Re-checked         (ISO timestamp of last re-vet attempt)

All operations are idempotent on DOT Number. Append twice for the same DOT and
the second call updates the existing row instead of duplicating.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from app.vetting.gate import VettingResult

logger = logging.getLogger("brokerops.vetting.quarantine")

QUARANTINE_TAB = "Carrier Quarantine"
# 33 main columns + 4 quarantine metadata
QUARANTINE_EXTRA_COLUMNS = [
    "Quarantine Reason",
    "Quarantined At",
    "Original Row Number",
    "Last Re-checked",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_quarantine_tab_exists(svc, spreadsheet_id: str) -> int:
    """Create the Carrier Quarantine tab if it doesn't exist.

    Idempotent — safe to call repeatedly. Returns the sheetId of the tab.

    On creation:
      - Copies the header row from `Carrier Database` (cols A–AG)
      - Appends the 4 quarantine metadata headers (cols AH–AK)
      - Freezes the header row
      - Sets column count to 37
    """
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == QUARANTINE_TAB:
            logger.info("Quarantine tab already exists (sheetId=%s)",
                        s["properties"]["sheetId"])
            return s["properties"]["sheetId"]

    # Read header from main tab
    header_resp = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range="Carrier Database!A1:AG1",
    ).execute()
    main_header = header_resp.get("values", [[]])[0]
    full_header = list(main_header) + list(QUARANTINE_EXTRA_COLUMNS)

    # Create the new tab
    add_resp = svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [{
                "addSheet": {
                    "properties": {
                        "title": QUARANTINE_TAB,
                        "gridProperties": {
                            "rowCount": 500,
                            "columnCount": len(full_header),
                            "frozenRowCount": 1,
                        },
                    }
                }
            }]
        },
    ).execute()
    new_sheet_id = add_resp["replies"][0]["addSheet"]["properties"]["sheetId"]
    logger.info("Created %s (sheetId=%s) with %d cols",
                QUARANTINE_TAB, new_sheet_id, len(full_header))

    # Write header row
    svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{QUARANTINE_TAB}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": [full_header]},
    ).execute()

    return new_sheet_id


def get_quarantine_rows(svc, spreadsheet_id: str) -> list[dict[str, str]]:
    """Read all quarantine rows as list of dicts keyed by header."""
    resp = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{QUARANTINE_TAB}!A:AK",
    ).execute()
    rows = resp.get("values", [])
    if not rows:
        return []
    header = rows[0]
    out = []
    for r in rows[1:]:
        padded = r + [""] * (len(header) - len(r))
        out.append(dict(zip(header, padded)))
    return out


def _find_quarantine_row_by_dot(svc, spreadsheet_id: str, dot: str) -> Optional[int]:
    """Return 1-indexed row number of the existing quarantine entry, or None."""
    resp = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{QUARANTINE_TAB}!A:E",
    ).execute()
    rows = resp.get("values", [])
    if not rows:
        return None
    header = rows[0]
    try:
        dot_idx = header.index("DOT Number")
    except ValueError:
        return None
    target = str(dot).strip()
    for i, r in enumerate(rows[1:], start=2):
        if len(r) > dot_idx and str(r[dot_idx]).strip() == target:
            return i
    return None


def append_to_quarantine(
    svc,
    spreadsheet_id: str,
    row: dict[str, Any],
    result: VettingResult,
    original_row_number: Optional[int] = None,
) -> int:
    """Add (or update) a quarantine entry for the given carrier row.

    Idempotent on DOT Number — if a row already exists with the same DOT, it is
    updated in place (carrier data refreshed, Last Re-checked stamped) instead
    of duplicating.

    Returns the 1-indexed row number written.
    """
    ensure_quarantine_tab_exists(svc, spreadsheet_id)

    # Read full quarantine header
    header_resp = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{QUARANTINE_TAB}!A1:AK1",
    ).execute()
    header = header_resp.get("values", [[]])[0]

    # Build the row payload — try to match every header from the input dict
    payload: list[str] = []
    for col in header:
        if col == "Quarantine Reason":
            payload.append(f"{result.status}: {result.reason}")
        elif col == "Quarantined At":
            payload.append(_now_iso())
        elif col == "Original Row Number":
            payload.append(str(original_row_number or ""))
        elif col == "Last Re-checked":
            payload.append(_now_iso())
        else:
            # Look for col in row dict; tolerate alias keys
            val = row.get(col)
            if val is None or val == "":
                # Try the underscored alias (e.g. "Vetting Status" → "Vetting_Status")
                alias = col.replace(" ", "_")
                val = row.get(alias, "")
            payload.append("" if val is None else str(val))

    dot = str(row.get("DOT Number") or row.get("DOT_Number") or "").strip()
    existing_row = _find_quarantine_row_by_dot(svc, spreadsheet_id, dot) if dot else None

    if existing_row:
        # Update in place. Preserve the original Quarantined At and Original Row Number.
        existing_resp = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{QUARANTINE_TAB}!A{existing_row}:AK{existing_row}",
        ).execute()
        existing_row_vals = existing_resp.get("values", [[]])[0]
        # Pad to header length
        existing_row_vals += [""] * (len(header) - len(existing_row_vals))
        for i, col in enumerate(header):
            if col == "Quarantined At" and existing_row_vals[i]:
                payload[i] = existing_row_vals[i]
            if col == "Original Row Number" and existing_row_vals[i]:
                payload[i] = existing_row_vals[i]

        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{QUARANTINE_TAB}!A{existing_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [payload]},
        ).execute()
        logger.info("Updated quarantine row %d for DOT %s (%s)",
                    existing_row, dot, result.status)
        return existing_row

    # Append a fresh row
    append_resp = svc.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{QUARANTINE_TAB}!A:AK",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [payload]},
    ).execute()
    updated = append_resp.get("updates", {}).get("updatedRange", "")
    logger.info("Appended quarantine row for DOT %s (%s) -> %s",
                dot, result.status, updated)
    # Parse out the row number from updatedRange like "Carrier Quarantine!A50:AK50"
    try:
        tail = updated.split("!")[1]
        row_num = int("".join(c for c in tail.split(":")[0] if c.isdigit()))
        return row_num
    except (IndexError, ValueError):
        return -1


def release_from_quarantine(svc, spreadsheet_id: str, dot: str) -> bool:
    """Move a row from Quarantine back to Carrier Database after re-vetting passes.

    Returns True if a row was released, False if no matching DOT was found.
    """
    if not dot:
        return False
    target_row = _find_quarantine_row_by_dot(svc, spreadsheet_id, str(dot).strip())
    if not target_row:
        return False

    # Read the row, strip the 4 metadata columns, append to main tab
    resp = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{QUARANTINE_TAB}!A{target_row}:AG{target_row}",
    ).execute()
    main_payload = resp.get("values", [[]])[0]

    svc.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range="Carrier Database!A:AG",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [main_payload]},
    ).execute()

    # Delete the row from the quarantine tab
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    quarantine_sheet_id = None
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == QUARANTINE_TAB:
            quarantine_sheet_id = s["properties"]["sheetId"]
            break
    if quarantine_sheet_id is None:
        return False

    svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [{
                "deleteDimension": {
                    "range": {
                        "sheetId": quarantine_sheet_id,
                        "dimension": "ROWS",
                        "startIndex": target_row - 1,
                        "endIndex": target_row,
                    }
                }
            }]
        },
    ).execute()
    logger.info("Released DOT %s from quarantine (was row %d)", dot, target_row)
    return True
