"""BrokerOps AI – Bulk re-vet the existing Carrier Database.

`sweep_carrier_database()` reads every row from the main tab, runs
`vet_complete()`, and updates col AG. With `re_fetch_fmcsa=True`, fetches
fresh FMCSA data first (used by the rebuild migration and after rule changes).

Rate limited at 1 req/sec to FMCSA. Idempotent.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Optional

from app.config import get_settings
from app.google_auth import get_sheets_service
from app.vetting.data_sync import fetch_fresh_fmcsa
from app.vetting.gate import (
    vet_complete,
    PASS_BASIC,
)
from app.vetting.li_insurance_lookup import get_insurance
from app.vetting.quarantine import (
    QUARANTINE_TAB,
    ensure_quarantine_tab_exists,
    append_to_quarantine,
    release_from_quarantine,
    get_quarantine_rows,
)


def _overlay_li_on_row(row: dict) -> dict:
    """L&I overlay for sheet-header rows (spaces, not underscores).

    Quarantine rows use spaced headers ("Insurance Liability", "Insurance
    Cargo"); fetch_fresh_fmcsa writes underscored keys. vet_complete()
    reads both via _get_first(). Writes both spellings so both code paths
    see the refreshed value.

    Overwrite policy: FMCSA L&I bulk data is the authoritative current
    snapshot. Legacy quarantine rows often store BIPD in thousands (e.g.
    "1000" meaning $1M), which the dollar-denominated gate misreads as
    $1,000. Always prefer L&I for BIPD when an L&I record exists. For
    cargo, only overwrite blanks — federal cargo filings are sparse
    (HHG-only) so L&I will usually be 0 and we don't want to clobber a
    real contractually-verified value from sheet.
    """
    dot = (row.get("DOT Number") or row.get("DOT_Number") or "").strip()
    if not dot:
        return row
    li = get_insurance(dot)
    if not li:
        return row

    def _blank(k):
        v = row.get(k)
        if v in (None, ""):
            return True
        try:
            return int(str(v).replace("$", "").replace(",", "")) == 0
        except (ValueError, TypeError):
            return False

    # BIPD: always prefer authoritative L&I bulk value (fixes thousands-encoding
    # bug in legacy sheet rows and ensures fresh insurer data).
    if li.bipd_liability > 0:
        row["Insurance Liability"] = li.bipd_liability
        row["Insurance_Liability"] = li.bipd_liability

    # Cargo: fill only blanks (federal cargo is rarely filed; don't clobber).
    if _blank("Insurance Cargo") and _blank("Insurance_Cargo") and li.cargo > 0:
        row["Insurance Cargo"] = li.cargo
        row["Insurance_Cargo"] = li.cargo

    if li.insurer_name:
        row["Insurance Company"] = li.insurer_name
        row["Insurance_Company"] = li.insurer_name
    return row

logger = logging.getLogger("brokerops.vetting.sweep")

MAIN_TAB = "Carrier Database"
FMCSA_RATE_LIMIT_SEC = 1.0


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


def _write_vetting_status_column(svc, sheet_id: str, statuses: list[str]) -> None:
    """Write the Vetting Status column (AG) for all data rows in one call."""
    if not statuses:
        return
    last_row = len(statuses) + 1  # header row + N data rows
    body = {"values": [[s] for s in statuses]}
    svc.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"{MAIN_TAB}!AG2:AG{last_row}",
        valueInputOption="USER_ENTERED",
        body=body,
    ).execute()


def sweep_carrier_database(re_fetch_fmcsa: bool = False) -> dict:
    """Re-vet every row in the Carrier Database tab.

    If re_fetch_fmcsa=True, fetch fresh FMCSA data for every row before vetting
    (slow — ~1 req/sec). Use after rule changes or for the initial migration.

    Notes is preserved (col AE) and Classification (col AF) is preserved.

    Returns summary dict.
    """
    started = time.time()
    sheet_id = get_settings().CARRIER_MASTER_SHEET_ID
    svc = get_sheets_service()
    ensure_quarantine_tab_exists(svc, sheet_id)

    header, data = _read_main(svc, sheet_id)
    if not header:
        return {"total": 0, "error": "empty sheet"}

    summary = {
        "total": len(data),
        "pass_basic": 0,
        "fails_by_status": {},
        "moved_to_quarantine": 0,
        "rescued_from_quarantine": 0,
        "fmcsa_refetched": 0,
        "fmcsa_errors": 0,
        "duration_sec": 0,
    }

    statuses: list[str] = []
    quarantine_targets: list[tuple[int, dict]] = []  # (1-indexed row #, carrier dict)

    for idx, raw in enumerate(data):
        row_num = idx + 2  # 1-indexed (header at row 1)
        carrier = _row_to_dict(header, raw)

        if re_fetch_fmcsa:
            dot = (carrier.get("DOT Number") or "").strip()
            if dot:
                fresh = fetch_fresh_fmcsa(dot)
                if fresh:
                    summary["fmcsa_refetched"] += 1
                    # Merge fresh data into carrier dict (don't overwrite Notes/Classification)
                    for k, v in fresh.items():
                        if k in ("Notes", "Internal_Notes", "Classification"):
                            continue
                        carrier[k] = v
                else:
                    summary["fmcsa_errors"] += 1
                time.sleep(FMCSA_RATE_LIMIT_SEC)

        result = vet_complete(carrier)
        statuses.append(result.status)
        if result.status == PASS_BASIC:
            summary["pass_basic"] += 1
        else:
            summary["fails_by_status"][result.status] = (
                summary["fails_by_status"].get(result.status, 0) + 1
            )
            quarantine_targets.append((row_num, carrier))

    _write_vetting_status_column(svc, sheet_id, statuses)
    summary["duration_sec"] = round(time.time() - started, 2)
    return summary


def sweep_quarantine() -> dict:
    """Re-vet every row in the Carrier Quarantine tab.

    For any row that now passes vet_complete, release back to Carrier Database.
    """
    started = time.time()
    sheet_id = get_settings().CARRIER_MASTER_SHEET_ID
    svc = get_sheets_service()
    ensure_quarantine_tab_exists(svc, sheet_id)

    rows = get_quarantine_rows(svc, sheet_id)
    summary = {
        "total": len(rows),
        "still_failing": 0,
        "released": 0,
        "fails_by_status": {},
        "duration_sec": 0,
    }

    released_dots: list[str] = []
    for row in rows:
        _overlay_li_on_row(row)
        result = vet_complete(row)
        if result.status == PASS_BASIC:
            dot = (row.get("DOT Number") or row.get("DOT_Number") or "").strip()
            if dot:
                released_dots.append(dot)
        else:
            summary["still_failing"] += 1
            summary["fails_by_status"][result.status] = (
                summary["fails_by_status"].get(result.status, 0) + 1
            )

    for dot in released_dots:
        if release_from_quarantine(svc, sheet_id, dot):
            summary["released"] += 1

    summary["duration_sec"] = round(time.time() - started, 2)
    return summary
