"""
FMCSA Skeleton Backfill — 2026-04-14
=====================================

Backfills HQ location (street/city/state/ZIP) and fleet size for for_hire
carrier rows in the Carrier Database sheet that have phones but no city/state.

Source of truth for skeleton identification:
    Classification (col AF) == 'for_hire' AND (City blank OR State blank)

Data source:
    FMCSA QC Mobile API endpoint /carriers/{dot_number}
    Accessed via app.fmcsa._cached_get which honors FMCSA_API_KEY.

    NOTE: The original task mentioned `get_carrier_basics` but that helper only
    returns crash/inspection stats, NOT HQ location. The /carriers/{dot} base
    endpoint is the correct source for phyStreet/phyCity/phyState/phyZipcode,
    and it uses the same _cached_get infrastructure.

Safety invariants:
    - NEVER overwrite a non-blank cell. Backfill only.
    - NEVER touch Status (col B) or Classification (col AF).
    - Append a marker to Notes (col AE) if FMCSA returns no data.
    - Single batched values.batchUpdate write at the end (no row-by-row writes).
    - 1 second sleep between FMCSA calls.

Usage:
    cd C:/Users/Owner/brokerops-ai
    CLOUDSDK_PYTHON=python PYTHONPATH=. python scripts/fmcsa_skeleton_backfill_20260414.py
"""
from __future__ import annotations

import logging
import sys
import time
from datetime import datetime
from typing import Any

from app.config import get_settings
from app.fmcsa import _BASE_URL, _cached_get
from app.google_auth import get_sheets_service
from app.sheets import read_range

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            "scripts/logs/fmcsa_skeleton_backfill_20260414.log", mode="w"
        ),
    ],
)
log = logging.getLogger("fmcsa_backfill")

# ── Constants ────────────────────────────────────────────────────────────────

TAB = "'Carrier Database'"
HEADER_RANGE = f"{TAB}!A1:AF1"
DATA_RANGE = f"{TAB}!A1:AF200"

# Column indices (0-based within the row)
COL_CARRIER_ID = 0    # A
COL_STATUS = 1        # B   (DO NOT TOUCH)
COL_COMPANY = 2       # C
COL_MC = 3            # D
COL_DOT = 4           # E
COL_ADDRESS = 11      # L
COL_CITY = 12         # M
COL_STATE = 13        # N
COL_ZIP = 14          # O
COL_EQUIPMENT = 15    # P
COL_FLEET = 16        # Q
COL_OUTREACH = 28     # AC
COL_NOTES = 30        # AE
COL_CLASSIFICATION = 31  # AF (DO NOT TOUCH)

MARKER_DATE = "2026-04-14"


def fetch_hq(dot_number: str) -> dict | None:
    """Fetch raw carrier record from FMCSA /carriers/{dot}.

    Returns the 'carrier' sub-dict or None on miss/error.
    """
    try:
        data = _cached_get(f"{_BASE_URL}/{dot_number}")
    except Exception as exc:
        log.warning("DOT %s: FMCSA fetch error: %s", dot_number, exc)
        return None

    content = data.get("content")
    if isinstance(content, list):
        content = content[0] if content else None
    if not isinstance(content, dict):
        return None
    carrier = content.get("carrier", content)
    if not isinstance(carrier, dict) or not carrier:
        return None
    return carrier


def col_letter(idx0: int) -> str:
    """0-based column index to A1 letter. Supports up to ZZ."""
    if idx0 < 26:
        return chr(ord("A") + idx0)
    return chr(ord("A") + (idx0 // 26) - 1) + chr(ord("A") + (idx0 % 26))


def main() -> int:
    settings = get_settings()
    sheet_id = settings.CARRIER_MASTER_SHEET_ID
    if not sheet_id:
        log.error("CARRIER_MASTER_SHEET_ID is empty")
        return 2
    if not settings.FMCSA_API_KEY:
        log.error("FMCSA_API_KEY is empty")
        return 2

    log.info("Reading carrier sheet %s", sheet_id)
    rows = read_range(sheet_id, DATA_RANGE)
    if not rows:
        log.error("Empty sheet")
        return 2

    headers = rows[0]
    data = rows[1:]

    def pad(r: list[str]) -> list[str]:
        return r + [""] * (len(headers) - len(r))

    # ── Identify skeleton rows ───────────────────────────────────────────────
    skeletons: list[dict[str, Any]] = []
    for i, raw in enumerate(data, start=2):  # sheet row = 2..
        r = pad(raw)
        cls = r[COL_CLASSIFICATION].strip().lower()
        if cls != "for_hire":
            continue
        city = r[COL_CITY].strip()
        state = r[COL_STATE].strip()
        if city and state:
            continue
        skeletons.append(
            {
                "row": i,
                "dot": r[COL_DOT].strip(),
                "mc": r[COL_MC].strip(),
                "company": r[COL_COMPANY].strip(),
                "address": r[COL_ADDRESS].strip(),
                "city": city,
                "state": state,
                "zip": r[COL_ZIP].strip(),
                "fleet": r[COL_FLEET].strip(),
                "notes": r[COL_NOTES].strip(),
            }
        )

    log.info("Identified %d skeleton rows (for_hire + missing city/state)", len(skeletons))

    # Sanity: the lane coverage audit expected ~78. Bail if wildly off.
    if not (60 <= len(skeletons) <= 95):
        log.error("Skeleton count %d outside expected 60..95 — STOP", len(skeletons))
        return 3

    # ── Iterate FMCSA and accumulate updates ─────────────────────────────────
    updates: list[dict[str, Any]] = []  # values.batchUpdate "data" entries
    n_backfilled = 0
    n_no_data = 0
    n_errored = 0
    n_any_field_written = 0
    name_mismatch_review: list[dict[str, str]] = []
    inactive_review: list[dict[str, str]] = []

    for idx, sk in enumerate(skeletons, start=1):
        dot = sk["dot"]
        row = sk["row"]
        log.info(
            "[%d/%d] row %d DOT %s '%s' before city='%s' state='%s'",
            idx, len(skeletons), row, dot, sk["company"], sk["city"], sk["state"],
        )

        if not dot:
            log.warning("row %d has no DOT, skipping", row)
            n_errored += 1
            continue

        try:
            carrier = fetch_hq(dot)
        except Exception as exc:
            log.exception("row %d DOT %s unexpected error: %s", row, dot, exc)
            n_errored += 1
            time.sleep(1.0)
            continue

        if carrier is None:
            # Mark with a note and move on
            new_note_fragment = f"[FMCSA BACKFILL {MARKER_DATE}] no data returned"
            merged_note = (
                f"{sk['notes']} | {new_note_fragment}" if sk["notes"] else new_note_fragment
            )
            updates.append(
                {
                    "range": f"{TAB}!{col_letter(COL_NOTES)}{row}",
                    "values": [[merged_note]],
                }
            )
            log.info("row %d DOT %s: NO DATA — note appended", row, dot)
            n_no_data += 1
            time.sleep(1.0)
            continue

        # Extract fields
        fm_street = (carrier.get("phyStreet") or "").strip()
        fm_city = (carrier.get("phyCity") or "").strip()
        fm_state = (carrier.get("phyState") or "").strip()
        fm_zip = str(carrier.get("phyZipcode") or "").strip()
        fm_units = carrier.get("totalPowerUnits")
        fm_legal = (carrier.get("legalName") or "").strip()
        fm_dba = (carrier.get("dbaName") or "").strip()
        fm_allowed = (carrier.get("allowedToOperate") or "").strip().upper()
        fm_status_code = (carrier.get("statusCode") or "").strip().upper()

        wrote_any = False

        # Col L: Address — only if blank
        if not sk["address"] and fm_street:
            updates.append(
                {
                    "range": f"{TAB}!{col_letter(COL_ADDRESS)}{row}",
                    "values": [[fm_street]],
                }
            )
            wrote_any = True

        # Col M: City — only if blank
        if not sk["city"] and fm_city:
            updates.append(
                {
                    "range": f"{TAB}!{col_letter(COL_CITY)}{row}",
                    "values": [[fm_city]],
                }
            )
            wrote_any = True

        # Col N: State — only if blank
        if not sk["state"] and fm_state:
            updates.append(
                {
                    "range": f"{TAB}!{col_letter(COL_STATE)}{row}",
                    "values": [[fm_state]],
                }
            )
            wrote_any = True

        # Col O: ZIP — only if blank
        if not sk["zip"] and fm_zip:
            updates.append(
                {
                    "range": f"{TAB}!{col_letter(COL_ZIP)}{row}",
                    "values": [[fm_zip]],
                }
            )
            wrote_any = True

        # Col Q: Fleet Size — only if blank and FMCSA returned a positive int
        if not sk["fleet"] and fm_units is not None:
            try:
                units_int = int(fm_units)
            except (TypeError, ValueError):
                units_int = 0
            if units_int > 0:
                updates.append(
                    {
                        "range": f"{TAB}!{col_letter(COL_FLEET)}{row}",
                        "values": [[units_int]],
                    }
                )
                wrote_any = True

        if wrote_any:
            n_backfilled += 1
            n_any_field_written += 1

        # Review flags (logged only, no sheet writes)
        sheet_name = sk["company"].strip().upper()
        fm_name_compare = (fm_dba or fm_legal).upper()
        if (
            sheet_name
            and fm_name_compare
            and sheet_name != fm_name_compare
            and sheet_name not in fm_name_compare
            and fm_name_compare not in sheet_name
        ):
            name_mismatch_review.append(
                {
                    "row": row,
                    "dot": dot,
                    "sheet_name": sk["company"],
                    "fmcsa_legal": fm_legal,
                    "fmcsa_dba": fm_dba,
                }
            )

        if fm_allowed == "N" or fm_status_code in ("I", "N"):
            inactive_review.append(
                {
                    "row": row,
                    "dot": dot,
                    "company": fm_legal or sk["company"],
                    "allowed": fm_allowed,
                    "status": fm_status_code,
                }
            )

        log.info(
            "row %d DOT %s: after city='%s' state='%s' zip='%s' "
            "units=%s | outcome=%s",
            row, dot, fm_city, fm_state, fm_zip, fm_units,
            "backfilled" if wrote_any else "skipped(already-populated)",
        )

        time.sleep(1.0)  # Rate limit

    # ── Single batched write ─────────────────────────────────────────────────
    if updates:
        log.info("Executing batched write: %d cell updates", len(updates))
        svc = get_sheets_service().spreadsheets()
        body = {"valueInputOption": "USER_ENTERED", "data": updates}
        resp = svc.values().batchUpdate(spreadsheetId=sheet_id, body=body).execute()
        log.info(
            "batchUpdate complete: totalUpdatedCells=%s totalUpdatedRanges=%s",
            resp.get("totalUpdatedCells"),
            resp.get("totalUpdatedRanges"),
        )
    else:
        log.info("No updates to write")

    # ── Report ───────────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    print("FMCSA SKELETON BACKFILL REPORT")
    print(f"Date: {datetime.now().isoformat()}")
    print("=" * 70)
    print(f"Skeleton rows identified:     {len(skeletons)}")
    print(f"Successfully backfilled:      {n_backfilled}")
    print(f"FMCSA returned no data:       {n_no_data}")
    print(f"Errored:                      {n_errored}")
    print(f"Total cell updates written:   {len(updates)}")
    print()

    if name_mismatch_review:
        print(f"Name mismatches to review ({len(name_mismatch_review)}):")
        for x in name_mismatch_review[:20]:
            print(
                f"  row {x['row']} DOT {x['dot']}: "
                f"sheet='{x['sheet_name']}' vs FMCSA legal='{x['fmcsa_legal']}' dba='{x['fmcsa_dba']}'"
            )
        if len(name_mismatch_review) > 20:
            print(f"  ... {len(name_mismatch_review) - 20} more")
        print()

    if inactive_review:
        print(f"Inactive/not-allowed carriers ({len(inactive_review)}):")
        for x in inactive_review:
            print(
                f"  row {x['row']} DOT {x['dot']} '{x['company']}': "
                f"allowed={x['allowed']} status={x['status']}"
            )
        print()

    print("NOTE: classification re-run may be worth doing on backfilled rows")
    print("      (some hidden private-fleet names may now be visible). Sasha decides.")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(main())
