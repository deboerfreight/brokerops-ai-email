"""
Main tab cleanup — quarantine passenger carriers and private-fleet distributors.

Run from project root:
    PYTHONPATH=. python scripts/main_tab_cleanup_20260414.py [--dry-run]

Logic:
  1. Read Carrier Database (main tab) rows.
  2. For every row, fetch FMCSA /carriers/{dot} (1 req/sec).
     - If `isPassengerCarrier == "Y"` => quarantine as fail_passenger_only
  3. Apply name-pattern matching for high-confidence private-fleet distributors.
     - Quarantine as fail_private_fleet_confirmed
  4. Ambiguous rows are NOT moved; they're flagged in the log only.
  5. Row deletion is done in reverse row order to avoid index shifting.

Idempotent. Safe to re-run. Existing quarantine entries are updated in place.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from app.fmcsa import _BASE_URL, _cached_get
from app.google_auth import get_sheets_service
from app.vetting.gate import VettingResult
from app.vetting.quarantine import (
    QUARANTINE_TAB,
    append_to_quarantine,
    ensure_quarantine_tab_exists,
)

# ── Configuration ──────────────────────────────────────────────────────────

SPREADSHEET_ID = "1B5nzDCisdpG29bI0MjLtD8dIE2rsjdXg3Qncj-E58WE"
MAIN_TAB = "Carrier Database"
MAIN_SHEET_ID = 0  # confirmed via metadata
LOG_PATH = Path("scripts/logs/main_tab_cleanup_20260414.log")
SNAPSHOT_PATH = Path("scripts/logs/main_tab_cleanup_20260414_decisions.json")

FMCSA_RATE_LIMIT_SECONDS = 1.0  # non-negotiable

# ── Decision constants ────────────────────────────────────────────────────

FAIL_PASSENGER_ONLY = "fail_passenger_only"
FAIL_PRIVATE_FLEET = "fail_private_fleet_confirmed"
FAIL_NON_FREIGHT = "fail_non_freight_service"
NEEDS_REVIEW = "needs_review_manual"
KEEP = "keep"

# ── Name-pattern lists ────────────────────────────────────────────────────

# Strict, well-known private-fleet distributor / vertical-integrated brands.
# Only carriers whose Legal Name CONTAINS one of these tokens get auto-quarantined
# as private fleet. List is intentionally short; ambiguous brands go to needs_review.
PRIVATE_FLEET_BRANDS = [
    "KEHE",
    "KE HE",
    "SYSCO",
    "US FOODS",
    "US FOODSERVICE",
    "PEPSICO",
    "PEPSI BOTTLING",
    "COCA-COLA",
    "COCA COLA",
    "WALMART",
    "COSTCO",
    "HOME DEPOT",
    "LOWES",
    "LOWE'S",
    "IMPERIAL DADE",
    "DADE PAPER",
    "UNIFIRST",
    "CINTAS",
    "SOUTHEAST MILK",
    "PRAIRIE FARMS",
    "DEAN FOODS",
    "JELD WEN",
    "JELD-WEN",
    "GEORGIA-PACIFIC",
    "GEORGIA PACIFIC",
    "WEYERHAEUSER",
    "INTERNATIONAL PAPER",
    "MONDELEZ",
    "KRAFT HEINZ",
    "TYSON FOODS",
    "JBS",
    "CARGILL",
    "ADM",
    "ARCHER DANIELS",
    "PERDUE FARMS",
    "CENTRAL GARDEN",
    "PATTERSON COMPANIES",
    "GCP APPLIED",
    "REGAL CHEMICAL",
    "GARANT",
    "REVERE SEED",
    "CORECIVIC",
    "BROOKDALE",
    "MILLIKEN",
    "KLOECKNER METALS",
    "AMERICAN BUILDING SUPPLY",
    "CONTRACTOR SALES",
]

# Name tokens (substring) that, when matched, indicate a non-freight or
# tow/wrecker / passenger / construction-services-only carrier. Used as a
# secondary signal — not auto-quarantine on its own unless `_strict_*` set.
PASSENGER_TOKENS = [
    "TROLLEY", "BUS CO", "TOURS", "TOUR ", " TOUR", "CHARTER", "COACH",
    "TRANSIT", " LIMO", "LIMO ", "SHUTTLE", "PASSENGER", "TAXI", "BUS LINE",
    "BUS LINES", "BUS TOURS", "FAMILY COACHES",
]

# Tow/wrecker carriers — not for-hire freight. They'll never haul a load.
TOW_TOKENS = [
    "WRECKER", "TOWING", " TOW ", "TOW LLC", "TOW INC", "RECOVERY LLC",
    "ROADSIDE", "SALVAGE", "AUTO RECYCLING",
]

# ── Logging ────────────────────────────────────────────────────────────────

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("main_tab_cleanup")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(fh)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(sh)

# ── Helpers ────────────────────────────────────────────────────────────────


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_fmcsa_carrier(dot: str) -> Optional[dict]:
    """Fetch the raw FMCSA /carriers/{dot} carrier object, or None on failure."""
    if not dot:
        return None
    try:
        d = _cached_get(f"{_BASE_URL}/{dot}")
    except Exception as exc:
        logger.warning("FMCSA fetch failed for DOT %s: %s", dot, exc)
        return None
    content = d.get("content", d)
    if isinstance(content, list) and content:
        content = content[0]
    if isinstance(content, dict):
        return content.get("carrier", content)
    return None


def name_matches(name: str, tokens: list[str]) -> Optional[str]:
    """Return the matching token if name contains any of `tokens` (case-insensitive)."""
    if not name:
        return None
    upper = name.upper()
    for t in tokens:
        if t in upper:
            return t
    return None


def classify_row(
    row: dict,
    fmcsa_carrier: Optional[dict],
) -> tuple[str, str, float]:
    """Return (decision, reason, confidence) for a row.

    decision is one of:
        KEEP                              — leave in main tab
        FAIL_PASSENGER_ONLY               — passenger carrier, quarantine
        FAIL_PRIVATE_FLEET                — known private-fleet brand, quarantine
        FAIL_NON_FREIGHT                  — tow/recovery/non-freight, quarantine
        NEEDS_REVIEW                      — flag for Derek, leave in main tab
    """
    sheet_name = (row.get("Company Name") or "").strip()
    fmcsa_legal = (fmcsa_carrier or {}).get("legalName", "") or ""
    fmcsa_dba = (fmcsa_carrier or {}).get("dbaName", "") or ""
    is_pax = (fmcsa_carrier or {}).get("isPassengerCarrier", "")

    # --- Hard rule 1: FMCSA-flagged passenger carrier
    if str(is_pax).upper().startswith("Y"):
        return (
            FAIL_PASSENGER_ONLY,
            f"FMCSA isPassengerCarrier=Y (legal={fmcsa_legal!r}, sheet={sheet_name!r})",
            0.99,
        )

    # --- Hard rule 2: name token match against passenger-operator names
    # (catch the cases where FMCSA didn't return data)
    for nm in (sheet_name, fmcsa_legal, fmcsa_dba):
        tok = name_matches(nm, PASSENGER_TOKENS)
        if tok:
            # Confirm: zero freight equipment AND has a passenger token => quarantine
            eq = (row.get("Equipment Types") or "").strip()
            if not eq or eq.upper() in ("", "NONE"):
                return (
                    FAIL_PASSENGER_ONLY,
                    f"name matches passenger token {tok!r} (name={nm!r}) and no freight equipment",
                    0.85,
                )
            # Has equipment but passenger token — needs review
            return (
                NEEDS_REVIEW,
                f"name matches passenger token {tok!r} but has equipment {eq!r}",
                0.55,
            )

    # --- Hard rule 3: known private-fleet brand
    for nm in (sheet_name, fmcsa_legal, fmcsa_dba):
        tok = name_matches(nm, PRIVATE_FLEET_BRANDS)
        if tok:
            return (
                FAIL_PRIVATE_FLEET,
                f"name matches private-fleet brand {tok!r} (name={nm!r})",
                0.92,
            )

    # --- Hard rule 4: tow / wrecker / salvage operations
    for nm in (sheet_name, fmcsa_legal, fmcsa_dba):
        tok = name_matches(nm, TOW_TOKENS)
        if tok:
            return (
                FAIL_NON_FREIGHT,
                f"name matches tow/wrecker token {tok!r} (name={nm!r})",
                0.90,
            )

    # Default: keep
    return (KEEP, "no flags", 0.0)


# ── Main ────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="classify only, no writes")
    parser.add_argument("--limit", type=int, default=0, help="cap rows fetched (0 = all)")
    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("Main tab cleanup starting (dry_run=%s)", args.dry_run)
    logger.info("=" * 70)

    svc = get_sheets_service()

    # 1. Read main tab
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{MAIN_TAB}!A:AG"
    ).execute()
    rows = resp.get("values", [])
    if not rows:
        logger.error("Main tab empty")
        return
    header = rows[0]
    data_rows: list[dict] = []
    for i, r in enumerate(rows[1:], start=2):
        padded = r + [""] * (len(header) - len(r))
        d = dict(zip(header, padded))
        d["_row"] = i
        data_rows.append(d)
    logger.info("Loaded %d data rows from %s", len(data_rows), MAIN_TAB)

    if args.limit:
        data_rows = data_rows[: args.limit]
        logger.info("Limited to first %d rows for debug", args.limit)

    # 2. Fetch FMCSA + classify each row
    decisions: list[dict] = []
    keep_count = 0
    pax_count = 0
    pf_count = 0
    nf_count = 0
    review_count = 0
    fmcsa_fail_count = 0

    for idx, row in enumerate(data_rows):
        dot = (row.get("DOT Number") or "").strip()
        sheet_name = (row.get("Company Name") or "").strip()

        if not dot:
            decisions.append({
                "row": row["_row"],
                "dot": "",
                "sheet_name": sheet_name,
                "decision": NEEDS_REVIEW,
                "reason": "no DOT number in row",
                "confidence": 0.0,
                "fmcsa_legal": "",
                "is_passenger": "",
            })
            review_count += 1
            continue

        # Rate-limit FMCSA
        if idx > 0:
            time.sleep(FMCSA_RATE_LIMIT_SECONDS)

        fmcsa = fetch_fmcsa_carrier(dot)
        if fmcsa is None:
            fmcsa_fail_count += 1

        decision, reason, conf = classify_row(row, fmcsa)
        rec = {
            "row": row["_row"],
            "dot": dot,
            "sheet_name": sheet_name,
            "fmcsa_legal": (fmcsa or {}).get("legalName", ""),
            "is_passenger": (fmcsa or {}).get("isPassengerCarrier", ""),
            "decision": decision,
            "reason": reason,
            "confidence": conf,
        }
        decisions.append(rec)

        if decision == KEEP:
            keep_count += 1
        elif decision == FAIL_PASSENGER_ONLY:
            pax_count += 1
            logger.info("PAX row=%d dot=%s name=%r reason=%s",
                        row["_row"], dot, sheet_name, reason)
        elif decision == FAIL_PRIVATE_FLEET:
            pf_count += 1
            logger.info("PRIVATE_FLEET row=%d dot=%s name=%r reason=%s",
                        row["_row"], dot, sheet_name, reason)
        elif decision == FAIL_NON_FREIGHT:
            nf_count += 1
            logger.info("NON_FREIGHT row=%d dot=%s name=%r reason=%s",
                        row["_row"], dot, sheet_name, reason)
        else:
            review_count += 1
            logger.info("NEEDS_REVIEW row=%d dot=%s name=%r reason=%s",
                        row["_row"], dot, sheet_name, reason)

        if (idx + 1) % 20 == 0:
            logger.info("Progress: %d/%d rows classified", idx + 1, len(data_rows))

    # 3. Write decisions snapshot
    SNAPSHOT_PATH.write_text(json.dumps(decisions, indent=2), encoding="utf-8")
    logger.info("Wrote decisions snapshot to %s", SNAPSHOT_PATH)
    logger.info(
        "Classification totals: keep=%d pax=%d private_fleet=%d non_freight=%d review=%d fmcsa_fail=%d",
        keep_count, pax_count, pf_count, nf_count, review_count, fmcsa_fail_count,
    )

    if args.dry_run:
        logger.info("[DRY RUN] skipping quarantine writes and row deletes")
        return

    # 4. Move bad rows to quarantine + delete (reverse row order)
    quarantine_targets = [
        d for d in decisions
        if d["decision"] in (FAIL_PASSENGER_ONLY, FAIL_PRIVATE_FLEET, FAIL_NON_FREIGHT)
    ]
    quarantine_targets.sort(key=lambda d: d["row"], reverse=True)

    ensure_quarantine_tab_exists(svc, SPREADSHEET_ID)
    moved = 0
    # Sheets API quota: 60 read req/min/user. Each append_to_quarantine does
    # ~3-4 reads; each delete is 1 write. Budget ~6 reads per move, so cap at
    # ~10 moves/min => sleep 6.5s between iterations to stay safe.
    SHEETS_THROTTLE = 6.5
    for tgt in quarantine_targets:
        # Find the full row dict from data_rows
        full_row = next((r for r in data_rows if r["_row"] == tgt["row"]), None)
        if not full_row:
            logger.warning("Could not find row %d in data_rows", tgt["row"])
            continue

        # Build a VettingResult so append_to_quarantine logs the right reason
        vr = VettingResult(
            passed=False,
            status=tgt["decision"],
            reason=tgt["reason"],
            checked_at=_now_iso(),
        )
        # Retry append with backoff for 429s
        appended = False
        for attempt in range(5):
            try:
                append_to_quarantine(
                    svc,
                    SPREADSHEET_ID,
                    full_row,
                    vr,
                    original_row_number=tgt["row"],
                )
                appended = True
                break
            except Exception as exc:
                msg = str(exc)
                if "429" in msg or "RATE_LIMIT" in msg or "Quota exceeded" in msg:
                    wait = 30 * (attempt + 1)
                    logger.warning(
                        "429 on append for row %d dot %s — sleeping %ds (attempt %d)",
                        tgt["row"], tgt["dot"], wait, attempt + 1,
                    )
                    time.sleep(wait)
                    continue
                logger.error("append_to_quarantine failed for row %d dot %s: %s",
                             tgt["row"], tgt["dot"], exc)
                break
        if not appended:
            continue

        # Delete the row from main tab
        try:
            svc.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={
                    "requests": [{
                        "deleteDimension": {
                            "range": {
                                "sheetId": MAIN_SHEET_ID,
                                "dimension": "ROWS",
                                "startIndex": tgt["row"] - 1,
                                "endIndex": tgt["row"],
                            }
                        }
                    }]
                },
            ).execute()
            moved += 1
            logger.info("MOVED row=%d dot=%s name=%r decision=%s",
                        tgt["row"], tgt["dot"], tgt["sheet_name"], tgt["decision"])
        except Exception as exc:
            logger.error("delete row %d failed: %s", tgt["row"], exc)

        # Throttle next iteration to stay under 60 reads/min
        time.sleep(SHEETS_THROTTLE)

    # 5. Final count
    final_resp = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{MAIN_TAB}!A:A"
    ).execute()
    final_rows = len(final_resp.get("values", [])) - 1
    logger.info(
        "DONE. moved=%d, final main tab rows=%d, needs_review_in_main=%d",
        moved, final_rows, review_count,
    )


if __name__ == "__main__":
    main()
