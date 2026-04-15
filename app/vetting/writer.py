"""BrokerOps AI – Pre-write & post-write enforcement for the Carrier Database.

The architectural guarantee: every write to the Carrier Database tab goes
through `validate_before_write` (split passes from quarantines) and
`write_validated` (write + post-write verify).
"""
from __future__ import annotations

import logging
from typing import Any, List, Optional, Tuple

from app.vetting.gate import vet_complete, VettingResult, PASS_BASIC
from app.vetting.quarantine import append_to_quarantine

logger = logging.getLogger("brokerops.vetting.writer")


def validate_before_write(rows: List[dict]) -> Tuple[
    List[dict],
    List[Tuple[dict, VettingResult]],
]:
    """Split incoming carrier rows into (passes, quarantines).

    For each row:
      - Run vet_complete()
      - If status == pass_basic: include in passes
      - Else: include in quarantines as (row, result)

    Note: this function does NOT call FMCSA. Callers that want a fresh re-fetch
    before validation should wire it themselves (see `sweep.py` for the pattern).
    """
    passes: List[dict] = []
    quarantines: List[Tuple[dict, VettingResult]] = []
    for row in rows:
        result = vet_complete(row)
        # Stamp the row with the new vetting status so downstream writes
        # include col AG correctly.
        enriched = dict(row)
        enriched["Vetting Status"] = result.status
        enriched["Vetting_Status"] = result.status
        if result.status == PASS_BASIC:
            passes.append(enriched)
        else:
            quarantines.append((enriched, result))
    return passes, quarantines


def write_validated(
    rows: List[dict],
    tab: str = "Carrier Database",
    svc=None,
    spreadsheet_id: Optional[str] = None,
) -> dict:
    """End-to-end validated write.

    Splits via validate_before_write, writes passes via the standard
    `app.sheets.insert_carrier`, sends quarantines via append_to_quarantine,
    then performs a post-write verify by re-running vet_complete on each
    written row.

    Returns:
        {
          "written": int,
          "quarantined": int,
          "post_verify_failed": int,
          "errors": [str, ...],
        }
    """
    # Lazy imports to avoid circular dependencies (sheets.py imports from
    # app.vetting.gate via the back-compat shim).
    from app.sheets import insert_carrier
    from app.google_auth import get_sheets_service
    from app.config import get_settings

    summary = {
        "written": 0,
        "quarantined": 0,
        "post_verify_failed": 0,
        "errors": [],
    }

    if svc is None:
        svc = get_sheets_service()
    if spreadsheet_id is None:
        spreadsheet_id = get_settings().CARRIER_MASTER_SHEET_ID

    passes, quarantines = validate_before_write(rows)

    # Write passes via the sheet helper
    written_dots: List[str] = []
    for row in passes:
        try:
            insert_carrier(row)
            summary["written"] += 1
            dot = (
                row.get("DOT_Number")
                or row.get("DOT Number")
                or ""
            )
            if dot:
                written_dots.append(str(dot).strip())
        except Exception as exc:
            summary["errors"].append(f"insert_carrier failed: {exc}")
            logger.error("insert_carrier failed: %s", exc, exc_info=True)

    # Quarantine the rest
    for row, result in quarantines:
        try:
            append_to_quarantine(svc, spreadsheet_id, row, result)
            summary["quarantined"] += 1
        except Exception as exc:
            summary["errors"].append(f"append_to_quarantine failed: {exc}")
            logger.error("append_to_quarantine failed: %s", exc, exc_info=True)

    # Post-write verify — read back any rows we just inserted and re-run
    # vet_complete. If anything that just passed now fails, log loudly and
    # quarantine it (defense in depth: catches races/bugs).
    if written_dots:
        try:
            from app.sheets import get_all_carriers
            current = get_all_carriers()
            by_dot = {}
            for c in current:
                d = (
                    c.get("DOT Number")
                    or c.get("DOT_Number")
                    or ""
                )
                if d:
                    by_dot[str(d).strip()] = c
            for dot in written_dots:
                live = by_dot.get(dot)
                if live is None:
                    summary["errors"].append(
                        f"post-verify: DOT {dot} not found in main tab after write"
                    )
                    summary["post_verify_failed"] += 1
                    logger.error("Post-write verify FAILED: DOT %s missing", dot)
                    continue
                result = vet_complete(live)
                if result.status != PASS_BASIC:
                    summary["post_verify_failed"] += 1
                    summary["errors"].append(
                        f"post-verify: DOT {dot} now {result.status}"
                    )
                    logger.error(
                        "Post-write verify FAILED for DOT %s: %s — %s. Quarantining.",
                        dot, result.status, result.reason,
                    )
                    try:
                        append_to_quarantine(svc, spreadsheet_id, live, result)
                    except Exception as exc:
                        summary["errors"].append(
                            f"post-verify quarantine of DOT {dot} failed: {exc}"
                        )
        except Exception as exc:
            summary["errors"].append(f"post-write verify pass failed: {exc}")
            logger.error("post-write verify pass failed: %s", exc, exc_info=True)

    logger.info(
        "write_validated complete: written=%d, quarantined=%d, post_verify_failed=%d, errors=%d",
        summary["written"], summary["quarantined"],
        summary["post_verify_failed"], len(summary["errors"]),
    )
    return summary
