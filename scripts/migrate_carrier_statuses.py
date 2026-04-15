"""
Carrier_Master "Outreach Status" canonical-value migration.

Fixes stale lowercase sentinel values in the Carrier Database sheet so that
app/workflows/carrier_outreach.py will actually pick carriers up. The
outreach scheduler filters on canonical enum values and currently
no-ops against the 128-row sheet because 49 rows are lowercase
("queued" / "sent" / "responded") and 79 rows are "PROSPECT" (pre-promotion
state, intentionally excluded from outreach — see scripts/prospect_carriers.py
promote_prospects()).

Mapping (derived from reading the actual code, not guessed):

    "PROSPECT"   -> PROSPECT       (no change — canonical pre-outreach state)
    "queued"     -> NEW            (ready for initial outreach)
    "sent"       -> OUTREACH_SENT  (initial outreach already sent)
    "responded"  -> OUTREACH_INTERESTED  (reply received, positive default)

Justification references:
  - NEW filter in carrier_outreach.py:388 (status == "NEW" -> initial outreach)
  - OUTREACH_SENT set at carrier_outreach.py:247 and filtered at :393
  - OUTREACH_INTERESTED set at outreach_reply.py:217 (canonical "they said yes")
  - PROSPECT -> NEW is an EXPLICIT gate in scripts/prospect_carriers.py:525
    (promote_prospects). Do NOT auto-promote here; that's a separate op.

Usage:

    # Dry-run (default) — prints every change and a summary, writes nothing.
    python scripts/migrate_carrier_statuses.py

    # Actually write the changes.
    python scripts/migrate_carrier_statuses.py --apply

    # Bypass the "expected 128 rows" paranoia guard (only if sheet grew).
    python scripts/migrate_carrier_statuses.py --apply --force

Safety:
  - Dry-run by default. --apply required to write.
  - Exits nonzero if ANY row has a value not in the known mapping (so Derek
    sees unexpected values, never silently skips).
  - Exits nonzero if row count != 128 without --force when --apply is set.
  - Idempotent: rows already on canonical values are skipped silently.
  - Every write is logged (JSONL) to scripts/logs/migrate_carrier_statuses_<ts>.jsonl
  - One cell at a time via write_range. No batching. 128 rows is tiny.

"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

# Make the repo root importable when run as `python scripts/...`
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.config import get_settings                              # noqa: E402
from app.sheets import (                                          # noqa: E402
    read_range,
    write_range,
    CARRIER_DB_TAB,
    CARRIER_DB_RANGE,
)

logger = logging.getLogger("brokerops.migrate_carrier_statuses")

STATUS_COLUMN_HEADER = "Outreach Status"
EXPECTED_ROW_COUNT = 128  # per last audit pass

# Canonical states recognized by the code (carrier_outreach.py, outreach_reply.py,
# onboarding.py, prospect_carriers.py). Any value in this set is already correct
# and will be skipped (idempotency).
CANONICAL_STATES = {
    "PROSPECT",
    "NEW",
    "OUTREACH_SENT",
    "FOLLOW_UP_1",
    "FOLLOW_UP_2",
    "OUTREACH_INTERESTED",
    "OUTREACH_DECLINED",
    "UNRESPONSIVE",
    "COI_RECEIVED",
    "",  # blank cells — treat as already-canonical no-op (don't touch)
}

# Stale values we know how to rewrite.
MIGRATION_MAP = {
    "queued": "NEW",
    "sent": "OUTREACH_SENT",
    "responded": "OUTREACH_INTERESTED",
}


def _col_letter(idx: int) -> str:
    """Convert 0-based column index to A1 letter(s). Handles up to ZZ."""
    if idx < 26:
        return chr(ord("A") + idx)
    first = chr(ord("A") + (idx // 26) - 1)
    second = chr(ord("A") + (idx % 26))
    return first + second


def _ensure_logs_dir() -> Path:
    logs_dir = Path(__file__).resolve().parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Migrate Carrier_Master Outreach Status values to canonical enum.")
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually write changes. Default is dry-run (read-only).",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Bypass the 'expected row count' paranoia guard (only needed if the sheet has grown).",
    )
    return p.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    args = _parse_args()
    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info("Starting Carrier_Master status migration in %s mode.", mode)

    settings = get_settings()
    sheet_id = settings.CARRIER_MASTER_SHEET_ID

    # Read the whole sheet
    rows = read_range(sheet_id, CARRIER_DB_RANGE)
    if not rows:
        logger.error("Carrier Database is empty — aborting.")
        return 2

    headers = rows[0]
    data_rows = rows[1:]
    data_row_count = len(data_rows)
    logger.info("Read %d header columns, %d data rows.", len(headers), data_row_count)

    # Locate status column
    if STATUS_COLUMN_HEADER not in headers:
        logger.error(
            "Column '%s' not found in sheet headers: %s",
            STATUS_COLUMN_HEADER, headers,
        )
        return 2
    status_idx = headers.index(STATUS_COLUMN_HEADER)
    status_col_letter = _col_letter(status_idx)
    logger.info(
        "'%s' is column %s (index %d).",
        STATUS_COLUMN_HEADER, status_col_letter, status_idx,
    )

    # Locate MC Number column for audit logging
    mc_idx = headers.index("MC Number") if "MC Number" in headers else None

    # Paranoia guard: row count sanity check
    if data_row_count != EXPECTED_ROW_COUNT:
        msg = (
            f"Row count mismatch: expected {EXPECTED_ROW_COUNT}, got {data_row_count}. "
            "Another process may have mutated the sheet mid-run."
        )
        if args.apply and not args.force:
            logger.error("%s — refusing to --apply without --force.", msg)
            return 3
        logger.warning("%s — continuing (%s).", msg, "forced" if args.force else "dry-run")

    # Tally current state and plan changes
    current_counts: Counter[str] = Counter()
    planned_new_counts: Counter[str] = Counter()
    changes: list[dict] = []  # each: {sheet_row, mc, old, new}
    unknown_rows: list[tuple[int, str, str]] = []  # (sheet_row, mc, value)

    for offset, row in enumerate(data_rows):
        sheet_row = offset + 2  # 1-indexed, header is row 1
        padded = row + [""] * (len(headers) - len(row))
        old_raw = padded[status_idx]
        old = (old_raw or "").strip()
        mc = padded[mc_idx] if mc_idx is not None else ""

        current_counts[old] += 1

        if old in CANONICAL_STATES:
            # Already canonical (including "" blank). Skip silently for idempotency.
            planned_new_counts[old] += 1
            continue

        if old in MIGRATION_MAP:
            new = MIGRATION_MAP[old]
            changes.append({
                "sheet_row": sheet_row,
                "mc": mc,
                "old": old,
                "new": new,
            })
            planned_new_counts[new] += 1
            continue

        # Unknown value — do NOT migrate silently.
        unknown_rows.append((sheet_row, mc, old))
        planned_new_counts[old] += 1  # leave as-is in count

    # Show current state
    logger.info("── Current 'Outreach Status' counts ──")
    for val, cnt in sorted(current_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        display = val if val else "(blank)"
        logger.info("  %-22s %d", display, cnt)

    # Unknown values = hard stop
    if unknown_rows:
        logger.error("── UNKNOWN values found (not in CANONICAL or MIGRATION_MAP) ──")
        for sheet_row, mc, val in unknown_rows:
            logger.error("  row=%d mc=%s value=%r", sheet_row, mc, val)
        logger.error(
            "Refusing to proceed: %d row(s) have unknown status values. "
            "Update MIGRATION_MAP or CANONICAL_STATES in this script after review.",
            len(unknown_rows),
        )
        return 4

    # Show planned changes
    logger.info("── Planned changes: %d row(s) ──", len(changes))
    if not changes:
        logger.info("Nothing to migrate. Sheet is already in canonical shape.")
        logger.info("Hypothetical post-migration counts = current counts (no-op).")
        return 0

    for ch in changes:
        logger.info(
            "  row=%-4d mc=%-10s  %-12s -> %s",
            ch["sheet_row"], ch["mc"] or "(none)", ch["old"], ch["new"],
        )

    # Hypothetical / post counts
    logger.info("── %s post-migration counts ──", "Projected" if not args.apply else "Final")
    for val, cnt in sorted(planned_new_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        display = val if val else "(blank)"
        logger.info("  %-22s %d", display, cnt)

    # Dry-run: stop here
    if not args.apply:
        logger.info(
            "DRY-RUN complete. %d row(s) would be updated. "
            "Re-run with --apply to write.",
            len(changes),
        )
        return 0

    # ── APPLY MODE ──
    logs_dir = _ensure_logs_dir()
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    log_path = logs_dir / f"migrate_carrier_statuses_{ts}.jsonl"
    logger.info("Writing audit trail to %s", log_path)

    written = 0
    errors = 0
    with log_path.open("w", encoding="utf-8") as log_fp:
        for ch in changes:
            cell = f"{CARRIER_DB_TAB}!{status_col_letter}{ch['sheet_row']}"
            try:
                write_range(sheet_id, cell, [[ch["new"]]])
                entry = {
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "row": ch["sheet_row"],
                    "mc": ch["mc"],
                    "old_status": ch["old"],
                    "new_status": ch["new"],
                    "cell": cell,
                }
                log_fp.write(json.dumps(entry) + "\n")
                log_fp.flush()
                written += 1
                logger.info(
                    "Wrote row=%d mc=%s %s -> %s",
                    ch["sheet_row"], ch["mc"] or "(none)", ch["old"], ch["new"],
                )
            except Exception as e:
                errors += 1
                logger.exception(
                    "Write FAILED for row=%d mc=%s: %s",
                    ch["sheet_row"], ch["mc"], e,
                )

    logger.info(
        "APPLY complete. wrote=%d errors=%d total_planned=%d",
        written, errors, len(changes),
    )
    logger.info("Audit log: %s", log_path)
    return 0 if errors == 0 else 5


if __name__ == "__main__":
    sys.exit(main())
