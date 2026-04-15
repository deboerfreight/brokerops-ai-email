#!/usr/bin/env python3
"""Manually invoke the MDL vendor outreach dispatcher.

Usage:
    python scripts/dispatch_mdl_vendor_outreach.py --once
    python scripts/dispatch_mdl_vendor_outreach.py --once --dry-run

The dispatcher is a cron-invoked (eventually) polling loop that reads
the MDL Vendor Outreach sheet, finds rows with K=TRUE and H empty, and
sends Nina's first-touch template via Gmail. See
app/workflows/mdl_vendor_outreach_dispatcher.py for the full logic.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run one cycle of the MDL Vendor Outreach dispatcher (Nina Weston)."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single dispatch cycle and exit (currently the only mode).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be sent without actually sending or writing to the sheet.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(name)-45s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("scripts.dispatch_mdl_vendor_outreach")

    if not args.once:
        logger.warning(
            "No --once flag provided. Scheduled mode is not wired yet; "
            "defaulting to a single cycle. Pass --once explicitly to silence "
            "this warning."
        )

    if args.dry_run:
        logger.info("=== DRY RUN MODE — no emails will be sent ===")

    from app.workflows.mdl_vendor_outreach_dispatcher import run

    stats = run(dry_run=args.dry_run)

    print("\n" + "=" * 50)
    print("  MDL Vendor Outreach — Dispatcher Summary")
    print("=" * 50)
    print(f"  Rows scanned:          {stats['rows_scanned']}")
    print(f"  Sent:                  {stats['sent']}")
    print(f"  Skipped (unchecked):   {stats['skipped_unchecked']}")
    print(f"  Skipped (already sent):{stats['skipped_already_sent']}")
    print(f"  Validation failed:     {stats['validation_failed']}")
    print(f"  Send failed:           {stats['send_failed']}")
    print("=" * 50)

    return 1 if stats["send_failed"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
