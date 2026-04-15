#!/usr/bin/env python3
"""Manually invoke the MDL vendor reply sweep.

Usage:
    python scripts/process_mdl_vendor_replies.py --once

Thin CLI wrapper around ``app.workflows.outreach_reply.run_mdl_vendor_replies``
mirroring the pattern of ``scripts/dispatch_mdl_vendor_outreach.py``. Invoked
on a 5-minute cadence by the Windows Task Scheduler job
``BrokerOps-MDL-Vendor-Dispatcher`` (see ``docs/mdl_vendor_cron_wiring.md``).

The reply sweep scans recent inbound Gmail messages, matches thread IDs
against col J of the MDL Vendor Outreach sheet, classifies each reply, and
stamps col I (status). Col F (Derek's Notes) is never read or written.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run one cycle of the MDL Vendor reply sweep (Nina Weston)."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single sweep and exit (currently the only mode).",
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
    logger = logging.getLogger("scripts.process_mdl_vendor_replies")

    if not args.once:
        logger.warning(
            "No --once flag provided. Scheduled mode is not wired here; "
            "defaulting to a single sweep. Pass --once explicitly to silence "
            "this warning."
        )

    from app.workflows.outreach_reply import run_mdl_vendor_replies

    stats = run_mdl_vendor_replies()

    print("\n" + "=" * 50)
    print("  MDL Vendor Outreach -- Reply Sweep Summary")
    print("=" * 50)
    print(f"  Scanned:         {stats.get('scanned', 0)}")
    print(f"  Matched:         {stats.get('matched', 0)}")
    print(f"  RFQ received:    {stats.get('rfq_received', 0)}")
    print(f"  Replied:         {stats.get('replied', 0)}")
    print(f"  Stalled:         {stats.get('stalled', 0)}")
    print(f"  Awaiting reply:  {stats.get('awaiting_reply', 0)}")
    print(f"  Errors:          {stats.get('errors', 0)}")
    print("=" * 50)

    return 1 if stats.get("errors", 0) > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
