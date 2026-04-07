#!/usr/bin/env python3
"""Send carrier outreach emails manually.

Usage:
    # Full run (sends real emails):
    python -m scripts.send_outreach

    # Dry run (log only, no emails sent):
    python -m scripts.send_outreach --dry-run

    # Limit batch size:
    python -m scripts.send_outreach --limit 5

    # Custom delay between sends (seconds):
    python -m scripts.send_outreach --delay 15
"""
from __future__ import annotations

import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    parser = argparse.ArgumentParser(
        description="Run carrier outreach cycle (Sofia Reyes persona)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would happen without sending emails or updating sheets.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of emails to send in this run (default: 20).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=10.0,
        help="Seconds to wait between sends (default: 10).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    args = parser.parse_args()

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(name)-40s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("scripts.send_outreach")

    if args.dry_run:
        logger.info("=== DRY RUN MODE — no emails will be sent ===")

    from app.workflows.carrier_outreach import run

    stats = run(
        dry_run=args.dry_run,
        batch_limit=args.limit,
        send_delay=args.delay,
    )

    # Print summary
    print("\n" + "=" * 50)
    print("  Carrier Outreach Summary")
    print("=" * 50)
    print(f"  Initial outreach sent:   {stats['initial_sent']}")
    print(f"  Follow-up #1 sent:       {stats['followup_1_sent']}")
    print(f"  Follow-up #2 sent:       {stats['followup_2_sent']}")
    print(f"  Marked unresponsive:     {stats['marked_unresponsive']}")
    print(f"  Phone-only skipped:      {stats['phone_only_skipped']}")
    print(f"  Errors:                  {stats['errors']}")
    print("=" * 50)

    if stats["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
