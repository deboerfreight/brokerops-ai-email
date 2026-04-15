"""
CLI wrapper for the Playwright-driven carrier enrichment workflow.

Usage:
    PYTHONPATH=. python scripts/enrich_carriers_playwright.py --dry-run --limit 10
    PYTHONPATH=. python scripts/enrich_carriers_playwright.py --limit 10
    PYTHONPATH=. python scripts/enrich_carriers_playwright.py --dots 2888357,947398
    PYTHONPATH=. python scripts/enrich_carriers_playwright.py           # full run
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

from app.workflows.enrich_carriers_playwright import run_enrichment


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-5s %(name)s | %(message)s",
    )
    # Quiet down noisy libs
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("googleapiclient").setLevel(logging.WARNING)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Playwright carrier enrichment (for_hire, blank-email)",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Discover + extract; do not write to sheet.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Process at most N eligible carriers.")
    parser.add_argument("--dots", type=str, default=None,
                        help="Comma-separated DOT numbers to restrict to.")
    parser.add_argument("--interval", type=float, default=10.0,
                        help="Min seconds between requests to the same host (default 10s = 6/min).")
    parser.add_argument("--checkpoint", type=str,
                        default="scripts/.checkpoints/enrich_playwright_20260414.json",
                        help="Checkpoint JSON path (resumable).")
    parser.add_argument("--reset-checkpoint", action="store_true",
                        help="Delete the checkpoint before running.")
    parser.add_argument("--log-json", type=str, default=None,
                        help="Write the summary dict to this path as JSON.")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    _configure_logging(args.verbose)

    if args.reset_checkpoint and os.path.exists(args.checkpoint):
        os.remove(args.checkpoint)
        logging.info("Deleted checkpoint %s", args.checkpoint)

    dots = None
    if args.dots:
        dots = {d.strip() for d in args.dots.split(",") if d.strip()}

    summary = run_enrichment(
        limit=args.limit,
        dots=dots,
        dry_run=args.dry_run,
        checkpoint_path=args.checkpoint,
        per_host_interval_s=args.interval,
    )

    # Print a terse result to stdout
    print("\n==== ENRICHMENT SUMMARY ====")
    print(f"  elapsed:          {summary['elapsed_s']:.1f}s")
    print(f"  carriers loaded:  {summary['eligible_loaded']}")
    print(f"  results:          {summary['results_count']}")
    print(f"  emails found:     {summary['emails_found_any']}")
    print(f"  emails picked:    {summary['emails_picked']}")
    print(f"  writes queued:    {summary['writes_queued']}")
    print(f"  writes committed: {summary['writes_committed']}")
    print(f"  dry run:          {summary['dry_run']}")
    print()
    for c in summary["per_carrier"]:
        print(
            f"  DOT {c['dot']:>8}  {c['name'][:32]:<32} {c['state'] or '?? ':>2}  "
            f"pages={c['pages_fetched']} blk={c['pages_blocked']} err={c['pages_errored']} "
            f"cand={len(c['emails_found'])}  pick={c['picked_email'] or '-':<32} "
            f"src={c['picked_source'] or '-'}"
        )

    if args.log_json:
        os.makedirs(os.path.dirname(args.log_json) or ".", exist_ok=True)
        with open(args.log_json, "w", encoding="utf-8") as fp:
            json.dump(summary, fp, indent=2, default=str)
        print(f"\n  summary JSON: {args.log_json}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
