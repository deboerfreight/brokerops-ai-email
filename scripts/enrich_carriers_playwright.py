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

from app.workflows.enrich_carriers_playwright import run_enrichment, backfill_blank_states


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
    parser.add_argument(
        "--backfill-states",
        action="store_true",
        help=(
            "Run the one-shot City/State/ZIP backfill for main-tab carriers with "
            "blank State. Skips carriers currently in the Quarantine tab. "
            "Bypasses the full enrichment pass — use standalone for the Apr-15 "
            "42-row fix. See memory/feedback_carrier_category_rules.md."
        ),
    )
    parser.add_argument(
        "--backfill-log",
        type=str,
        default="scripts/logs/state_backfill_20260415.log",
        help="Log file path for --backfill-states run.",
    )
    args = parser.parse_args()

    _configure_logging(args.verbose)

    # ── Backfill-states mode ─────────────────────────────────────────────────
    if args.backfill_states:
        summary = backfill_blank_states(
            dry_run=args.dry_run,
            log_path=args.backfill_log,
        )
        print("\n==== STATE BACKFILL SUMMARY ====")
        print(f"  elapsed:                  {summary['elapsed_s']:.1f}s")
        print(f"  blank-state rows found:   {summary['blank_state_rows_found']}")
        print(f"  DOTs attempted:           {summary['dots_attempted']}")
        print(f"  DOTs filled:              {summary['dots_filled']}")
        print(f"  DOTs still blank:         {summary['dots_still_blank']}")
        print(f"  DOTs skipped (quarantine):{summary['dots_skipped_quarantined']}")
        print(f"  writes committed:         {summary['writes_committed']}")
        print(f"  dry run:                  {summary['dry_run']}")
        print()
        for d in summary["per_dot"]:
            tag = d["result"].upper()
            reason = f" ({d.get('reason', '')})" if d.get("reason") else ""
            geo = f"  {d['city']}, {d['state']} {d['zip']}" if d["result"] == "filled" else ""
            print(f"  DOT {d['dot']:>8}  {d['name'][:40]:<40}  {tag}{reason}{geo}")
        if summary["skipped_quarantined_dots"]:
            print(f"\n  Quarantined (skipped): {', '.join(summary['skipped_quarantined_dots'])}")
        if args.log_json:
            os.makedirs(os.path.dirname(args.log_json) or ".", exist_ok=True)
            with open(args.log_json, "w", encoding="utf-8") as fp:
                json.dump(summary, fp, indent=2, default=str)
            print(f"\n  summary JSON: {args.log_json}")
        return 0

    # ── Standard enrichment mode ─────────────────────────────────────────────
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
