"""CLI wrapper for the carrier vetting daily sweep.

Usage:
    PYTHONPATH=. python scripts/run_vetting_sweep.py --all
    PYTHONPATH=. python scripts/run_vetting_sweep.py --main
    PYTHONPATH=. python scripts/run_vetting_sweep.py --quarantine

  --all           run sweep_carrier_database + sweep_quarantine
  --main          run sweep_carrier_database only (re-vet existing rows; no FMCSA)
  --quarantine    run sweep_quarantine only (release any rows that now pass)
  --refetch       (with --main or --all) also refresh FMCSA before vetting

Exit codes: 0 on success, 1 on uncaught exception.
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import sys
from datetime import datetime, timezone

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from app.vetting.sweep import sweep_carrier_database, sweep_quarantine

# Python logs to a separate file from the .bat wrapper log to avoid Windows
# file-handle contention when the scheduled task redirects stdout into the
# wrapper log at the same time Python opens its own handler.
LOG_PATH = "scripts/logs/vetting_sweep_python.log"


def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main():
    parser = argparse.ArgumentParser(description="Carrier vetting sweep")
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--all", action="store_true",
                   help="run main + quarantine sweeps")
    g.add_argument("--main", action="store_true",
                   help="run main-tab sweep only")
    g.add_argument("--quarantine", action="store_true",
                   help="run quarantine sweep only")
    parser.add_argument("--refetch", action="store_true",
                        help="re-fetch FMCSA before main-tab vetting")
    args = parser.parse_args()

    if not (args.all or args.main or args.quarantine):
        args.all = True

    _setup_logging()
    log = logging.getLogger("run_vetting_sweep")
    log.info("=" * 60)
    log.info("vetting sweep tick at %s (args=%s)",
             datetime.now(timezone.utc).isoformat(), vars(args))

    summary: dict = {}
    try:
        if args.all or args.main:
            log.info("running sweep_carrier_database(re_fetch_fmcsa=%s)", args.refetch)
            summary["main"] = sweep_carrier_database(re_fetch_fmcsa=args.refetch)
            log.info("main result: %s", summary["main"])
        if args.all or args.quarantine:
            log.info("running sweep_quarantine")
            summary["quarantine"] = sweep_quarantine()
            log.info("quarantine result: %s", summary["quarantine"])
    except Exception as exc:
        log.exception("sweep failed: %s", exc)
        sys.exit(1)

    log.info("done: %s", json.dumps(summary, default=str))
    log.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
