"""Download the FMCSA L&I bulk CSVs and rebuild the local insurance lookup DB.

Run manually or via scheduled task on the 3rd Friday of each month (the day
after FMCSA publishes the new snapshot to the DOT datahub).

Usage:
    PYTHONPATH=. python scripts/refresh_li_insurance.py

What it does:
    1. Downloads the Insur - All With History CSV (~38 MB, 467K rows)
    2. Downloads the Carrier - All With History CSV (~320 MB, 1.85M rows)
    3. Rebuilds data/fmcsa_li/insurance_lookup.sqlite
    4. Spot-checks 5 known DOTs and reports whether the lookup works
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import httpx

from app.vetting.li_insurance_lookup import (
    CARRIER_CSV_URL,
    INSUR_CSV_URL,
    LI_DATA_DIR,
    build_lookup_db,
    get_insurance,
    lookup_db_stats,
)

logger = logging.getLogger("brokerops.refresh_li")

USER_AGENT = "BrokerOpsAI/1.0 (+https://brokerops.ai) L&I lookup refresh"

# Known DOTs for post-build verification
SPOT_CHECK_DOTS = {
    "186106": "CYPRESS TRUCK LINES",
    "499032": "SUNBELT TRANSPORT",
    "541257": "ABCO TRANSPORTATION",
    "2911927": "SHELTON TRUCKING",
    "1537209": "TRI STATE / NEW LINE TRANSPORT",
}


def _download(url: str, dest: Path) -> tuple[int, float]:
    """Stream-download a URL to disk. Returns (bytes, seconds)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    total = 0
    headers = {"User-Agent": USER_AGENT}
    with httpx.stream("GET", url, headers=headers, timeout=600) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=65536):
                f.write(chunk)
                total += len(chunk)
    return total, time.time() - t0


def refresh(skip_download: bool = False) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    LI_DATA_DIR.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d")

    insur_path = LI_DATA_DIR / f"li_insur_{ts}.csv"
    carrier_path = LI_DATA_DIR / f"li_carrier_{ts}.csv"

    if not skip_download:
        for url, dest, label in [
            (INSUR_CSV_URL, insur_path, "Insur - All With History"),
            (CARRIER_CSV_URL, carrier_path, "Carrier - All With History"),
        ]:
            logger.info("Downloading %s -> %s", label, dest.name)
            bytes_, elapsed = _download(url, dest)
            logger.info(
                "  %.2f MB in %.1fs (%.2f MB/s)",
                bytes_ / 1024 / 1024,
                elapsed,
                (bytes_ / 1024 / 1024) / max(elapsed, 0.1),
            )
            # 1 req/sec between requests to be a good citizen
            time.sleep(1)
    else:
        logger.info("skip_download=True — using latest files already on disk")

    # Build the lookup DB
    logger.info("Building SQLite lookup DB...")
    t0 = time.time()
    count = build_lookup_db()
    logger.info("Built in %.1fs: %d DOTs indexed", time.time() - t0, count)

    stats = lookup_db_stats()
    logger.info("DB stats: %s", stats)

    # Spot-check
    logger.info("Spot-checking known DOTs:")
    ok = 0
    for dot, label in SPOT_CHECK_DOTS.items():
        p = get_insurance(dot)
        if p:
            logger.info(
                "  DOT %s (%s): bipd=$%s cargo=$%s insurer=%r",
                dot, label, f"{p.bipd_liability:,}", f"{p.cargo:,}", p.insurer_name,
            )
            ok += 1
        else:
            logger.warning("  DOT %s (%s): NOT FOUND in lookup", dot, label)

    logger.info("Spot-check: %d/%d resolved", ok, len(SPOT_CHECK_DOTS))
    if ok == 0:
        logger.error("NO spot-check DOTs resolved — refresh failed")
        return 1
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Refresh FMCSA L&I insurance lookup DB")
    ap.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloads; rebuild from the newest CSVs already in data/fmcsa_li/",
    )
    args = ap.parse_args()
    return refresh(skip_download=args.skip_download)


if __name__ == "__main__":
    sys.exit(main())
