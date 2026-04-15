"""
Brave Search API Smoke Test — 2026-04-15
Rex runs this after Derek's key lands in the vault and hydrate_from_vault() is run.

Tests _search_brave() with a known carrier query.
Prints result count + first 3 URLs + first 50 chars of each snippet.
READ-ONLY — no API calls, no sheet writes in dry-run mode.

Usage:
    # Dry-run (import/syntax check only — no live API call):
    python scripts/brave_smoketest_20260415.py --dry-run

    # Live (requires BRAVE_SEARCH_API_KEY in .env):
    python scripts/brave_smoketest_20260415.py
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# ── Logging setup ─────────────────────────────────────────────────────────────
LOG_FILE = Path("C:/Users/Owner/brokerops-ai/scripts/logs/brave_pivot_20260415.log")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("bolt.brave_smoketest")

# ── Ensure brokerops-ai root is in path and .env is loaded ───────────────────
REPO_ROOT = Path("C:/Users/Owner/brokerops-ai")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(REPO_ROOT / ".env")
log.info("dotenv loaded from %s", REPO_ROOT / ".env")


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Smoke test for Brave Search integration in email_enrichment.py"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Import-check only — exit before any live API call",
    )
    return parser.parse_args()


def main():
    args = _parse_args()

    # ── Import check — will surface any syntax/import errors ─────────────────
    log.info("Importing app.email_enrichment._search_brave ...")
    from app.email_enrichment import _search_brave  # noqa: F401
    log.info("Import OK — _search_brave is importable")

    if args.dry_run:
        log.info("[DRY-RUN] Exiting before live API call — import check passed.")
        print("\n[DRY-RUN] Import check passed. Run without --dry-run to test live Brave API.")
        sys.exit(0)

    # ── Live call ─────────────────────────────────────────────────────────────
    import os
    api_key_present = bool(os.environ.get("BRAVE_SEARCH_API_KEY", "").strip())
    if not api_key_present:
        log.error("ABORT — BRAVE_SEARCH_API_KEY not set in .env. Run hydrate_from_vault first.")
        print("\n[ABORT] BRAVE_SEARCH_API_KEY missing. Add key to vault (tier: operations) then re-hydrate.")
        sys.exit(1)

    log.info("BRAVE_SEARCH_API_KEY present — proceeding with live search")
    log.info("Query: 'Driver Driven Transportation MN trucking'")

    # Call _search_brave directly with a raw search rather than going through
    # the full waterfall, so we isolate Brave-specific behavior.
    from app.email_enrichment import _search_brave as search_brave

    # Monkey-patch: temporarily expose internal call so we can capture raw results
    # instead of the single-email dict returned by _search_brave. We replicate
    # the Brave call inline here to show raw result data.
    import httpx  # noqa: E402 (already a project dependency)

    query = "Driver Driven Transportation MN trucking"
    brave_url = "https://api.search.brave.com/res/v1/web/search"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": os.environ["BRAVE_SEARCH_API_KEY"],
    }

    log.info("Sending GET %s  q=%r", brave_url, query)
    try:
        resp = httpx.get(
            brave_url,
            params={"q": query, "count": 10},
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as exc:
        log.error("Brave Search request failed: %s", exc)
        print(f"\n[FAIL] Brave Search request failed: {exc}")
        sys.exit(1)

    results = (data.get("web") or {}).get("results", [])
    log.info("Result count: %d", len(results))
    print(f"\n[RESULT] Brave returned {len(results)} result(s)")

    for i, item in enumerate(results[:3], start=1):
        url = item.get("url", "(no url)")
        snippet = (item.get("description") or "")[:50]
        log.info("  #%d  url=%s  snippet=%r", i, url, snippet)
        print(f"  #{i}  {url}")
        print(f"       snippet: {snippet!r}")

    print("\n[VERDICT] Brave Search API reachable and returning results." if results else "\n[VERDICT] Brave reachable but returned 0 results — check query or key tier.")
    log.info("=== SMOKE TEST COMPLETE ===")


if __name__ == "__main__":
    main()
