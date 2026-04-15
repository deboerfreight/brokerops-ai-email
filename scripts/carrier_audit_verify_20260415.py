"""
Phase 2 — Playwright verification of MEDIUM-confidence flags from the audit.

Reads the 4 MEDIUM DOTs, hits brokersnapshot.com for each, parses the text
for operation type markers, and writes findings to a JSON file for the
markdown merge step.
"""
from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path

from app.enrichment.playwright_fetcher import PlaywrightFetcher

OUTPUT_JSON = Path("scripts/logs/carrier_audit_playwright_20260415.json")

# 4 MEDIUM DOTs from the Phase 1 run
TARGETS = [
    {"row": 24, "dot": "3355445", "name": "Universal Waste Management LLC", "flag": "waste_hauler_review"},
    {"row": 41, "dot": "3579107", "name": "Hall Hauling & Construction LLC", "flag": "private_fleet_review"},
    {"row": 92, "dot": "1071704", "name": "Industrial Waste Service INC", "flag": "waste_hauler_review"},
    {"row": 132, "dot": "1026131", "name": "Fitzgerald Excavating & Trucking INC", "flag": "private_fleet_review"},
]

RATE_LIMIT_SEC = 15.0


def extract_findings(text: str) -> dict:
    """Parse brokersnapshot page text for operation-type markers."""
    t = text.upper()
    out = {
        "for_hire": None,
        "private": None,
        "interstate": None,
        "intrastate": None,
        "operation_classification": [],
        "cargo_hints": [],
    }
    # For-hire / private markers
    if "FOR HIRE" in t or "FOR-HIRE" in t:
        out["for_hire"] = True
    if re.search(r"\bPRIVATE\b", t):
        out["private"] = True
    if "INTERSTATE" in t:
        out["interstate"] = True
    if "INTRASTATE" in t:
        out["intrastate"] = True
    # Classification hints
    for token in ["AUTH. FOR HIRE", "EXEMPT FOR HIRE", "PRIVATE PROPERTY", "PRIVATE(PROPERTY)", "PRIVATE PASSENGER"]:
        if token in t:
            out["operation_classification"].append(token)
    # Cargo
    for cargo in ["GENERAL FREIGHT", "REFRIGERATED FOOD", "BUILDING MATERIALS",
                  "CONSTRUCTION", "GARBAGE, REFUSE", "GARBAGE", "REFUSE",
                  "MACHINERY, LARGE OBJECTS", "METAL: SHEETS, COILS, ROLLS"]:
        if cargo in t:
            out["cargo_hints"].append(cargo)
    return out


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    results = []
    with PlaywrightFetcher(fresh_context_per_request=True) as fetcher:
        for i, target in enumerate(TARGETS):
            if i > 0:
                logging.info("Sleeping %.1fs before next fetch...", RATE_LIMIT_SEC)
                time.sleep(RATE_LIMIT_SEC)
            url = f"https://brokersnapshot.com/Company?dot={target['dot']}"
            logging.info("Fetching %s — %s (DOT %s)", target["name"], url, target["dot"])
            try:
                page = fetcher.fetch_page(url)
            except Exception as e:
                logging.error("Fetch failed: %s", e)
                results.append({**target, "error": str(e)})
                continue

            if page.get("blocked"):
                logging.warning("Blocked: %s", page.get("block_reason"))
                results.append({**target, "blocked": True, "block_reason": page.get("block_reason"), "title": page.get("title")})
                continue

            text = page.get("text") or ""
            findings = extract_findings(text)
            # Snippet — first 400 chars of text for context
            snippet = text[:400].replace("\n", " ").strip()
            results.append({
                **target,
                "final_url": page.get("final_url"),
                "title": page.get("title"),
                "status": page.get("status"),
                "findings": findings,
                "snippet": snippet,
            })
            logging.info("  findings: %s", findings)

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nWrote {len(results)} results to {OUTPUT_JSON}")
    for r in results:
        print(json.dumps(r, indent=2)[:600])
        print("---")


if __name__ == "__main__":
    main()
