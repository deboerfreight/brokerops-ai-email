"""
Quick Apollo enrichment test — runs 3 sample carriers through the
reordered waterfall. Does NOT write to Sheets.

Apollo free tier: 10,000 records/month. This test uses ≤6 API calls.
"""
import sys
import os
import logging

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.email_enrichment import enrich_carrier_email

logging.basicConfig(level=logging.INFO, format="%(name)s | %(message)s")

# 3 sample carriers — mix of sizes to test Apollo coverage
TEST_CARRIERS = [
    {
        "DOT_Number": "2243723",
        "MC_Number": "MC-757248",
        "Legal_Name": "CELADON TRUCKING INC",
        "City": "MIAMI",
        "State": "FL",
    },
    {
        "DOT_Number": "3256498",
        "MC_Number": "MC-1032050",
        "Legal_Name": "DOLPHIN LOGISTICS LLC",
        "City": "HIALEAH",
        "State": "FL",
    },
    {
        "DOT_Number": "2785310",
        "MC_Number": "MC-923147",
        "Legal_Name": "SOUTHEAST FREIGHT LINES",
        "City": "FORT LAUDERDALE",
        "State": "FL",
    },
]


def main():
    print("=" * 60)
    print("Apollo Enrichment Test — 3 carriers, read-only")
    print("=" * 60)

    results = []
    for carrier in TEST_CARRIERS:
        print(f"\n--- {carrier['Legal_Name']} (DOT {carrier['DOT_Number']}) ---")
        result = enrich_carrier_email(carrier)
        results.append((carrier["Legal_Name"], result))
        print(f"  Email:   {result.get('email') or '(none)'}")
        print(f"  Source:  {result['source']}")
        print(f"  Website: {result.get('website') or '(none)'}")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    sources = {}
    for name, r in results:
        src = r["source"]
        sources[src] = sources.get(src, 0) + 1
        status = "HIT" if r.get("email") else "MISS"
        print(f"  [{status}] {name:40s} -> {src}")

    print(f"\nSource breakdown: {dict(sources)}")
    hits = sum(1 for _, r in results if r.get("email"))
    print(f"Hit rate: {hits}/{len(results)}")


if __name__ == "__main__":
    main()
