"""
Preflight Gate 2 — Gmail dedup cross-reference.

For each NEW carrier in Carrier_Master, search Gmail for any prior thread
involving their Primary_Email (sent OR received, any label, any time).

Output:
  scripts/logs/outreach_preflight_dedup_<timestamp>.json

NO SENDS. Read-only.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

# Project root on path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.sheets import get_all_carriers  # noqa: E402
from app.google_auth import get_gmail_service  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("preflight_dedup")


def _search_threads_for(email: str, svc) -> list[dict]:
    """Return list of {threadId, subject, date, direction} for any thread
    touching this email address, sent or received, any label."""
    query = f"(to:{email} OR from:{email})"
    resp = svc.users().messages().list(userId="me", q=query, maxResults=50).execute()
    msgs = resp.get("messages", [])
    if not msgs:
        return []

    seen_threads: dict[str, dict] = {}
    for m in msgs:
        msg_id = m["id"]
        try:
            full = svc.users().messages().get(
                userId="me", id=msg_id, format="metadata",
                metadataHeaders=["From", "To", "Subject", "Date"],
            ).execute()
        except Exception as e:
            logger.warning("Could not fetch message %s: %s", msg_id, e)
            continue

        tid = full.get("threadId", msg_id)
        headers = {h["name"].lower(): h["value"] for h in full.get("payload", {}).get("headers", [])}
        subject = headers.get("subject", "")
        date = headers.get("date", "")
        from_h = headers.get("from", "").lower()
        to_h = headers.get("to", "").lower()

        direction = "unknown"
        if email.lower() in from_h:
            direction = "inbound"
        elif email.lower() in to_h:
            direction = "outbound"

        internal_date = int(full.get("internalDate", "0"))

        if tid not in seen_threads or internal_date > seen_threads[tid]["internal_date"]:
            seen_threads[tid] = {
                "threadId": tid,
                "subject": subject,
                "date": date,
                "internal_date": internal_date,
                "direction": direction,
            }

    return list(seen_threads.values())


def main() -> int:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = os.path.join(ROOT, "scripts", "logs")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"outreach_preflight_dedup_{ts}.json")

    logger.info("Loading carriers...")
    carriers = get_all_carriers()
    logger.info("Loaded %d carriers", len(carriers))

    # NEW carriers with a valid email
    new_carriers = []
    for c in carriers:
        status = (c.get("Onboarding_Status") or "").strip().upper()
        if status != "NEW":
            continue
        email = (c.get("Primary_Email") or "").strip()
        if not email or email.upper() == "PHONE_ONLY" or "@" not in email:
            continue
        new_carriers.append(c)

    logger.info("Found %d NEW carriers with valid email", len(new_carriers))

    svc = get_gmail_service()

    results = []
    flagged = 0
    for idx, c in enumerate(new_carriers, start=1):
        mc = c.get("MC_Number", "") or c.get("DOT_Number", "")
        name = c.get("Legal_Name", "") or c.get("DBA_Name", "")
        email = c.get("Primary_Email", "").strip()

        logger.info("[%d/%d] checking %s (%s)", idx, len(new_carriers), mc, email)

        try:
            threads = _search_threads_for(email, svc)
        except Exception as e:
            logger.error("Gmail search failed for %s: %s", email, e)
            threads = []
            results.append({
                "mc": mc,
                "company": name,
                "email": email,
                "error": str(e),
                "threads": [],
                "has_prior": False,
                "has_outbound": False,
                "has_inbound_only": False,
                "recommendation": "ALLOW (search failed, default allow)",
            })
            continue

        has_outbound = any(t["direction"] == "outbound" for t in threads)
        has_inbound = any(t["direction"] == "inbound" for t in threads)
        has_prior = bool(threads)

        if has_outbound:
            recommendation = "EXCLUDE (prior outbound)"
            flagged += 1
        elif has_inbound:
            recommendation = "FLAG (prior inbound only — Derek's judgment)"
            flagged += 1
        else:
            recommendation = "ALLOW (no prior history)"

        results.append({
            "mc": mc,
            "company": name,
            "email": email,
            "threads": threads,
            "has_prior": has_prior,
            "has_outbound": has_outbound,
            "has_inbound_only": has_inbound and not has_outbound,
            "recommendation": recommendation,
        })

    output = {
        "generated_at": ts,
        "total_new_carriers_checked": len(new_carriers),
        "flagged_count": flagged,
        "allowed_count": len(new_carriers) - flagged,
        "results": results,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    logger.info("Wrote %s", out_path)
    logger.info("Summary: %d checked, %d flagged, %d allowed",
                len(new_carriers), flagged, len(new_carriers) - flagged)

    # Print a brief summary table
    print("\n=== Flagged carriers ===")
    for r in results:
        if r["has_prior"]:
            print(f"  {r['mc']:<12} {r['email']:<40} {r['recommendation']}")

    print(f"\nOutput: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
