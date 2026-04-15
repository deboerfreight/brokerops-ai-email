"""
BrokerOps AI — Service Type audit + reclassification (Fix 2, 2026-04-15).

Query all carriers tagged Service Type=General. For any whose legal name
contains auto-transport-adjacent terms, reclassify to Auto Transport.

Pattern list (word-boundary, case-insensitive):
  auto transport, auto express, auto trans, auto haul,
  car hauler, car haul, vehicle transport, auto carrier

Specifically checks: 305 Auto Express CORP (DOT 2506276)

Reports:
  - how many carriers reclassified
  - which ones (name + DOT)
  - no false positives (e.g. "Automobile Trucking" won't match — word boundary
    requires the full trigger phrase)

Writes audit log to scripts/logs/service_type_audit_20260415.md
Then applies the reclassification to the live sheet via batch update.

Usage:
    python scripts/service_type_audit_20260415.py [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path
from datetime import datetime

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

_LOG_FILE = _REPO_ROOT / "scripts" / "logs" / "pipeline_fixes_reply_drafts_20260415.log"
_AUDIT_MD  = _REPO_ROOT / "scripts" / "logs" / "service_type_audit_20260415.md"
_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(_LOG_FILE)),
    ],
)
logger = logging.getLogger("service_type_audit_20260415")

# Word-boundary patterns that indicate auto-hauling semantics.
# "Automobile Trucking" does NOT match because "automobile" is not in this list.
_AUTO_TRANSPORT_RE = re.compile(
    r"\b("
    r"auto transport|auto express|auto trans|auto haul|"
    r"car hauler|car haul|vehicle transport|auto carrier"
    r")\b",
    re.IGNORECASE,
)

_TARGET_DOT = "2506276"  # 305 Auto Express CORP


def main() -> None:
    parser = argparse.ArgumentParser(description="Service Type audit + reclassification.")
    parser.add_argument("--dry-run", action="store_true", help="Report only, no sheet writes")
    args = parser.parse_args()

    try:
        from dotenv import load_dotenv
        load_dotenv(_REPO_ROOT / ".env")
    except ImportError:
        pass

    from app.sheets import get_all_carriers, update_carrier_field_by_dot
    from app.config import get_settings

    sheet_id = get_settings().CARRIER_MASTER_SHEET_ID
    if not sheet_id:
        logger.error("CARRIER_MASTER_SHEET_ID not set")
        sys.exit(1)

    logger.info("Loading carriers...")
    all_carriers = get_all_carriers()
    logger.info("Total carriers: %d", len(all_carriers))

    reclassified = []
    checked_305 = False

    for c in all_carriers:
        stype = (c.get("Service Type") or c.get("Service_Type") or "").strip().lower()
        if stype != "general":
            continue

        name = (c.get("Company Name") or c.get("Legal_Name") or "").strip()
        dot  = (c.get("DOT Number") or c.get("DOT_Number") or "").strip()

        if dot == _TARGET_DOT:
            checked_305 = True

        if _AUTO_TRANSPORT_RE.search(name):
            matched_term = _AUTO_TRANSPORT_RE.search(name).group(0)
            reclassified.append({
                "dot": dot,
                "name": name,
                "matched_term": matched_term,
            })
            logger.info(
                "RECLASSIFY  DOT=%-12s  %-50s  matched=%r",
                dot, name, matched_term,
            )
        elif dot == _TARGET_DOT:
            # 305 Auto Express should have been caught by the pattern
            logger.warning(
                "VERIFY NEEDED  DOT=%s  %s  — pattern did NOT match; check name spelling",
                dot, name,
            )

    if not checked_305:
        logger.warning("DOT=%s (305 Auto Express CORP) not found in sheet at all", _TARGET_DOT)

    # Write audit markdown
    lines = [
        f"# Service Type Audit — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        f"Total carriers queried: {len(all_carriers)}",
        f"Carriers with Service Type=General checked: "
        f"{sum(1 for c in all_carriers if (c.get('Service Type') or '').strip().lower() == 'general')}",
        f"Carriers reclassified to Auto Transport: {len(reclassified)}",
        "",
        "## Reclassified carriers",
        "",
    ]
    if reclassified:
        lines.append("| DOT | Name | Matched term |")
        lines.append("|-----|------|-------------|")
        for r in reclassified:
            lines.append(f"| {r['dot']} | {r['name']} | `{r['matched_term']}` |")
    else:
        lines.append("_(none)_")

    lines += [
        "",
        "## Notes on false-positive safety",
        "",
        '- Pattern uses `\\b` (word boundary) — "Automobile Trucking" does not match.',
        '- Trigger list: `auto transport`, `auto express`, `auto trans`, `auto haul`,',
        '  `car hauler`, `car haul`, `vehicle transport`, `auto carrier`.',
        f'- 305 Auto Express CORP (DOT {_TARGET_DOT}) found in sheet: {checked_305}',
        "",
        f"Dry-run mode: {args.dry_run}",
    ]

    _AUDIT_MD.write_text("\n".join(lines))
    logger.info("Audit report written to %s", _AUDIT_MD)

    if args.dry_run:
        logger.info("DRY RUN — no sheet writes. %d carriers would be reclassified.", len(reclassified))
        return

    # Apply reclassifications
    for r in reclassified:
        try:
            update_carrier_field_by_dot(r["dot"], "Service Type", "Auto Transport")
            logger.info("Updated DOT=%s -> Service Type=Auto Transport", r["dot"])
        except Exception as e:
            logger.error("Failed to update DOT=%s: %s", r["dot"], e)

    logger.info(
        "service_type_audit_20260415 complete: %d reclassified",
        len(reclassified),
    )


if __name__ == "__main__":
    main()
