"""
BrokerOps AI — One-off: set Outreach_Exclude on a carrier row by DOT number.

Idempotent: if the column is already non-blank, overwrites with the new reason
only if the value differs (so re-running with the same reason is a true no-op).

Usage:
    python scripts/exclude_carrier.py --dot 3949115 --reason "Domain mismatch: ..."

Logs to scripts/logs/pipeline_fixes_reply_drafts_20260415.log
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

_LOG_FILE = _REPO_ROOT / "scripts" / "logs" / "pipeline_fixes_reply_drafts_20260415.log"
_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(_LOG_FILE)),
    ],
)
logger = logging.getLogger("exclude_carrier")


def main() -> None:
    parser = argparse.ArgumentParser(description="Set Outreach_Exclude on a carrier row by DOT.")
    parser.add_argument("--dot", required=True, help="DOT number of the carrier to exclude")
    parser.add_argument("--reason", required=True, help="Human-readable reason text stored in Outreach_Exclude")
    args = parser.parse_args()

    try:
        from dotenv import load_dotenv
        load_dotenv(_REPO_ROOT / ".env")
    except ImportError:
        pass

    from app.sheets import get_carrier_by_dot, update_carrier_field_by_dot
    from app.config import get_settings

    sheet_id = get_settings().CARRIER_MASTER_SHEET_ID
    if not sheet_id:
        logger.error("CARRIER_MASTER_SHEET_ID not set")
        sys.exit(1)

    carrier = get_carrier_by_dot(args.dot)
    if not carrier:
        logger.error("DOT=%s not found in sheet — cannot exclude", args.dot)
        sys.exit(1)

    name = carrier.get("Company Name") or carrier.get("Legal_Name") or "(unknown)"
    current = (carrier.get("Outreach_Exclude") or "").strip()

    if current == args.reason.strip():
        logger.info(
            "DOT=%s (%s): Outreach_Exclude already set to same value — idempotent skip",
            args.dot, name,
        )
        return

    if current:
        logger.info(
            "DOT=%s (%s): overwriting existing Outreach_Exclude=%r with new reason",
            args.dot, name, current,
        )

    update_carrier_field_by_dot(args.dot, "Outreach_Exclude", args.reason.strip())
    logger.info(
        "DOT=%s (%s): Outreach_Exclude set to: %s",
        args.dot, name, args.reason.strip(),
    )
    logger.info("exclude_carrier complete")


if __name__ == "__main__":
    main()
