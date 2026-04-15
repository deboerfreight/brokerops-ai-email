"""
BrokerOps AI — One-off: mark the 5 carriers sent in the Slack-unfurl incident
on 2026-04-15 so future batch eligibility gates correctly exclude them.

Sets Outreach_Status=E1_SENT and Outreach_E1_SentAt=2026-04-15T20:07:00Z for
each DOT. Does NOT overwrite Outreach_Thread_Id if already populated.

Idempotent: if Outreach_Status is already E1_SENT, skips that row.

Usage:
    python scripts/mark_sent_20260415.py

Logs to scripts/logs/approval_flow_hardening_20260415.log
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# ── Bootstrap ─────────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

_LOG_FILE = _REPO_ROOT / "scripts" / "logs" / "approval_flow_hardening_20260415.log"
_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(_LOG_FILE)),
    ],
)
logger = logging.getLogger("mark_sent_20260415")

try:
    from dotenv import load_dotenv
    load_dotenv(_REPO_ROOT / ".env")
except ImportError:
    pass

# ── Carriers sent in the incident ─────────────────────────────────────────────
SENT_CARRIERS = [
    {"dot": "299073",  "name": "Ryder Transportation Solutions LLC"},
    {"dot": "263813",  "name": "MCI Express"},
    {"dot": "752774",  "name": "Pro Transport Inc"},
    {"dot": "3358407", "name": "Milum Express LLC"},
    {"dot": "1899547", "name": "H & M Container Transport INC"},
]

SENT_AT = "2026-04-15T20:07:00Z"


def main() -> None:
    from app.sheets import (
        get_carrier_by_dot,
        update_carrier_field_by_dot,
        CARRIER_DB_TAB,
        read_range,
        write_range,
    )
    from app.config import get_settings

    sheet_id = get_settings().CARRIER_MASTER_SHEET_ID
    if not sheet_id:
        logger.error("CARRIER_MASTER_SHEET_ID not set")
        sys.exit(1)

    logger.info("=== mark_sent_20260415: marking 5 incident carriers ===")

    for entry in SENT_CARRIERS:
        dot = entry["dot"]
        name = entry["name"]
        logger.info("Processing DOT=%s  %s", dot, name)

        carrier = get_carrier_by_dot(dot)
        if not carrier:
            logger.warning("  DOT=%s not found in sheet — skipping", dot)
            continue

        current_status = carrier.get("Outreach_Status") or carrier.get("Outreach Status") or ""
        current_thread = carrier.get("Outreach_Thread_Id") or ""

        if current_status == "E1_SENT":
            logger.info("  DOT=%s already marked E1_SENT — idempotent skip", dot)
            continue

        # Write Outreach_Status
        update_carrier_field_by_dot(dot, "Outreach_Status", "E1_SENT")
        logger.info("  DOT=%s  Outreach_Status -> E1_SENT", dot)

        # Write Outreach_E1_SentAt
        update_carrier_field_by_dot(dot, "Outreach_E1_SentAt", SENT_AT)
        logger.info("  DOT=%s  Outreach_E1_SentAt -> %s", dot, SENT_AT)

        # Do NOT overwrite Outreach_Thread_Id if already set
        if current_thread:
            logger.info("  DOT=%s  Outreach_Thread_Id already set (%s) — preserving", dot, current_thread)
        else:
            logger.info("  DOT=%s  Outreach_Thread_Id blank — leaving as-is (no thread ID available)", dot)

    logger.info("=== mark_sent_20260415 complete ===")


if __name__ == "__main__":
    main()
