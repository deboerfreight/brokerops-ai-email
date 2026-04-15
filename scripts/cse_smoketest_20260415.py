"""
CSE Smoke Test — DOT 203412 (Driver Driven Transportation, MN)
Rex — 2026-04-15

Runs enrich_carrier_email() against a single carrier dict for DOT 203412.
Logs everything to scripts/logs/cse_smoketest_20260415.log.
READ-ONLY — no sheet writes.
"""
from __future__ import annotations

import logging
import sys
import os
from pathlib import Path

# ── Logging setup ─────────────────────────────────────────────────────────────
LOG_FILE = Path("C:/Users/Owner/brokerops-ai/scripts/logs/cse_smoketest_20260415.log")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("rex.cse_smoketest")

# ── Ensure brokerops-ai root is in path and .env is loaded ───────────────────
REPO_ROOT = Path("C:/Users/Owner/brokerops-ai")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Load .env explicitly before importing app modules
from dotenv import load_dotenv
load_dotenv(REPO_ROOT / ".env")
log.info("dotenv loaded from %s", REPO_ROOT / ".env")

# ── Import app modules ────────────────────────────────────────────────────────
from app.config import get_settings
from app.email_enrichment import enrich_carrier_email

# ── Verify settings ───────────────────────────────────────────────────────────
settings = get_settings()
api_key_present = bool(settings.GOOGLE_CSE_API_KEY)
cx_present = bool(settings.GOOGLE_CSE_CX)
apollo_present = bool(settings.APOLLO_API_KEY)

log.info("Settings check — GOOGLE_CSE_API_KEY present: %s, GOOGLE_CSE_CX present: %s, APOLLO_API_KEY present: %s",
         api_key_present, cx_present, apollo_present)

if not api_key_present or not cx_present:
    log.error("ABORT — CSE keys not loaded from .env. Cannot proceed.")
    sys.exit(1)

# ── Carrier dict for DOT 203412 ───────────────────────────────────────────────
CARRIER = {
    "DOT_Number": "203412",
    "MC_Number": "",
    "Legal_Name": "Driver Driven Transportation",
    "City": "",
    "State": "MN",
}

log.info("=== SMOKE TEST START — DOT %s (%s) ===", CARRIER["DOT_Number"], CARRIER["Legal_Name"])

# ── Run enrichment ────────────────────────────────────────────────────────────
try:
    result = enrich_carrier_email(CARRIER)
except Exception as exc:
    log.exception("enrich_carrier_email raised an unhandled exception: %s", exc)
    log.error("VERDICT: ERROR — unhandled exception in enrichment pipeline")
    sys.exit(1)

# ── Classify result ───────────────────────────────────────────────────────────
email = result.get("email")
source = result.get("source", "UNKNOWN")
website = result.get("website")

log.info("Raw result — email present: %s, source: %s, website: %s",
         bool(email), source, website)

if email and source != "PHONE_ONLY":
    # HIT
    redacted = email[:3] + "***@" + email.split("@")[1] if "@" in email else email[:3] + "..."
    log.info("VERDICT: HIT — source=%s, email_redacted=%s, website=%s", source, redacted, website)
    print(f"\n[VERDICT] HIT — source={source}, email (redacted)={redacted}, website={website}")
elif source == "PHONE_ONLY":
    log.info("VERDICT: CLEAN MISS — pipeline ran to completion, no email found for this carrier")
    print(f"\n[VERDICT] CLEAN MISS — source=PHONE_ONLY, website={website}")
else:
    log.warning("VERDICT: CLEAN MISS (no email, source=%s)", source)
    print(f"\n[VERDICT] CLEAN MISS — source={source}, website={website}")

log.info("=== SMOKE TEST COMPLETE ===")
