"""
Gate 5 — Dry-run outreach preview (NO SEND).

For each NEW carrier with a valid email:
  1. Render Sofia's initial outreach via the REAL template function.
  2. Write an RFC 5322 .eml file.
  3. Write a manifest CSV.

Excludes any carrier flagged by the most recent Gate 2 dedup JSON.

This script renders + writes only. A hard assert at import time refuses to
run if any send-path token slips in.
"""
from __future__ import annotations

import csv
import glob
import json
import logging
import os
import sys
from datetime import datetime, timezone
from email.message import EmailMessage

# Project root on path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# ── HARD SAFETY ASSERT ─────────────────────────────────────────────────────
#
# This script must never import the send path. If someone accidentally adds
# `from app.gmail import send_email` above, the assert below fires at import
# time and refuses to run. Also verified by a regex scan of this file.

_SRC_PATH = os.path.abspath(__file__)
with open(_SRC_PATH, "r", encoding="utf-8") as _f:
    _SRC = _f.read()

# Tokens constructed at runtime so this literal list does not itself trip
# the scan. Anyone who edits this file and adds a real send call will trip it.
_FORBIDDEN = [
    "send" + "_email",
    "send" + "_message",
    "messages" + ".send",
    "smtp" + "lib",
    "reply" + "_to_thread",
]
for _token in _FORBIDDEN:
    lines = [
        line for line in _SRC.splitlines()
        if _token in line
        and not line.strip().startswith("#")
        and "_FORBIDDEN" not in line
        and "_token" not in line
    ]
    assert not lines, f"HARD ASSERT FAILED: forbidden token '{_token}' present in preview script: {lines}"

from app.sheets import get_all_carriers  # noqa: E402
from app.workflows.carrier_outreach import build_initial_outreach  # noqa: E402
from app.config import get_settings  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("preview_outreach")


def _load_latest_dedup_flags() -> dict[str, str]:
    """Return {mc: exclusion_reason} for any carrier flagged by the most recent
    Gate 2 dedup run. Empty dict if no dedup file exists."""
    log_dir = os.path.join(ROOT, "scripts", "logs")
    pattern = os.path.join(log_dir, "outreach_preflight_dedup_*.json")
    matches = sorted(glob.glob(pattern))
    if not matches:
        logger.warning("No dedup JSON found; proceeding with no exclusions")
        return {}
    latest = matches[-1]
    logger.info("Using dedup flags from %s", latest)
    with open(latest, "r", encoding="utf-8") as f:
        data = json.load(f)

    flags: dict[str, str] = {}
    for r in data.get("results", []):
        if r.get("has_outbound"):
            flags[r["mc"]] = "prior_outbound"
        elif r.get("has_inbound_only"):
            flags[r["mc"]] = "prior_inbound_flag_for_review"
    return flags


def _safe_filename(s: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in s)


def main() -> int:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = os.path.join(ROOT, "scripts", "logs", "outreach_preview")
    os.makedirs(out_dir, exist_ok=True)

    dedup_flags = _load_latest_dedup_flags()
    logger.info("Loaded %d dedup exclusions", len(dedup_flags))

    logger.info("Loading carriers...")
    carriers = get_all_carriers()
    logger.info("Loaded %d carriers", len(carriers))

    new_with_email = []
    for c in carriers:
        status = (c.get("Onboarding_Status") or "").strip().upper()
        if status != "NEW":
            continue
        email = (c.get("Primary_Email") or "").strip()
        if not email or email.upper() == "PHONE_ONLY" or "@" not in email:
            continue
        new_with_email.append(c)

    logger.info("Found %d NEW carriers with valid email", len(new_with_email))

    settings = get_settings()
    from_addr = settings.BROKER_EMAIL

    manifest_path = os.path.join(out_dir, f"manifest_{ts}.csv")
    rendered = 0
    excluded = 0

    with open(manifest_path, "w", encoding="utf-8", newline="") as mf:
        writer = csv.writer(mf)
        writer.writerow(["mc", "company", "email", "subject", "filename", "excluded", "exclusion_reason"])

        for c in new_with_email:
            mc = c.get("MC_Number", "") or c.get("DOT_Number", "")
            name = c.get("Legal_Name", "") or c.get("DBA_Name", "")
            email = c.get("Primary_Email", "").strip()

            subject, body = build_initial_outreach(c)

            exclusion_reason = dedup_flags.get(mc, "")
            is_excluded = bool(exclusion_reason)

            # Always render the .eml (so Derek can still eyeball even excluded ones).
            msg = EmailMessage()
            msg["From"] = from_addr
            msg["To"] = email
            msg["Subject"] = subject
            msg["Date"] = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
            msg["X-Preview-Status"] = "EXCLUDED" if is_excluded else "PENDING"
            if is_excluded:
                msg["X-Preview-Exclusion-Reason"] = exclusion_reason
            msg.set_content(body)

            filename = f"{_safe_filename(str(mc))}_{ts}.eml"
            eml_path = os.path.join(out_dir, filename)
            with open(eml_path, "wb") as f:
                f.write(bytes(msg))

            writer.writerow([mc, name, email, subject, filename,
                             "TRUE" if is_excluded else "FALSE", exclusion_reason])
            rendered += 1
            if is_excluded:
                excluded += 1

    logger.info("Rendered %d .eml files (%d excluded) to %s",
                rendered, excluded, out_dir)
    logger.info("Manifest: %s", manifest_path)

    # Final summary
    print("\n=== Outreach preview summary ===")
    print(f"NEW with valid email:   {len(new_with_email)}")
    print(f"Rendered .eml files:    {rendered}")
    print(f"Excluded by dedup:      {excluded}")
    print(f"Output dir:             {out_dir}")
    print(f"Manifest:               {manifest_path}")
    print()
    print("To open one on Windows:")
    # Show the first non-excluded one if any
    for fname in sorted(os.listdir(out_dir)):
        if fname.endswith(".eml"):
            print(f"  start {os.path.join('scripts', 'logs', 'outreach_preview', fname)}")
            break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
