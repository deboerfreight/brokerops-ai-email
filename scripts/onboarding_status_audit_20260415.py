"""
BrokerOps AI — Onboarding_Status audit (Fix 5, 2026-04-15). READ-ONLY.

Queries all 203 rows. Reports:
  - Count per distinct Onboarding_Status value (including blank)
  - Rows where legacy Onboarding_Status conflicts with new Outreach_Status
  - Rows with both Onboarding_Status AND Outreach_Status blank (unknown state)

Writes report to scripts/logs/onboarding_status_audit_20260415.md

DO NOT CHANGE ANY VALUES. This is diagnostic only.

Usage:
    python scripts/onboarding_status_audit_20260415.py
"""
from __future__ import annotations

import logging
import sys
from collections import Counter
from pathlib import Path
from datetime import datetime

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

_LOG_FILE = _REPO_ROOT / "scripts" / "logs" / "pipeline_fixes_reply_drafts_20260415.log"
_REPORT_MD = _REPO_ROOT / "scripts" / "logs" / "onboarding_status_audit_20260415.md"
_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(_LOG_FILE)),
    ],
)
logger = logging.getLogger("onboarding_status_audit_20260415")

# Outreach_Status values that indicate active/completed outreach
_OUTREACH_ACTIVE = frozenset({
    "E1_SENT", "E2_SENT", "E3_SENT", "replied_interested",
    "replied_not_interested", "bounced", "ooo_paused", "ooo_redirected",
    "redirected", "outreach_error",
})

# Legacy Onboarding_Status values that indicate active engagement
_ONBOARDING_ACTIVE = frozenset({
    "replied_interested", "docs_requested", "docs_request_scheduled",
    "docs_received_partial", "docs_verified", "agreement_pending",
    "onboarded", "paused", "rejected",
})


def main() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv(_REPO_ROOT / ".env")
    except ImportError:
        pass

    from app.sheets import get_all_carriers
    from app.config import get_settings

    sheet_id = get_settings().CARRIER_MASTER_SHEET_ID
    if not sheet_id:
        logger.error("CARRIER_MASTER_SHEET_ID not set")
        sys.exit(1)

    logger.info("Loading all carriers (read-only)...")
    carriers = get_all_carriers()
    logger.info("Total rows: %d", len(carriers))

    ob_counter: Counter = Counter()
    os_counter: Counter = Counter()
    conflicts = []
    both_blank = []

    for c in carriers:
        name = (c.get("Company Name") or c.get("Legal_Name") or "(unknown)").strip()
        dot  = (c.get("DOT Number") or c.get("DOT_Number") or "").strip()

        ob_val = (c.get("Onboarding_Status") or "").strip()
        os_val = (c.get("Outreach_Status") or "").strip()

        ob_key = ob_val if ob_val else "(blank)"
        os_key = os_val if os_val else "(blank)"

        ob_counter[ob_key] += 1
        os_counter[os_key] += 1

        # Conflict: legacy Onboarding_Status says active but Outreach_Status says nothing
        if ob_val in _ONBOARDING_ACTIVE and os_val not in _OUTREACH_ACTIVE:
            conflicts.append({
                "dot": dot, "name": name,
                "Onboarding_Status": ob_val,
                "Outreach_Status": os_val or "(blank)",
                "drift": f"legacy={ob_val!r} / outreach={repr(os_val) if os_val else '(blank)'}",
            })

        # Also: Outreach_Status active but Onboarding_Status still blank
        if os_val in _OUTREACH_ACTIVE and not ob_val:
            conflicts.append({
                "dot": dot, "name": name,
                "Onboarding_Status": "(blank)",
                "Outreach_Status": os_val,
                "drift": f"outreach={os_val!r} but Onboarding_Status blank",
            })

        if not ob_val and not os_val:
            both_blank.append({"dot": dot, "name": name})

    # Write report
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"# Onboarding_Status Audit — {ts}",
        "",
        f"**Total rows:** {len(carriers)}  ",
        f"**Conflicts (drift between legacy / new columns):** {len(conflicts)}  ",
        f"**Both blank (unknown state):** {len(both_blank)}  ",
        "",
        "## Onboarding_Status distribution",
        "",
        "| Value | Count |",
        "|-------|-------|",
    ]
    for val, cnt in sorted(ob_counter.items(), key=lambda x: -x[1]):
        lines.append(f"| `{val}` | {cnt} |")

    lines += [
        "",
        "## Outreach_Status distribution",
        "",
        "| Value | Count |",
        "|-------|-------|",
    ]
    for val, cnt in sorted(os_counter.items(), key=lambda x: -x[1]):
        lines.append(f"| `{val}` | {cnt} |")

    lines += [
        "",
        "## Drift — Onboarding_Status conflicts with Outreach_Status",
        "",
    ]
    if conflicts:
        lines.append("| DOT | Name | Onboarding_Status | Outreach_Status | Note |")
        lines.append("|-----|------|-------------------|-----------------|------|")
        for r in conflicts:
            lines.append(
                f"| {r['dot']} | {r['name'][:40]} "
                f"| `{r['Onboarding_Status']}` "
                f"| `{r['Outreach_Status']}` "
                f"| {r['drift']} |"
            )
    else:
        lines.append("_(none — no conflicts detected)_")

    lines += [
        "",
        "## Rows with both columns blank (unknown state)",
        "",
    ]
    if both_blank:
        lines.append("| DOT | Name |")
        lines.append("|-----|------|")
        for r in both_blank[:50]:  # cap at 50 to keep report readable
            lines.append(f"| {r['dot']} | {r['name'][:50]} |")
        if len(both_blank) > 50:
            lines.append(f"| ... | _(+{len(both_blank) - 50} more)_ |")
    else:
        lines.append("_(none)_")

    lines += ["", "---", "_READ-ONLY audit. No values were changed._"]

    _REPORT_MD.write_text("\n".join(lines))
    logger.info("Report written to %s", _REPORT_MD)
    logger.info(
        "onboarding_status_audit complete: total=%d conflicts=%d both_blank=%d",
        len(carriers), len(conflicts), len(both_blank),
    )

    # Print summary to stdout
    print(f"\nOnboarding_Status distribution:")
    for val, cnt in sorted(ob_counter.items(), key=lambda x: -x[1]):
        print(f"  {val:<35} {cnt}")
    print(f"\nConflicts: {len(conflicts)}")
    print(f"Both blank: {len(both_blank)}")
    print(f"\nFull report: {_REPORT_MD}")


if __name__ == "__main__":
    main()
