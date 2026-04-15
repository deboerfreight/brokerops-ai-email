"""
BrokerOps AI — Carrier DB Cleanup Audit, 2026-04-15
=====================================================
Read-only pass against the main Carrier Database tab.

Steps:
  1. Pull all carriers from main tab.
  2. Apply name-based heuristic flags (HARD REMOVE / KEEP_FUEL / clean).
  3. For flagged carriers with a website, verify via Playwright (brokersnapshot
     fallback if no direct website).
  4. Write carrier_cleanup_audit_20260415.json + .md to scripts/logs/.
  5. Print summary for Sasha report.

HARD CONSTRAINT: No writes to the sheet. Read-only.

Run from project root:
    PYTHONPATH=. python scripts/carrier_cleanup_audit_20260415.py [--no-playwright]
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from app.sheets import read_range
from app.config import get_settings

# ── Output paths ──────────────────────────────────────────────────────────────
LOGS_DIR = Path("C:/Users/Owner/brokerops-ai/scripts/logs")
JSON_OUT = LOGS_DIR / "carrier_cleanup_audit_20260415.json"
MD_OUT   = LOGS_DIR / "carrier_cleanup_audit_20260415.md"
LOG_OUT  = LOGS_DIR / "carrier_cleanup_audit_20260415.log"

LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_OUT), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("carrier_cleanup_audit")

# ── Sheet constants ───────────────────────────────────────────────────────────
MAIN_TAB   = "Carrier Database"
MAIN_RANGE = f"'{MAIN_TAB}'!A1:ZZ"

# ── Playwright rate limit: 6 req/min per host = 10s minimum spacing ───────────
PLAYWRIGHT_RATE_SEC = 11.0
PLAYWRIGHT_HOST_TRACKER: dict[str, float] = {}

# ── Heuristic pattern sets ────────────────────────────────────────────────────

# Each entry is (pattern_string, flag_reason).
# Patterns are matched case-insensitively as substrings unless preceded by \b.

HARD_REMOVE_PATTERNS: list[tuple[str, str]] = [
    # Towing / recovery
    (r"\btowing\b",                "towing"),
    (r"\btow\s",                   "towing"),
    (r"\bwrecker\b",               "towing"),
    (r"\brecovery\b",              "vehicle_recovery"),

    # Excavating / earthwork / paving
    (r"\bexcavat",                 "excavating"),
    (r"\bgrading\b",               "excavating"),
    (r"\bpaving\b",                "paving"),
    (r"\basphalt\b",               "paving"),
    (r"\bconcrete\b",              "concrete"),
    (r"\bready.?mix\b",            "concrete"),

    # Passenger / transit
    (r"\bbus\s",                   "passenger_bus"),
    (r"\bcoach\b",                 "passenger_bus"),
    (r"\btransit\b",               "passenger_transit"),
    (r"\bshuttle\b",               "passenger_shuttle"),
    (r"\btours\b",                 "passenger_tours"),
    (r"\bcharter\b",               "passenger_charter"),

    # Heavy haul / rigging / crane (specialty, not our lanes)
    (r"\bheavy.?haul\b",           "heavy_haul_rigging"),
    (r"\brigging\b",               "heavy_haul_rigging"),
    (r"\bcrane\b",                 "heavy_haul_rigging"),

    # Waste / sanitation
    (r"\bwaste\b",                 "waste"),
    (r"\bgarbage\b",               "waste"),
    (r"\brefuse\b",                "waste"),
    (r"\bdisposal\b",              "waste"),
    (r"\bsanitation\b",            "waste"),
    (r"\bseptic\b",                "waste"),
    (r"\bporta\b",                 "waste"),
    (r"\bdumpster\b",              "waste"),

    # Landscape / lawn / tree
    (r"\blandscap",                "landscaping"),
    (r"\blawn\b",                  "landscaping"),
    (r"\btree.?service\b",         "landscaping"),
    (r"\barborist\b",              "landscaping"),
    (r"\bmulch\b",                 "landscaping"),
    (r"\bnursery\b",               "landscaping"),

    # Oilfield services
    (r"\boilfield\b",              "oilfield_services"),
    (r"\bfrac\b",                  "oilfield_services"),
    (r"\bwell.?service\b",         "oilfield_services"),
    (r"\bdrilling\b",              "oilfield_services"),

    # Logging / timber
    (r"\blogging\b",               "logging_timber"),
    (r"\btimber\b",                "logging_timber"),
    (r"\bforestry\b",              "logging_timber"),
    (r"\bpulpwood\b",              "logging_timber"),

    # Auto transport (different lane + equipment)
    (r"\bauto.?transport",         "auto_transport"),
    (r"\bcar.?hauler\b",           "auto_transport"),
    (r"\bvehicle.?transport",      "auto_transport"),

    # Moving / HHG
    (r"\bmoving\b",                "moving_hhg"),
    (r"\bvan.?lines\b",            "moving_hhg"),
    (r"\brelocation\b",            "moving_hhg"),
    (r"\bmovers\b",                "moving_hhg"),

    # Roll-off
    (r"\broll.?off\b",             "roll_off"),
    (r"\bhook.?lift\b",            "roll_off"),

    # Livestock
    (r"\blivestock\b",             "livestock"),
    (r"\bcattle\b",                "livestock"),
    (r"\bhorse\b",                 "livestock"),
    (r"\bequine\b",                "livestock"),

    # Private fleet / manufacturer markers
    (r"\bmanufactur",              "private_fleet_manufacturer"),
    (r"\s+mfg\s",                  "private_fleet_manufacturer"),
    (r"\bequipment\s+co\b",        "private_fleet_manufacturer"),
    (r"\bmachinery\b",             "private_fleet_manufacturer"),
]

# Compile patterns once
_HARD_REMOVE_RE: list[tuple[re.Pattern, str]] = [
    (re.compile(pat, re.IGNORECASE), reason)
    for pat, reason in HARD_REMOVE_PATTERNS
]

FUEL_PATTERNS: list[tuple[str, str]] = [
    (r"\bfuel\b",                  "fuel"),
    (r"\bpetroleum\b",             "fuel"),
    (r"\bgasoline\b",              "fuel"),
    (r"\bgas\s+co\b",              "fuel"),
    (r"\boil\s+co\b",              "fuel"),
    (r"\benergy\b",                "fuel"),
    (r"\bpropane\b",               "fuel_propane"),
    (r"\blpg\b",                   "fuel_propane"),
    (r"\btanker\b",                "fuel"),
]

_FUEL_RE: list[tuple[re.Pattern, str]] = [
    (re.compile(pat, re.IGNORECASE), reason)
    for pat, reason in FUEL_PATTERNS
]

# For-hire rescue markers — if name contains one of these, nudge toward rescue
FOR_HIRE_RESCUE_MARKERS = re.compile(
    r"\b(trucking|transport|logistics|freight|express|carriers?|hauling|dispatch|delivery|cargo|lines)\b",
    re.IGNORECASE,
)


def _match_patterns(
    text: str, patterns: list[tuple[re.Pattern, str]]
) -> list[str]:
    """Return list of matched reasons."""
    reasons = []
    for pattern, reason in patterns:
        if pattern.search(text):
            reasons.append(reason)
    return reasons


def _extract_website(notes: str) -> Optional[str]:
    """Pull Website: ... out of the Notes cell if present."""
    m = re.search(r"Website:\s*(https?://\S+)", notes or "", re.IGNORECASE)
    return m.group(1).rstrip(";,") if m else None


def _brokersnapshot_url(dot: str) -> Optional[str]:
    return f"https://brokersnapshot.com/Company?dot={dot}" if dot else None


def _rate_limit_wait(host: str) -> None:
    """Block until we're within 6 req/min for this host."""
    last = PLAYWRIGHT_HOST_TRACKER.get(host, 0.0)
    elapsed = time.time() - last
    if elapsed < PLAYWRIGHT_RATE_SEC:
        wait = PLAYWRIGHT_RATE_SEC - elapsed
        logger.info("Rate limiting %s — sleeping %.1fs", host, wait)
        time.sleep(wait)
    PLAYWRIGHT_HOST_TRACKER[host] = time.time()


def fetch_website_snippet(
    fetcher,
    url: str,
    dot: str,
) -> dict:
    """Fetch a URL, return {title, h1, snippet, blocked, error}."""
    from urllib.parse import urlparse
    host = urlparse(url).netloc

    _rate_limit_wait(host)

    try:
        page = fetcher.fetch_page(url)
    except Exception as exc:
        logger.warning("Playwright fetch error for %s: %s", url, exc)
        return {"title": None, "h1": None, "snippet": None, "blocked": False, "error": str(exc)}

    if page.get("blocked"):
        logger.warning("Playwright blocked on %s: %s", url, page.get("block_reason"))
        return {"title": page.get("title"), "h1": None, "snippet": None,
                "blocked": True, "error": page.get("block_reason")}

    title = page.get("title") or ""
    text  = page.get("text") or ""

    # Extract first h1 from html
    h1_match = re.search(r"<h1[^>]*>(.*?)</h1>", page.get("html") or "", re.IGNORECASE | re.DOTALL)
    h1 = re.sub(r"<[^>]+>", "", h1_match.group(1)).strip() if h1_match else None

    snippet = text[:500].replace("\n", " ").strip()

    return {"title": title, "h1": h1, "snippet": snippet, "blocked": False, "error": None}


def rescue_check(name: str, flag_reasons: list[str], web_result: Optional[dict]) -> tuple[bool, str]:
    """
    Return (should_rescue, rescue_note).
    Rescue if the website content suggests legit for-hire freight ops.
    """
    if not web_result:
        return False, ""

    blob = " ".join(filter(None, [
        web_result.get("title"),
        web_result.get("h1"),
        web_result.get("snippet"),
    ])).upper()

    if not blob.strip():
        return False, ""

    # Strong rescue signals
    RESCUE_POSITIVE = [
        "DRY VAN", "FLATBED", "BOX TRUCK", "REEFER", "REFRIGERATED",
        "GENERAL FREIGHT", "FOR HIRE", "FOR-HIRE", "COMMON CARRIER",
        "TRUCKLOAD", "LTL", "BROKER", "FREIGHT BROKER",
        "COMMON AUTHORITY",
    ]
    # Strong disqualifier signals
    RESCUE_NEGATIVE = [
        "PRIVATE(PROPERTY)", "PASSENGER", "PASSENGERS",
        "GARBAGE", "REFUSE", "TOWING SERVICE", "AUTO SALVAGE",
        "PRIVATE PASSENGER",
    ]

    pos_hits = [t for t in RESCUE_POSITIVE if t in blob]
    neg_hits = [t for t in RESCUE_NEGATIVE if t in blob]

    if neg_hits:
        return False, f"website confirms non-target: {', '.join(neg_hits)}"
    if pos_hits:
        return True, f"website shows freight ops: {', '.join(pos_hits)}"

    # Ambiguous — for-hire marker in name helps
    if FOR_HIRE_RESCUE_MARKERS.search(name):
        return True, "for-hire marker in name + no disqualifying website signal"

    return False, "no clear rescue signal from website"


def classify_carrier(carrier: dict) -> Optional[dict]:
    """
    Return an audit entry dict if the carrier is flagged, else None.
    """
    name = (
        carrier.get("Company Name")
        or carrier.get("Legal_Name")
        or carrier.get("Legal Name")
        or ""
    ).strip()

    if not name:
        return None

    # Check FUEL first (keep separately — higher priority than hard-remove)
    fuel_reasons = _match_patterns(name, _FUEL_RE)
    hard_reasons = _match_patterns(name, _HARD_REMOVE_RE)

    if not fuel_reasons and not hard_reasons:
        return None  # clean carrier

    dot = (
        carrier.get("DOT Number")
        or carrier.get("DOT_Number")
        or carrier.get("DOT")
        or ""
    ).strip()

    notes = carrier.get("Notes") or carrier.get("Internal_Notes") or ""
    website = _extract_website(notes) or carrier.get("Website") or None

    state = carrier.get("State") or carrier.get("state") or ""
    city  = carrier.get("City") or carrier.get("city") or ""
    email = carrier.get("Contact Email") or carrier.get("Primary_Email") or ""
    phone = carrier.get("Contact Phone") or carrier.get("Primary_Phone") or ""
    equip = carrier.get("Equipment Types") or carrier.get("Equipment_Type") or ""
    units = carrier.get("Fleet Size") or carrier.get("Power_Units") or ""

    if fuel_reasons:
        # Could also have hard-remove patterns, but fuel wins — KEEP_FUEL
        return {
            "dot": dot,
            "legal_name": name,
            "dba": "",
            "state": state,
            "city": city,
            "website": website or "",
            "equipment": equip,
            "power_units": units,
            "primary_email": email,
            "primary_phone": phone,
            "flag_reason": " | ".join(fuel_reasons),
            "website_snippet": None,
            "proposed_action": "keep_fuel",
            "confidence": "high",
            "notes": "Fuel/propane company — Derek has separate strategy; do not remove",
            "_raw": carrier,
        }

    # Hard-remove path
    has_for_hire = bool(FOR_HIRE_RESCUE_MARKERS.search(name))
    confidence = "high" if not has_for_hire else "medium"

    return {
        "dot": dot,
        "legal_name": name,
        "dba": "",
        "state": state,
        "city": city,
        "website": website or "",
        "equipment": equip,
        "power_units": units,
        "primary_email": email,
        "primary_phone": phone,
        "flag_reason": " | ".join(hard_reasons),
        "website_snippet": None,
        "proposed_action": "remove",
        "confidence": confidence,
        "notes": f"for-hire marker in name: {has_for_hire}",
        "_raw": carrier,
    }


def run_playwright_verification(flagged: list[dict], use_playwright: bool) -> list[dict]:
    """
    For flagged entries that have a website or DOT (for brokersnapshot fallback),
    hit the page and decide rescue vs. confirm.
    Mutates entries in place, returns them.
    """
    if not use_playwright:
        logger.info("Playwright disabled — skipping web verification step")
        return flagged

    # Only run verification on REMOVE entries that are medium confidence
    # AND have a website (direct) or DOT (brokersnapshot fallback).
    targets = [
        e for e in flagged
        if e["proposed_action"] == "remove"
        and (e.get("website") or e.get("dot"))
        and e["confidence"] in ("medium", "high")
    ]

    # Cap at 30 to stay within session limits; sort medium-confidence first
    targets = sorted(targets, key=lambda x: 0 if x["confidence"] == "medium" else 1)
    targets = targets[:30]

    logger.info("Playwright verification: %d candidates (cap 30)", len(targets))
    blocked_count = 0

    try:
        from app.enrichment.playwright_fetcher import PlaywrightFetcher
    except ImportError as exc:
        logger.error("Cannot import PlaywrightFetcher: %s — skipping verification", exc)
        return flagged

    with PlaywrightFetcher(fresh_context_per_request=True) as fetcher:
        for i, entry in enumerate(targets):
            url = entry.get("website") or _brokersnapshot_url(entry.get("dot", ""))
            if not url:
                continue

            logger.info(
                "[%d/%d] Verifying %s (DOT %s) — %s",
                i + 1, len(targets),
                entry["legal_name"], entry["dot"], url,
            )

            web_result = fetch_website_snippet(fetcher, url, entry["dot"])

            if web_result.get("blocked"):
                blocked_count += 1
                entry["website_snippet"] = f"[BLOCKED: {web_result.get('error')}]"
                entry["notes"] += f"; playwright_blocked={web_result.get('error')}"
                logger.warning("Blocked (%d total blocked)", blocked_count)
                # If >5% blocked out of total flagged, abort remaining playwright
                if blocked_count > max(1, len(flagged) * 0.05):
                    logger.warning(
                        "Blocked rate exceeded 5%% of flagged set (%d/%d) — stopping Playwright",
                        blocked_count, len(flagged),
                    )
                    break
                continue

            if web_result.get("error"):
                entry["website_snippet"] = f"[ERROR: {web_result['error']}]"
                entry["notes"] += f"; playwright_error={web_result['error']}"
                continue

            snippet = web_result.get("snippet") or ""
            entry["website_snippet"] = snippet

            should_rescue, rescue_note = rescue_check(
                entry["legal_name"], entry["flag_reason"].split(" | "), web_result
            )

            if should_rescue:
                entry["proposed_action"] = "rescue_keep"
                entry["confidence"] = "medium"
                entry["notes"] += f"; RESCUE: {rescue_note}"
                logger.info("  → RESCUED: %s", rescue_note)
            else:
                entry["notes"] += f"; website_check: {rescue_note or 'no rescue signal'}"
                logger.info("  → CONFIRMED REMOVE: %s", rescue_note or "no rescue signal")

    return flagged


def build_json_output(flagged: list[dict]) -> list[dict]:
    """Build clean JSON records (strip internal _raw key)."""
    out = []
    for e in flagged:
        out.append({
            "dot":              e["dot"],
            "legal_name":       e["legal_name"],
            "dba":              e.get("dba", ""),
            "state":            e.get("state", ""),
            "city":             e.get("city", ""),
            "website":          e.get("website", ""),
            "equipment":        e.get("equipment", ""),
            "power_units":      e.get("power_units", ""),
            "primary_email":    e.get("primary_email", ""),
            "primary_phone":    e.get("primary_phone", ""),
            "flag_reason":      e["flag_reason"],
            "website_snippet":  e.get("website_snippet"),
            "proposed_action":  e["proposed_action"],
            "confidence":       e["confidence"],
            "notes":            e["notes"],
        })
    return out


def _top_examples(items: list[dict], n: int = 10) -> str:
    lines = []
    for e in items[:n]:
        lines.append(
            f"- DOT {e['dot'] or '—'} | {e['legal_name']} | {e['state']} | "
            f"{e['flag_reason']} | {e['confidence']}"
        )
    return "\n".join(lines) if lines else "_none_"


def build_markdown(
    total_scanned: int,
    flagged: list[dict],
    playwright_blocked: int,
    run_ts: str,
) -> str:
    removes   = [e for e in flagged if e["proposed_action"] == "remove"]
    fuels     = [e for e in flagged if e["proposed_action"] == "keep_fuel"]
    rescues   = [e for e in flagged if e["proposed_action"] == "rescue_keep"]

    # Count by flag_reason (first token)
    reason_counts: dict[str, int] = {}
    for e in flagged:
        first_reason = e["flag_reason"].split(" | ")[0]
        reason_counts[first_reason] = reason_counts.get(first_reason, 0) + 1

    ambiguous = [e for e in flagged if e["confidence"] == "medium"]

    lines = [
        f"# Carrier Cleanup Audit — 2026-04-15",
        f"_Generated {run_ts}_",
        "",
        "## Summary",
        "",
        f"| Metric | Count |",
        f"|---|---|",
        f"| Total carriers scanned | {total_scanned} |",
        f"| Total flagged | {len(flagged)} |",
        f"| **Proposed REMOVE** | **{len(removes)}** |",
        f"| **Keep as FUEL group** | **{len(fuels)}** |",
        f"| **Rescued (keep, false positive)** | **{len(rescues)}** |",
        f"| Ambiguous (medium confidence) | {len(ambiguous)} |",
        f"| Playwright blocks encountered | {playwright_blocked} |",
        "",
        "## Flag Reason Breakdown",
        "",
        "| Reason | Count |",
        "|---|---|",
    ]

    for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
        lines.append(f"| {reason} | {count} |")

    lines += [
        "",
        "## Proposed REMOVE — Top 10 Examples",
        "",
        _top_examples(removes),
        "",
        "## Keep as FUEL Group — Top 10 Examples",
        "",
        _top_examples(fuels),
        "",
        "## Rescued (false positives) — All",
        "",
        _top_examples(rescues, n=50),
        "",
        "## Ambiguous — Needs Derek's Eyeball",
        "",
        "These are medium-confidence flags where the name has both a non-target keyword",
        "AND a for-hire freight marker (e.g., 'XYZ Transit Freight LLC').",
        "Playwright was run on these where possible; results shown in notes.",
        "",
        "| DOT | Name | State | Flag Reason | Confidence | Website Snippet | Notes |",
        "|---|---|---|---|---|---|---|",
    ]

    for e in ambiguous:
        snippet = (e.get("website_snippet") or "")[:120].replace("|", "\\|").replace("\n", " ")
        notes   = (e.get("notes") or "")[:200].replace("|", "\\|").replace("\n", " ")
        lines.append(
            f"| {e['dot']} | {e['legal_name']} | {e['state']} | "
            f"{e['flag_reason']} | {e['confidence']} | {snippet} | {notes} |"
        )

    lines += [
        "",
        "---",
        f"_Audit run {run_ts}. DO NOT apply changes until Derek approves via Sasha._",
    ]

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Carrier DB cleanup audit — read-only")
    parser.add_argument("--no-playwright", action="store_true", help="Skip Playwright verification")
    args = parser.parse_args()

    use_playwright = not args.no_playwright
    run_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info("=== Carrier Cleanup Audit starting %s ===", run_ts)
    logger.info("Playwright enabled: %s", use_playwright)

    # ── Step 1: Pull main tab ─────────────────────────────────────────────────
    settings = get_settings()
    sheet_id = settings.CARRIER_MASTER_SHEET_ID

    if not sheet_id:
        logger.error("CARRIER_MASTER_SHEET_ID not set in .env — cannot continue")
        raise SystemExit(1)

    logger.info("Reading main tab '%s' from sheet %s ...", MAIN_TAB, sheet_id)
    try:
        rows = read_range(sheet_id, MAIN_RANGE)
    except Exception as exc:
        logger.error("Google Sheets auth/read failed: %s", exc)
        logger.error("Stopping — DO NOT re-auth blindly. Check OAuth token / .env.")
        raise SystemExit(1)

    if not rows:
        logger.error("Main tab returned no rows")
        raise SystemExit(1)

    headers = rows[0]
    data    = rows[1:]
    logger.info("Fetched %d carrier rows (header: %d columns)", len(data), len(headers))

    # ── Step 2: Classify each carrier ────────────────────────────────────────
    carriers = []
    for row in data:
        padded = row + [""] * (len(headers) - len(row))
        carriers.append(dict(zip(headers, padded)))

    flagged: list[dict] = []
    for carrier in carriers:
        entry = classify_carrier(carrier)
        if entry:
            flagged.append(entry)

    logger.info(
        "Classification complete: %d / %d flagged",
        len(flagged), len(carriers),
    )

    # ── Step 3: Playwright verification ──────────────────────────────────────
    playwright_blocked = 0
    if use_playwright and flagged:
        flagged_before = len([e for e in flagged if e["proposed_action"] == "remove"])
        flagged = run_playwright_verification(flagged, use_playwright=True)
        playwright_blocked = sum(
            1 for e in flagged
            if "playwright_blocked" in e.get("notes", "")
        )
    else:
        logger.info("Playwright step skipped")

    # ── Step 4: Write outputs ─────────────────────────────────────────────────
    json_records = build_json_output(flagged)
    JSON_OUT.write_text(json.dumps(json_records, indent=2), encoding="utf-8")
    logger.info("Wrote JSON: %s (%d records)", JSON_OUT, len(json_records))

    md_content = build_markdown(
        total_scanned=len(carriers),
        flagged=json_records,
        playwright_blocked=playwright_blocked,
        run_ts=run_ts,
    )
    MD_OUT.write_text(md_content, encoding="utf-8")
    logger.info("Wrote Markdown: %s", MD_OUT)

    # ── Step 5: Summary for Sasha ─────────────────────────────────────────────
    removes   = [e for e in json_records if e["proposed_action"] == "remove"]
    fuels     = [e for e in json_records if e["proposed_action"] == "keep_fuel"]
    rescues   = [e for e in json_records if e["proposed_action"] == "rescue_keep"]
    ambiguous = [e for e in json_records if e["confidence"] == "medium"]

    print("\n" + "="*60)
    print("CARRIER CLEANUP AUDIT — SUMMARY")
    print("="*60)
    print(f"Total scanned:          {len(carriers)}")
    print(f"Total flagged:          {len(json_records)}")
    print(f"  -> Proposed REMOVE:    {len(removes)}")
    print(f"  -> Keep as FUEL group: {len(fuels)}")
    print(f"  -> Rescued (keep):     {len(rescues)}")
    print(f"  -> Medium confidence:  {len(ambiguous)}")
    print(f"Playwright blocks:      {playwright_blocked}")
    print()
    print(f"JSON output:  {JSON_OUT}")
    print(f"MD output:    {MD_OUT}")
    print(f"Log:          {LOG_OUT}")
    print("="*60)

    # Flag-reason breakdown
    reason_counts: dict[str, int] = {}
    for e in json_records:
        r = e["flag_reason"].split(" | ")[0]
        reason_counts[r] = reason_counts.get(r, 0) + 1
    print("\nFlag reason counts:")
    for reason, cnt in sorted(reason_counts.items(), key=lambda x: -x[1]):
        print(f"  {reason:<35} {cnt}")

    print("\nTop REMOVE examples:")
    for e in removes[:10]:
        print(f"  DOT {e['dot'] or '—':<10} {e['legal_name'][:50]:<52} {e['state']}  [{e['flag_reason']}]")

    if fuels:
        print("\nFUEL group examples:")
        for e in fuels[:10]:
            print(f"  DOT {e['dot'] or '—':<10} {e['legal_name'][:50]:<52} {e['state']}")

    if rescues:
        print("\nRescued carriers:")
        for e in rescues:
            print(f"  DOT {e['dot'] or '—':<10} {e['legal_name'][:50]:<52} {e['state']}  notes: {e['notes'][:80]}")

    logger.info("=== Audit complete ===")


if __name__ == "__main__":
    main()
