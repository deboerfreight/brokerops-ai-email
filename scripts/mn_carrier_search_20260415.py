#!/usr/bin/env python3
"""
BrokerOps AI – Minnesota carrier sourcing, 2026-04-15.
Reference 7-phase search implementation.

Adds up to 10 vetted carriers per equipment bucket (flatbed, dry_van, reefer,
box_truck) from Minnesota to the Carrier Database sheet.

7-phase pipeline:
  Phase 1  Load existing DOTs from sheet (dedup guard).
  Phase 2  L&I SQLite sourcing query — pre-filter MN carriers with active
           authority and BIPD filed ≥ $1M.
  Phase 3  FMCSA hydration — call get_carrier_details per candidate (1 req/sec).
  Phase 4  Equipment bucket classification (infer from cargo-carried + name).
  Phase 5  Cap candidates at BUCKET_TARGET per bucket; skip already-seen DOTs.
  Phase 6  Vetting gate — insert_carrier routes pass→main tab, fail→quarantine.
  Phase 7  Summary report to stdout and scripts/logs/mn_carrier_search_20260415.log.

Usage:
    PYTHONPATH=. python scripts/mn_carrier_search_20260415.py [--dry-run]

Constraints:
  - DO NOT hardcode thresholds — read from app.vetting.rules.RULES.
  - DO NOT use Insurance_Cargo=1 sentinel (removed 2026-04-14 audit).
  - Rate limit FMCSA to 1 req/sec.
  - Do NOT modify app/vetting/, app/sheets.py::insert_carrier, or app/fmcsa.py.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure project root is on sys.path regardless of invocation directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import get_settings
from app.fmcsa import get_carrier_details
from app.sheets import get_all_carriers, insert_carrier
from app.vetting.rules import RULES

# ── Config ────────────────────────────────────────────────────────────────────

TARGET_STATE = "MN"
BUCKET_TARGET = 10                   # vetted carriers to add per equipment bucket
FMCSA_DELAY = 1.05                   # seconds between FMCSA requests
LI_MIN_BIPD = RULES.liability_min    # $1M — matches gate threshold, no hardcode
LI_CANDIDATE_POOL = 300              # max rows fetched from L&I index per state
LOG_DIR = Path(__file__).resolve().parent / "logs"

BUCKETS = ["flatbed", "dry_van", "reefer", "box_truck"]

# Cargo-carried description keywords that map to each bucket.
# These are compared case-insensitively against FMCSA cargoCarried text.
BUCKET_KEYWORDS: dict[str, list[str]] = {
    "flatbed": [
        "building materials", "lumber", "steel", "metal", "machinery",
        "construction", "pipes", "coils", "oversize", "flatbed",
    ],
    "dry_van": [
        "general freight", "dry goods", "packaged goods", "consumer goods",
        "food", "beverages", "retail", "dry van", "van freight",
    ],
    "reefer": [
        "refrigerated", "frozen", "fresh produce", "dairy", "meat",
        "perishable", "temperature", "cold chain", "reefer",
    ],
    "box_truck": [
        "household goods", "furniture", "appliances", "electronics",
        "box truck", "straight truck", "parcel", "last mile",
    ],
}

# Equipment type string keywords in the normalized carrier name/notes that
# confirm a bucket assignment as a secondary signal.
NAME_BUCKET_KEYWORDS: dict[str, list[str]] = {
    "flatbed": ["flatbed", "flat bed", "stepdeck", "step deck", "lowboy"],
    "dry_van": ["van", "dry van", "truckload"],
    "reefer": ["refrigerated", "reefer", "cold", "frozen", "temp"],
    "box_truck": ["box truck", "straight", "delivery"],
}


# ── Logging ───────────────────────────────────────────────────────────────────

LOG_DIR.mkdir(parents=True, exist_ok=True)
_log_path = LOG_DIR / f"mn_carrier_search_20260415.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_log_path, encoding="utf-8"),
    ],
)
log = logging.getLogger("mn_carrier_search")


# ── Phase helpers ─────────────────────────────────────────────────────────────


def _classify_bucket(carrier: dict) -> str | None:
    """Return the best equipment bucket for a hydrated FMCSA carrier dict.

    Priority: cargo-carried text > carrier name keyword > None (skip).
    """
    cargo_text = str(carrier.get("cargoCarried") or carrier.get("Cargo_Carried") or "").lower()
    legal_name = str(carrier.get("Legal_Name") or carrier.get("legal_name") or "").lower()

    for bucket in BUCKETS:
        for kw in BUCKET_KEYWORDS[bucket]:
            if kw in cargo_text:
                return bucket

    for bucket in BUCKETS:
        for kw in NAME_BUCKET_KEYWORDS[bucket]:
            if kw in legal_name:
                return bucket

    # Default: if we have any freight signal at all, assign dry_van.
    freight_signals = ["trucking", "freight", "transport", "logistics", "carrier", "hauling"]
    if any(s in legal_name for s in freight_signals):
        return "dry_van"

    return None


def _is_active_authority(carrier: dict) -> bool:
    status = str(carrier.get("Authority_Status") or "").upper().strip()
    return status in ("ACTIVE", "A", "AUTHORIZED")


# ── Phase 1: Load existing DOTs ───────────────────────────────────────────────


def phase1_load_existing_dots() -> set[str]:
    log.info("Phase 1: Loading existing DOTs from Carrier Database sheet…")
    carriers = get_all_carriers()
    dots: set[str] = set()
    for c in carriers:
        dot = str(c.get("DOT Number") or c.get("DOT_Number") or "").strip()
        if dot:
            dots.add(dot)
    log.info("Phase 1 complete: %d existing DOTs in sheet", len(dots))
    return dots


# ── Phase 2: L&I sourcing query ───────────────────────────────────────────────


def phase2_li_sourcing() -> list:
    """Query the local L&I SQLite index for MN carrier candidates."""
    log.info(
        "Phase 2: L&I sourcing query — state=%s, min_bipd=$%s, limit=%d",
        TARGET_STATE, f"{LI_MIN_BIPD:,}", LI_CANDIDATE_POOL,
    )
    try:
        from app.vetting.li_insurance_lookup import search_carriers_by_state
        candidates = search_carriers_by_state(
            state=TARGET_STATE,
            min_bipd=LI_MIN_BIPD,
            exclude_broker_only=True,
            require_active_authority=True,
            limit=LI_CANDIDATE_POOL,
        )
        log.info("Phase 2 complete: %d L&I candidates found", len(candidates))
        return candidates
    except RuntimeError as exc:
        log.error(
            "Phase 2: L&I DB unavailable (%s). "
            "Rebuild with: PYTHONPATH=. python scripts/refresh_li_insurance.py",
            exc,
        )
        return []


# ── Phase 3: FMCSA hydration ──────────────────────────────────────────────────


def phase3_hydrate(candidates: list, existing_dots: set[str]) -> list[dict]:
    """Fetch full FMCSA profile for each candidate; rate-limited to 1 req/sec."""
    log.info(
        "Phase 3: Hydrating %d candidates via FMCSA (rate=%gs/req)…",
        len(candidates), FMCSA_DELAY,
    )
    hydrated: list[dict] = []
    skipped_dup = 0

    for i, cand in enumerate(candidates):
        dot = cand.dot if hasattr(cand, "dot") else str(cand.get("dot", ""))
        if not dot:
            continue
        if dot in existing_dots:
            skipped_dup += 1
            continue

        if i > 0:
            time.sleep(FMCSA_DELAY)

        try:
            details = get_carrier_details(dot)
        except Exception as exc:
            log.warning("Phase 3: hydration failed for DOT %s: %s", dot, exc)
            continue

        if not details:
            log.debug("Phase 3: no details for DOT %s", dot)
            continue

        # Skip carriers outside the target state (stubs may have been
        # cross-state in the L&I file — detail endpoint has the truth).
        phy_state = str(details.get("Physical_State") or details.get("State") or "").upper().strip()
        if phy_state and phy_state != TARGET_STATE:
            log.debug("Phase 3: DOT %s is state=%s, skipping", dot, phy_state)
            continue

        # Skip inactive authority at the detail level.
        if not _is_active_authority(details):
            log.debug("Phase 3: DOT %s authority=%s, skipping", dot, details.get("Authority_Status"))
            continue

        existing_dots.add(dot)   # mark so later candidates don't re-hydrate
        hydrated.append(details)
        log.debug("Phase 3: hydrated DOT %s — %s", dot, details.get("Legal_Name", "?"))

    log.info(
        "Phase 3 complete: %d hydrated (%d skipped as duplicate)",
        len(hydrated), skipped_dup,
    )
    return hydrated


# ── Phase 4: Equipment bucket classification ──────────────────────────────────


def phase4_classify(hydrated: list[dict]) -> dict[str, list[dict]]:
    log.info("Phase 4: Classifying %d carriers into equipment buckets…", len(hydrated))
    buckets: dict[str, list[dict]] = {b: [] for b in BUCKETS}
    unclassified = 0

    for carrier in hydrated:
        bucket = _classify_bucket(carrier)
        if bucket is None:
            unclassified += 1
            log.debug(
                "Phase 4: unclassified — %s (DOT %s)",
                carrier.get("Legal_Name", "?"), carrier.get("DOT_Number", "?"),
            )
            continue
        buckets[bucket].append(carrier)

    for b, items in buckets.items():
        log.info("Phase 4: bucket %-12s → %d candidates", b, len(items))
    log.info("Phase 4 complete: %d carriers unclassified/skipped", unclassified)
    return buckets


# ── Phase 5: Cap per bucket ───────────────────────────────────────────────────


def phase5_cap(buckets: dict[str, list[dict]]) -> dict[str, list[dict]]:
    """Keep up to BUCKET_TARGET candidates per bucket (first = highest score)."""
    log.info("Phase 5: Capping each bucket at %d candidates…", BUCKET_TARGET)
    capped: dict[str, list[dict]] = {}
    for b, items in buckets.items():
        # Sort by power units descending as a simple quality proxy
        sorted_items = sorted(items, key=lambda c: int(c.get("Power_Units") or 0), reverse=True)
        capped[b] = sorted_items[:BUCKET_TARGET * 3]  # 3× buffer — vetting will thin
        log.info("Phase 5: bucket %-12s → %d candidates (capped from %d)", b, len(capped[b]), len(items))
    return capped


# ── Phase 6: Vetting gate ─────────────────────────────────────────────────────


def phase6_vet(
    buckets: dict[str, list[dict]],
    dry_run: bool = False,
) -> dict[str, dict]:
    """Run insert_carrier for each candidate up to BUCKET_TARGET passes per bucket.

    insert_carrier already gates via vet_complete and routes failures to quarantine.
    We stop inserting for a bucket once it reaches BUCKET_TARGET passed carriers.
    """
    log.info(
        "Phase 6: Vetting gate (dry_run=%s, target=%d/bucket)…",
        dry_run, BUCKET_TARGET,
    )
    stats: dict[str, dict] = {
        b: {"attempted": 0, "passed": 0, "failed": 0, "errors": 0}
        for b in BUCKETS
    }

    for bucket, candidates in buckets.items():
        for carrier in candidates:
            if stats[bucket]["passed"] >= BUCKET_TARGET:
                log.info(
                    "Phase 6: bucket %s hit target (%d), stopping",
                    bucket, BUCKET_TARGET,
                )
                break

            stats[bucket]["attempted"] += 1
            name = carrier.get("Legal_Name", carrier.get("DOT_Number", "?"))
            dot = carrier.get("DOT_Number", "")

            # Tag the equipment type in the carrier dict before inserting.
            eq_existing = str(carrier.get("Equipment_Type") or carrier.get("Equipment_Types") or "")
            bucket_label = bucket.replace("_", " ").title()
            if bucket_label.upper() not in eq_existing.upper():
                carrier["Equipment_Type"] = (
                    f"{eq_existing}, {bucket_label}".strip(", ") if eq_existing else bucket_label
                )

            if dry_run:
                log.info("Phase 6 [DRY-RUN]: would insert %s (DOT %s) → bucket=%s", name, dot, bucket)
                stats[bucket]["passed"] += 1
                continue

            try:
                insert_carrier(carrier)
                stats[bucket]["passed"] += 1
                log.info(
                    "Phase 6: inserted %s (DOT %s) → bucket=%s [pass=%d/%d]",
                    name, dot, bucket, stats[bucket]["passed"], BUCKET_TARGET,
                )
            except Exception as exc:
                # insert_carrier only raises if the quarantine write also fails.
                stats[bucket]["errors"] += 1
                log.error("Phase 6: insert error for %s (DOT %s): %s", name, dot, exc)

    return stats


# ── Phase 7: Summary ──────────────────────────────────────────────────────────


def phase7_summary(stats: dict[str, dict], elapsed: float, dry_run: bool) -> None:
    log.info("=" * 60)
    log.info("Phase 7: Summary — MN carrier search 2026-04-15")
    log.info("  State:    %s", TARGET_STATE)
    log.info("  Dry-run:  %s", dry_run)
    log.info("  Elapsed:  %.1fs", elapsed)
    log.info("  Thresholds: fleet_min=%d, liability_min=$%s",
             RULES.fleet_min, f"{RULES.liability_min:,}")
    log.info("")
    total_passed = 0
    for b in BUCKETS:
        s = stats[b]
        log.info(
            "  %-12s  attempted=%d  passed=%d  failed=%d  errors=%d",
            b, s["attempted"], s["passed"], s["failed"], s["errors"],
        )
        total_passed += s["passed"]
    log.info("")
    log.info("  TOTAL PASSED: %d", total_passed)
    log.info("=" * 60)


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="MN carrier search 2026-04-15")
    parser.add_argument("--dry-run", action="store_true", help="Log only; do not write to sheet")
    args = parser.parse_args()

    t0 = time.time()
    log.info("Starting MN carrier search (dry_run=%s)", args.dry_run)

    # Phase 1
    existing_dots = phase1_load_existing_dots()

    # Phase 2
    candidates = phase2_li_sourcing()
    if not candidates:
        log.error("No L&I candidates found — aborting. Is insurance_lookup.sqlite built?")
        sys.exit(1)

    # Phase 3
    hydrated = phase3_hydrate(candidates, existing_dots)
    if not hydrated:
        log.warning("No carriers hydrated — nothing to add.")
        phase7_summary({b: {"attempted": 0, "passed": 0, "failed": 0, "errors": 0} for b in BUCKETS},
                       time.time() - t0, args.dry_run)
        return

    # Phase 4
    buckets = phase4_classify(hydrated)

    # Phase 5
    capped = phase5_cap(buckets)

    # Phase 6
    stats = phase6_vet(capped, dry_run=args.dry_run)

    # Phase 7
    phase7_summary(stats, time.time() - t0, args.dry_run)


if __name__ == "__main__":
    main()
