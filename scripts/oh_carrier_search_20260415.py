#!/usr/bin/env python3
"""
BrokerOps AI – Ohio carrier sourcing, 2026-04-15.
Mirrors the 7-phase reference implementation in mn_carrier_search_20260415.py.
Target: 10 vetted carriers per equipment bucket (flatbed, dry_van, reefer, box_truck).

Usage:
    PYTHONPATH=. python scripts/oh_carrier_search_20260415.py [--dry-run]

See mn_carrier_search_20260415.py for full inline documentation.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.fmcsa import get_carrier_details
from app.sheets import get_all_carriers, insert_carrier
from app.vetting.rules import RULES

# ── Config ────────────────────────────────────────────────────────────────────

TARGET_STATE = "OH"
BUCKET_TARGET = 10
FMCSA_DELAY = 1.05
LI_MIN_BIPD = RULES.liability_min
LI_CANDIDATE_POOL = 300
LOG_DIR = Path(__file__).resolve().parent / "logs"

BUCKETS = ["flatbed", "dry_van", "reefer", "box_truck"]

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

NAME_BUCKET_KEYWORDS: dict[str, list[str]] = {
    "flatbed": ["flatbed", "flat bed", "stepdeck", "step deck", "lowboy"],
    "dry_van": ["van", "dry van", "truckload"],
    "reefer": ["refrigerated", "reefer", "cold", "frozen", "temp"],
    "box_truck": ["box truck", "straight", "delivery"],
}

# ── Logging ───────────────────────────────────────────────────────────────────

LOG_DIR.mkdir(parents=True, exist_ok=True)
_log_path = LOG_DIR / "oh_carrier_search_20260415.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_log_path, encoding="utf-8"),
    ],
)
log = logging.getLogger("oh_carrier_search")


def _classify_bucket(carrier: dict) -> str | None:
    cargo_text = str(carrier.get("cargoCarried") or carrier.get("Cargo_Carried") or "").lower()
    legal_name = str(carrier.get("Legal_Name") or "").lower()
    for bucket in BUCKETS:
        for kw in BUCKET_KEYWORDS[bucket]:
            if kw in cargo_text:
                return bucket
    for bucket in BUCKETS:
        for kw in NAME_BUCKET_KEYWORDS[bucket]:
            if kw in legal_name:
                return bucket
    freight_signals = ["trucking", "freight", "transport", "logistics", "carrier", "hauling"]
    if any(s in legal_name for s in freight_signals):
        return "dry_van"
    return None


def _is_active_authority(carrier: dict) -> bool:
    return str(carrier.get("Authority_Status") or "").upper().strip() in ("ACTIVE", "A", "AUTHORIZED")


def phase1_load_existing_dots() -> set[str]:
    log.info("Phase 1: Loading existing DOTs from Carrier Database sheet…")
    carriers = get_all_carriers()
    dots = {str(c.get("DOT Number") or c.get("DOT_Number") or "").strip() for c in carriers}
    dots.discard("")
    log.info("Phase 1 complete: %d existing DOTs in sheet", len(dots))
    return dots


def phase2_li_sourcing() -> list:
    log.info("Phase 2: L&I sourcing query — state=%s, min_bipd=$%s, limit=%d",
             TARGET_STATE, f"{LI_MIN_BIPD:,}", LI_CANDIDATE_POOL)
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
        log.error("Phase 2: L&I DB unavailable (%s). "
                  "Rebuild with: PYTHONPATH=. python scripts/refresh_li_insurance.py", exc)
        return []


def phase3_hydrate(candidates: list, existing_dots: set[str]) -> list[dict]:
    log.info("Phase 3: Hydrating %d candidates via FMCSA (rate=%gs/req)…",
             len(candidates), FMCSA_DELAY)
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
            continue
        phy_state = str(details.get("Physical_State") or details.get("State") or "").upper().strip()
        if phy_state and phy_state != TARGET_STATE:
            continue
        if not _is_active_authority(details):
            continue
        existing_dots.add(dot)
        hydrated.append(details)
    log.info("Phase 3 complete: %d hydrated (%d skipped as duplicate)", len(hydrated), skipped_dup)
    return hydrated


def phase4_classify(hydrated: list[dict]) -> dict[str, list[dict]]:
    log.info("Phase 4: Classifying %d carriers into equipment buckets…", len(hydrated))
    buckets: dict[str, list[dict]] = {b: [] for b in BUCKETS}
    unclassified = 0
    for carrier in hydrated:
        bucket = _classify_bucket(carrier)
        if bucket is None:
            unclassified += 1
            continue
        buckets[bucket].append(carrier)
    for b, items in buckets.items():
        log.info("Phase 4: bucket %-12s → %d candidates", b, len(items))
    log.info("Phase 4 complete: %d unclassified", unclassified)
    return buckets


def phase5_cap(buckets: dict[str, list[dict]]) -> dict[str, list[dict]]:
    log.info("Phase 5: Capping each bucket at %d×3=%d candidates…", BUCKET_TARGET, BUCKET_TARGET * 3)
    return {
        b: sorted(items, key=lambda c: int(c.get("Power_Units") or 0), reverse=True)[: BUCKET_TARGET * 3]
        for b, items in buckets.items()
    }


def phase6_vet(buckets: dict[str, list[dict]], dry_run: bool = False) -> dict[str, dict]:
    log.info("Phase 6: Vetting gate (dry_run=%s, target=%d/bucket)…", dry_run, BUCKET_TARGET)
    stats: dict[str, dict] = {b: {"attempted": 0, "passed": 0, "failed": 0, "errors": 0} for b in BUCKETS}
    for bucket, candidates in buckets.items():
        for carrier in candidates:
            if stats[bucket]["passed"] >= BUCKET_TARGET:
                break
            stats[bucket]["attempted"] += 1
            name = carrier.get("Legal_Name", carrier.get("DOT_Number", "?"))
            dot = carrier.get("DOT_Number", "")
            eq_existing = str(carrier.get("Equipment_Type") or carrier.get("Equipment_Types") or "")
            bucket_label = bucket.replace("_", " ").title()
            if bucket_label.upper() not in eq_existing.upper():
                carrier["Equipment_Type"] = (
                    f"{eq_existing}, {bucket_label}".strip(", ") if eq_existing else bucket_label
                )
            if dry_run:
                log.info("Phase 6 [DRY-RUN]: would insert %s (DOT %s) → %s", name, dot, bucket)
                stats[bucket]["passed"] += 1
                continue
            try:
                insert_carrier(carrier)
                stats[bucket]["passed"] += 1
                log.info("Phase 6: inserted %s (DOT %s) → %s [%d/%d]",
                         name, dot, bucket, stats[bucket]["passed"], BUCKET_TARGET)
            except Exception as exc:
                stats[bucket]["errors"] += 1
                log.error("Phase 6: insert error for %s (DOT %s): %s", name, dot, exc)
    return stats


def phase7_summary(stats: dict[str, dict], elapsed: float, dry_run: bool) -> None:
    log.info("=" * 60)
    log.info("Phase 7: Summary — OH carrier search 2026-04-15")
    log.info("  State: %s | Dry-run: %s | Elapsed: %.1fs", TARGET_STATE, dry_run, elapsed)
    log.info("  Thresholds: fleet_min=%d, liability_min=$%s", RULES.fleet_min, f"{RULES.liability_min:,}")
    total = 0
    for b in BUCKETS:
        s = stats[b]
        log.info("  %-12s  attempted=%d  passed=%d  failed=%d  errors=%d",
                 b, s["attempted"], s["passed"], s["failed"], s["errors"])
        total += s["passed"]
    log.info("  TOTAL PASSED: %d", total)
    log.info("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description="OH carrier search 2026-04-15")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    t0 = time.time()
    log.info("Starting OH carrier search (dry_run=%s)", args.dry_run)
    existing_dots = phase1_load_existing_dots()
    candidates = phase2_li_sourcing()
    if not candidates:
        log.error("No L&I candidates — aborting. Is insurance_lookup.sqlite built?")
        sys.exit(1)
    hydrated = phase3_hydrate(candidates, existing_dots)
    if not hydrated:
        log.warning("No carriers hydrated — nothing to add.")
        phase7_summary({b: {"attempted": 0, "passed": 0, "failed": 0, "errors": 0} for b in BUCKETS},
                       time.time() - t0, args.dry_run)
        return
    buckets = phase4_classify(hydrated)
    capped = phase5_cap(buckets)
    stats = phase6_vet(capped, dry_run=args.dry_run)
    phase7_summary(stats, time.time() - t0, args.dry_run)


if __name__ == "__main__":
    main()
