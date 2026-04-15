# DEPRECATED 2026-04-15 — consolidated into prospect_carriers.py --state OH. See feedback_carrier_category_rules.md.
# Equivalent command: python -m scripts.prospect_carriers --state OH --buckets flatbed,dry_van,reefer,box_truck --limit 10
# Root cause for deprecation: this script called insert_carrier() directly without enforcing
# EXCLUDED_SERVICE_TYPE_PATTERNS, allowing towing/moving/excavating companies into the DB.
"""
Ohio carrier search — top 10 per equipment type (Flatbed / Dry Van / Reefer / Box Truck).

Pipeline (post-audit, 2026-04-15):
  Phase 1: Source via L&I SQLite (state=OH, BIPD >= RULES.liability_min,
           exclude broker-only, exclude private-fleet name patterns)
  Phase 2: Hydrate up to 300 candidates via QCMobile (1 req/sec)
  Phase 3: Partition into 4 buckets + score with app.fmcsa.score_carrier
  Phase 4: Write top 10 per bucket via app.sheets.insert_carrier (gate enforced)

This is the OH twin of scripts/tx_carrier_search_20260415.py. The only changes:
  - STATE = "OH"
  - REPORT_JSON path
  - Preferred_Lanes prefix OH_*
  - Internal_Notes label OH
  - Explicitly passes exclude_private_fleet_patterns=True (default; pinned for log clarity)

This run is the first real test of:
  1. The fixed `_normalize_carrier` that reads inspection counts directly from
     /carriers/{dot} (the BASICS endpoint was serving stale 2017 data).
  2. The L&I private-fleet name-pattern filter.
  3. The rate-based reefer rule firing on real inspection data instead of
     blanket-rejecting every reefer candidate (TX landed 0 reefer carriers).
"""
from __future__ import annotations

import io
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# Force UTF-8 stdout on Windows consoles.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from app.fmcsa import get_carrier_details, score_carrier
from app.sheets import (
    CARRIER_DB_RANGE,
    get_all_carriers,
    insert_carrier,
    read_range,
)
from app.config import get_settings
from app.vetting.li_insurance_lookup import search_carriers_by_state
from app.vetting.rules import RULES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("oh_search")
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

STATE = "OH"
SOURCING_LIMIT = 1500  # wide net — reefer bucket needs volume
HYDRATE_LIMIT = 300    # hard cap, ~5 min at 1 req/sec
TOP_N = 10
OVERFETCH_N = 15       # rank top 15 so vetting-gate rejections still leave us a full 10
QCMOBILE_SLEEP = 1.0

SCRIPT_DIR = Path(__file__).resolve().parent
LOG_DIR = SCRIPT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
REPORT_JSON = LOG_DIR / "oh_carrier_search_20260415.json"


# ── Helpers ─────────────────────────────────────────────────────────────────

def _load_seen_dots() -> set[str]:
    """Pre-seed from main tab + Quarantine tab so we skip anything we've
    already processed."""
    seen: set[str] = set()

    try:
        for c in get_all_carriers():
            dot = (c.get("DOT Number") or c.get("DOT_Number") or "").strip()
            if dot:
                seen.add(str(int(dot)) if dot.isdigit() else dot)
    except Exception as exc:
        logger.warning("main-tab dedup load failed: %s", exc)

    try:
        sid = get_settings().CARRIER_MASTER_SHEET_ID
        qrows = read_range(sid, "'Carrier Quarantine'!A:AK")
        if qrows and len(qrows) > 1:
            for r in qrows[1:]:
                if len(r) > 4:
                    dot = str(r[4]).strip()
                    if dot:
                        seen.add(str(int(dot)) if dot.isdigit() else dot)
    except Exception as exc:
        logger.warning("quarantine dedup load failed: %s", exc)

    return seen


def _title_case_name(name: str) -> str:
    if not name:
        return ""
    suffixes = {"LLC", "INC", "LTD", "LP", "LLP", "PLLC", "LC", "PC", "USA"}
    out = []
    for tok in name.split():
        clean = tok.strip(",.").upper()
        if clean in suffixes:
            out.append(clean)
        else:
            out.append(tok.capitalize())
    return " ".join(out)


def _bucket_flags(carrier: dict) -> dict[str, bool]:
    """Return which equipment buckets this carrier qualifies for."""
    eq = (carrier.get("Equipment_Types") or "").upper()
    cargo_carried = ""
    raw = carrier.get("_raw") or {}
    if isinstance(raw, dict):
        cargo_carried = str(raw.get("cargoCarried") or "").upper()
    units = int(carrier.get("Power_Units") or 0)

    flags = {
        "flatbed": "FLATBED" in eq,
        "dry_van": "DRY_VAN" in eq,
        "reefer": "REEFER" in eq,
        "box_truck": False,
    }

    # Box truck heuristic: general-freight cargo, fleet 3-25, not flatbed/reefer/tanker
    is_general = "GENERAL FREIGHT" in cargo_carried or "GEN FREIGHT" in cargo_carried
    has_other = any(t in eq for t in ("FLATBED", "REEFER", "TANKER"))
    if is_general and 3 <= units <= 25 and not has_other:
        flags["box_truck"] = True

    return flags


def _rank_bucket(entries: list[tuple[int, dict]]) -> list[dict]:
    """Rank a list of (score, carrier) by score desc, fleet desc, safety rating."""
    safety_rank = {"SATISFACTORY": 2, "NONE": 1, "": 1, "CONDITIONAL": 0}

    def key(item):
        score, c = item
        units = int(c.get("Power_Units") or 0)
        sr = (c.get("Safety_Rating") or "").upper()
        return (-score, -units, -safety_rank.get(sr, 1))

    return [c for _s, c in sorted(entries, key=key)]


def _fields_for_insert(carrier: dict, bucket_label: str) -> dict:
    """Build an insert_carrier payload following the post-audit
    prospect_carriers.enrich_and_store pattern.

    No `Insurance_Cargo = 1` sentinel — the canonical gate respects
    RULES.cargo_min = 0 and accepts blank cargo. Pass Insurance_Cargo raw.
    """
    legal = _title_case_name(carrier.get("Legal_Name") or "")
    dba = _title_case_name(carrier.get("DBA_Name") or "")
    today = datetime.utcnow().date().isoformat()
    return {
        "MC_Number": carrier.get("MC_Number", ""),
        "DOT_Number": carrier.get("DOT_Number", ""),
        "Legal_Name": legal,
        "DBA_Name": dba,
        "Primary_Phone": carrier.get("Contact_Phone", ""),
        "Equipment_Type": carrier.get("Equipment_Types", ""),
        "Preferred_Lanes": f"OH_{bucket_label.upper()}",
        "City": _title_case_name(carrier.get("City", "")),
        "State": carrier.get("State", ""),
        "ZIP": carrier.get("Zip", ""),
        "Authority_Status": carrier.get("Authority_Status", ""),
        "Authority_Verified_Date": today,
        "Authority_Source": "FMCSA",
        "On_Time_Score": str(carrier.get("_score", 0)),
        "Active": "TRUE",
        "Onboarding_Status": "PROSPECT",
        "Internal_Notes": (
            f"OH top-10 {bucket_label} search 2026-04-15. "
            f"Score={carrier.get('_score', 0)}. Fleet={carrier.get('Power_Units', 0)}."
        ),
        # Hydrated FMCSA fields required by the vetting gate
        "Power_Units": carrier.get("Power_Units", 0),
        "Driver_Count": carrier.get("Driver_Count", 0),
        "Insurance_Liability": carrier.get("Insurance_Liability", 0),
        # NO sentinel — raw value (see post-audit note in prospect_carriers.py)
        "Insurance_Cargo": carrier.get("Insurance_Cargo", 0),
        "Safety_Rating": carrier.get("Safety_Rating", ""),
        "Vehicle_OOS_Rate": carrier.get("Vehicle_OOS_Rate", 0),
        "Driver_OOS_Rate": carrier.get("Driver_OOS_Rate", 0),
        "Crash_Rate_Per100": carrier.get("Crash_Rate_Per100", 0),
        "Vehicle_Insp": carrier.get("Vehicle_Insp", 0),  # REQUIRED for new reefer rule
        "Vehicle_OOS_Insp": carrier.get("Vehicle_OOS_Insp", 0),
        "Equipment_Types": carrier.get("Equipment_Types", ""),
    }


def _summary_row(c: dict) -> dict:
    return {
        "dot": c.get("DOT_Number"),
        "name": c.get("Legal_Name"),
        "city": c.get("City"),
        "state": c.get("State"),
        "fleet": c.get("Power_Units"),
        "bipd": c.get("Insurance_Liability"),
        "safety": c.get("Safety_Rating"),
        "score": c.get("_score"),
        "phone": c.get("Contact_Phone"),
        "equipment": c.get("Equipment_Types"),
        "veh_oos_rate": c.get("Vehicle_OOS_Rate"),
        "veh_insp": c.get("Vehicle_Insp"),
    }


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> int:
    start = time.time()
    stats: dict = {
        "phase": {},
        "rules_snapshot": {
            "fleet_min": RULES.fleet_min,
            "liability_min": RULES.liability_min,
            "cargo_min": RULES.cargo_min,
            "vehicle_oos_max_pct": RULES.vehicle_oos_max_pct,
            "driver_oos_max_pct": RULES.driver_oos_max_pct,
            "crash_rate_max_per_100": RULES.crash_rate_max_per_100,
            "reefer_vehicle_oos_max_pct": RULES.reefer_vehicle_oos_max_pct,
            "reefer_min_inspection_count": RULES.reefer_min_inspection_count,
        },
        "buckets": {"flatbed": [], "dry_van": [], "reefer": [], "box_truck": []},
        "written_dots": [],
        "skipped_duplicates": 0,
        "hydrate_failed": 0,
        "score_rejected": [],
        "insert_errors": [],
        "reefer_diagnostic": {
            "would_have_passed_old_binary_rule": 0,
            "reefer_candidates_hydrated": 0,
            "pass_new_rate_rule": 0,
            "fail_new_rate_rule": 0,
            "needs_review_insufficient_insp": 0,
        },
    }

    # ── Phase 1: sourcing ───────────────────────────────────────────────────
    logger.info(
        "Phase 1: sourcing %s candidates from L&I SQLite (limit=%d, private_fleet_filter=ON)",
        STATE, SOURCING_LIMIT,
    )
    candidates = search_carriers_by_state(
        state=STATE,
        zip_prefixes=None,
        min_bipd=RULES.liability_min,  # canonical
        exclude_broker_only=True,
        exclude_private_fleet_patterns=True,  # NEW filter — pinned explicit
        limit=SOURCING_LIMIT,
    )
    stats["phase"]["sourced"] = len(candidates)
    logger.info("Phase 1 done: %d candidates", len(candidates))

    if len(candidates) < 300:
        logger.error(
            "STOP: fewer than 300 candidates (%d). Something's wrong. Aborting.",
            len(candidates),
        )
        stats["phase"]["aborted"] = "candidates_below_300"
        REPORT_JSON.write_text(json.dumps(stats, indent=2, default=str))
        return 1

    # ── Phase 2: hydrate ────────────────────────────────────────────────────
    logger.info("Phase 2: pre-loading seen_dots from main tab + Quarantine")
    seen_dots = _load_seen_dots()
    stats["phase"]["seen_preseeded"] = len(seen_dots)
    logger.info("seen_dots pre-seeded: %d", len(seen_dots))

    hydrated: list[dict] = []
    processed = 0
    for cand in candidates:
        if processed >= HYDRATE_LIMIT:
            break

        dot_raw = str(cand.dot).lstrip("0") or cand.dot
        if dot_raw in seen_dots or cand.dot in seen_dots:
            stats["skipped_duplicates"] += 1
            continue

        details = get_carrier_details(dot_raw)
        processed += 1

        if not details:
            stats["hydrate_failed"] += 1
        else:
            if not details.get("City"):
                details["City"] = cand.bus_city
            if not details.get("State"):
                details["State"] = cand.bus_state
            if not details.get("Zip"):
                details["Zip"] = cand.bus_zip
            hydrated.append(details)

        if processed % 25 == 0:
            logger.info(
                "Hydrated %d/%d (kept %d, skipped_dup %d, fail %d)",
                processed, HYDRATE_LIMIT, len(hydrated),
                stats["skipped_duplicates"], stats["hydrate_failed"],
            )

        time.sleep(QCMOBILE_SLEEP)

    stats["phase"]["hydrated"] = len(hydrated)
    stats["phase"]["hydrate_processed"] = processed
    logger.info("Phase 2 done: %d hydrated of %d processed", len(hydrated), processed)

    # ── Phase 3: partition + score ──────────────────────────────────────────
    logger.info("Phase 3: partition + score")
    fb_entries: list[tuple[int, dict]] = []
    dv_entries: list[tuple[int, dict]] = []
    rf_entries: list[tuple[int, dict]] = []
    bt_entries: list[tuple[int, dict]] = []

    for c in hydrated:
        s = score_carrier(c)
        if s < 0:
            stats["score_rejected"].append({
                "dot": c.get("DOT_Number"),
                "name": c.get("Legal_Name"),
                "reason": "hard_disqualified_by_score_carrier",
            })
            continue

        units = int(c.get("Power_Units") or 0)
        safety = (c.get("Safety_Rating") or "").upper()
        if units < RULES.fleet_min:
            stats["score_rejected"].append({
                "dot": c.get("DOT_Number"), "name": c.get("Legal_Name"),
                "reason": f"fleet<{RULES.fleet_min} (was {units})",
            })
            continue
        if safety in ("UNSATISFACTORY", "CONDITIONAL"):
            stats["score_rejected"].append({
                "dot": c.get("DOT_Number"), "name": c.get("Legal_Name"),
                "reason": f"safety={safety}",
            })
            continue
        if c.get("Authority_Status") != "ACTIVE":
            stats["score_rejected"].append({
                "dot": c.get("DOT_Number"), "name": c.get("Legal_Name"),
                "reason": f"authority={c.get('Authority_Status')}",
            })
            continue

        c["_score"] = s
        flags = _bucket_flags(c)
        if flags["flatbed"]:
            fb_entries.append((s, c))
        if flags["dry_van"]:
            dv_entries.append((s, c))
        if flags["reefer"]:
            rf_entries.append((s, c))
            # Reefer diagnostic: compare old binary rule vs new rate-based rule
            stats["reefer_diagnostic"]["reefer_candidates_hydrated"] += 1
            veh_oos_insp = int(c.get("Vehicle_OOS_Insp") or 0)
            veh_insp = int(c.get("Vehicle_Insp") or 0)
            veh_oos_rate = float(c.get("Vehicle_OOS_Rate") or 0)
            # Old binary rule: any vehicle OOS inspection = reject
            if veh_oos_insp == 0:
                stats["reefer_diagnostic"]["would_have_passed_old_binary_rule"] += 1
            # New rate-based rule outcome
            if veh_insp < RULES.reefer_min_inspection_count:
                stats["reefer_diagnostic"]["needs_review_insufficient_insp"] += 1
            elif veh_oos_rate > RULES.reefer_vehicle_oos_max_pct:
                stats["reefer_diagnostic"]["fail_new_rate_rule"] += 1
            else:
                stats["reefer_diagnostic"]["pass_new_rate_rule"] += 1
        if flags["box_truck"]:
            bt_entries.append((s, c))

    stats["phase"]["bucket_counts_before_rank"] = {
        "flatbed": len(fb_entries),
        "dry_van": len(dv_entries),
        "reefer": len(rf_entries),
        "box_truck": len(bt_entries),
    }
    logger.info(
        "Bucket counts (qualified): flatbed=%d dry_van=%d reefer=%d box_truck=%d",
        len(fb_entries), len(dv_entries), len(rf_entries), len(bt_entries),
    )

    top_flatbed = _rank_bucket(fb_entries)[:OVERFETCH_N]
    top_dry_van = _rank_bucket(dv_entries)[:OVERFETCH_N]
    top_reefer = _rank_bucket(rf_entries)[:OVERFETCH_N]
    top_box_truck = _rank_bucket(bt_entries)[:OVERFETCH_N]

    # ── Phase 4: write via insert_carrier ───────────────────────────────────
    logger.info(
        "Phase 4: writing top-%d per bucket via insert_carrier (overfetch %d)",
        TOP_N, OVERFETCH_N,
    )
    written: set[str] = set()  # DOTs already written this run (overlap dedup)
    bucket_write_stats: dict[str, dict] = {}

    def _write_bucket(bucket_name: str, rows: list[dict]) -> dict:
        bstats = {"attempted": 0, "inserted": 0, "overlap_reused": 0, "quarantined_or_error": 0}
        for c in rows:
            if bstats["inserted"] + bstats["overlap_reused"] >= TOP_N:
                break
            dot = str(c.get("DOT_Number", "") or "").strip()
            if not dot:
                continue
            if dot in written:
                # Already written via another bucket — record in this bucket's summary
                stats["buckets"][bucket_name].append(_summary_row(c))
                bstats["overlap_reused"] += 1
                continue
            bstats["attempted"] += 1
            try:
                insert_carrier(_fields_for_insert(c, bucket_name))
                written.add(dot)
                stats["written_dots"].append(dot)
                stats["buckets"][bucket_name].append(_summary_row(c))
                bstats["inserted"] += 1
                logger.info(
                    "Insert call OK for DOT %s (%s) via %s bucket",
                    dot, c.get("Legal_Name"), bucket_name,
                )
            except Exception as exc:
                err = {"dot": dot, "name": c.get("Legal_Name"),
                       "bucket": bucket_name, "error": str(exc)}
                stats["insert_errors"].append(err)
                bstats["quarantined_or_error"] += 1
                logger.error("INSERT FAILED for DOT %s: %s", dot, exc)
        return bstats

    bucket_write_stats["flatbed"] = _write_bucket("flatbed", top_flatbed)
    bucket_write_stats["dry_van"] = _write_bucket("dry_van", top_dry_van)
    bucket_write_stats["reefer"] = _write_bucket("reefer", top_reefer)
    bucket_write_stats["box_truck"] = _write_bucket("box_truck", top_box_truck)
    stats["phase"]["bucket_write_stats"] = bucket_write_stats

    stats["runtime_seconds"] = round(time.time() - start, 1)
    REPORT_JSON.write_text(json.dumps(stats, indent=2, default=str))
    logger.info("Report saved to %s", REPORT_JSON)
    logger.info("Runtime: %.1fs", stats["runtime_seconds"])
    logger.info("Distinct DOTs written: %d", len(written))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
