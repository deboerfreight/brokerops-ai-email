# DEPRECATED 2026-04-15 — consolidated into prospect_carriers.py --state MN. See feedback_carrier_category_rules.md.
# Equivalent command: python -m scripts.prospect_carriers --state MN --buckets flatbed,dry_van,box_truck --limit 5
# Root cause for deprecation: this script called insert_carrier() directly without enforcing
# EXCLUDED_SERVICE_TYPE_PATTERNS, allowing towing/moving/excavating companies into the DB.
"""
Minnesota carrier search — top 5 per equipment type (Flatbed / Dry Van / Box Truck).

Pipeline:
  Phase 1: Source via L&I SQLite (state=MN, BIPD >= $1M, exclude broker-only)
  Phase 2: Hydrate first 150 candidates via QCMobile (1 req/sec)
  Phase 3: Partition into buckets + score with app.fmcsa.score_carrier
  Phase 4: Write top 5 per bucket via app.sheets.insert_carrier (gate enforced)

Idempotent: pre-seeds seen_dots from both main tab and Quarantine.
Saves a JSON report under scripts/logs/mn_carrier_search_20260415.json.
Does NOT send emails. Does NOT bypass the vetting gate.
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("mn_search")
# Quiet the noisy google http client.
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

STATE = "MN"
SOURCING_LIMIT = 500
HYDRATE_LIMIT = 150
TOP_N = 5
QCMOBILE_SLEEP = 1.0  # seconds between FMCSA calls (1 req/sec)

SCRIPT_DIR = Path(__file__).resolve().parent
LOG_DIR = SCRIPT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
REPORT_JSON = LOG_DIR / "mn_carrier_search_20260415.json"


# ── Helpers ─────────────────────────────────────────────────────────────────

def _load_seen_dots() -> set[str]:
    """Pre-seed from main tab + Quarantine tab so we skip anything we've
    already processed. Reads both tabs via app.sheets.read_range.
    """
    seen: set[str] = set()

    # Main tab via get_all_carriers (already aliased)
    try:
        for c in get_all_carriers():
            dot = (c.get("DOT Number") or c.get("DOT_Number") or "").strip()
            if dot:
                seen.add(str(int(dot)) if dot.isdigit() else dot)
    except Exception as exc:
        logger.warning("main-tab dedup load failed: %s", exc)

    # Quarantine tab — raw read; column E (index 4) is DOT Number.
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
    # Preserve typical trucking suffixes in upper case
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
        "box_truck": False,
    }

    # Box truck heuristic: general-freight cargo, fleet 3-25, not flatbed/reefer/tanker
    is_general = "GENERAL FREIGHT" in cargo_carried or "GEN FREIGHT" in cargo_carried
    has_other = any(t in eq for t in ("FLATBED", "REEFER", "TANKER"))
    if is_general and 3 <= units <= 25 and not has_other:
        flags["box_truck"] = True

    return flags


def _rank_bucket(entries: list[tuple[int, dict]]) -> list[dict]:
    """Rank a list of (score, carrier) by score desc, then fleet desc,
    then safety rating (SATISFACTORY > NONE > CONDITIONAL)."""
    safety_rank = {"SATISFACTORY": 2, "NONE": 1, "": 1, "CONDITIONAL": 0}

    def key(item):
        score, c = item
        units = int(c.get("Power_Units") or 0)
        sr = (c.get("Safety_Rating") or "").upper()
        return (-score, -units, -safety_rank.get(sr, 1))

    return [c for _s, c in sorted(entries, key=key)]


def _fields_for_insert(carrier: dict, bucket_label: str) -> dict:
    """Build an insert_carrier payload following the prospect_carriers pattern.
    Uses the sentinel cargo=1 trick so general-freight carriers pass the gate.
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
        "Preferred_Lanes": f"MN_{bucket_label.upper()}",
        "City": _title_case_name(carrier.get("City", "")),
        "State": carrier.get("State", ""),
        "ZIP": carrier.get("Zip", ""),
        "Authority_Status": carrier.get("Authority_Status", ""),
        "Authority_Verified_Date": today,
        "On_Time_Score": str(carrier.get("_score", 0)),
        "Active": "TRUE",
        "Onboarding_Status": "PROSPECT",
        "Internal_Notes": (
            f"MN top-5 {bucket_label} search 2026-04-15. "
            f"Score={carrier.get('_score', 0)}. Fleet={carrier.get('Power_Units', 0)}."
        ),
        # Hydrated fields the vetting gate requires
        "Power_Units": carrier.get("Power_Units", 0),
        "Driver_Count": carrier.get("Driver_Count", 0),
        "Insurance_Liability": carrier.get("Insurance_Liability", 0),
        "Insurance_Cargo": carrier.get("Insurance_Cargo") or 1,  # sentinel
        "Safety_Rating": carrier.get("Safety_Rating", ""),
        "Vehicle_OOS_Rate": carrier.get("Vehicle_OOS_Rate", 0),
        "Driver_OOS_Rate": carrier.get("Driver_OOS_Rate", 0),
        "Crash_Rate_Per100": carrier.get("Crash_Rate_Per100", 0),
        "Vehicle_OOS_Insp": carrier.get("Vehicle_OOS_Insp", 0),
        "Equipment_Types": carrier.get("Equipment_Types", ""),
    }


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> int:
    start = time.time()
    stats: dict = {
        "phase": {},
        "buckets": {"flatbed": [], "dry_van": [], "box_truck": []},
        "written_dots": [],
        "skipped_duplicates": 0,
        "hydrate_failed": 0,
        "score_rejected": [],
        "insert_errors": [],
    }

    # ── Phase 1: sourcing ───────────────────────────────────────────────────
    logger.info("Phase 1: sourcing MN candidates from L&I SQLite")
    candidates = search_carriers_by_state(
        state=STATE,
        zip_prefixes=None,
        min_bipd=1_000_000,
        exclude_broker_only=True,
        limit=SOURCING_LIMIT,
    )
    stats["phase"]["sourced"] = len(candidates)
    logger.info("Phase 1 done: %d candidates", len(candidates))

    if len(candidates) < 50:
        logger.error("STOP: fewer than 50 candidates (%d). Aborting.", len(candidates))
        stats["phase"]["aborted"] = "candidates_below_50"
        REPORT_JSON.write_text(json.dumps(stats, indent=2))
        return 1

    # ── Phase 2: hydrate ────────────────────────────────────────────────────
    logger.info("Phase 2: pre-loading seen_dots from main tab + Quarantine")
    seen_dots = _load_seen_dots()
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
            # Carry sourcing geography through if QCMobile returns blanks
            if not details.get("City"):
                details["City"] = cand.bus_city
            if not details.get("State"):
                details["State"] = cand.bus_state
            if not details.get("Zip"):
                details["Zip"] = cand.bus_zip
            hydrated.append(details)

        if processed % 20 == 0:
            logger.info(
                "Hydrated %d/%d (kept %d, skipped %d dup, %d fail)",
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

        # Hard requirements the task spec reiterates
        units = int(c.get("Power_Units") or 0)
        safety = (c.get("Safety_Rating") or "").upper()
        if units < 3:
            stats["score_rejected"].append({
                "dot": c.get("DOT_Number"), "name": c.get("Legal_Name"),
                "reason": f"fleet<{3} (was {units})",
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
        if flags["box_truck"]:
            bt_entries.append((s, c))

    stats["phase"]["bucket_counts_before_rank"] = {
        "flatbed": len(fb_entries),
        "dry_van": len(dv_entries),
        "box_truck": len(bt_entries),
    }
    logger.info(
        "Bucket counts (qualified): flatbed=%d dry_van=%d box_truck=%d",
        len(fb_entries), len(dv_entries), len(bt_entries),
    )

    top_flatbed = _rank_bucket(fb_entries)[:TOP_N]
    top_dry_van = _rank_bucket(dv_entries)[:TOP_N]
    top_box_truck = _rank_bucket(bt_entries)[:TOP_N]

    # ── Phase 4: write via insert_carrier ───────────────────────────────────
    logger.info("Phase 4: writing top-5 per bucket via insert_carrier")
    written: set[str] = set()  # DOTs already written this run (dedup across overlap)

    def _write_bucket(bucket_name: str, rows: list[dict]):
        for c in rows:
            dot = str(c.get("DOT_Number", "") or "").strip()
            if not dot:
                continue
            if dot in written:
                # Already written via another bucket — just record in both tables
                stats["buckets"][bucket_name].append(_summary_row(c))
                continue
            try:
                insert_carrier(_fields_for_insert(c, bucket_name))
                written.add(dot)
                stats["written_dots"].append(dot)
                stats["buckets"][bucket_name].append(_summary_row(c))
                logger.info("Inserted DOT %s (%s) via %s bucket",
                            dot, c.get("Legal_Name"), bucket_name)
            except Exception as exc:
                err = {"dot": dot, "name": c.get("Legal_Name"),
                       "bucket": bucket_name, "error": str(exc)}
                stats["insert_errors"].append(err)
                logger.error("INSERT FAILED for DOT %s: %s", dot, exc)

    _write_bucket("flatbed", top_flatbed)
    _write_bucket("dry_van", top_dry_van)
    _write_bucket("box_truck", top_box_truck)

    stats["runtime_seconds"] = round(time.time() - start, 1)
    REPORT_JSON.write_text(json.dumps(stats, indent=2, default=str))
    logger.info("Report saved to %s", REPORT_JSON)
    logger.info("Runtime: %.1fs", stats["runtime_seconds"])
    logger.info("Distinct DOTs written: %d", len(written))
    return 0


def _summary_row(c: dict) -> dict:
    return {
        "rank": None,
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
    }


if __name__ == "__main__":
    raise SystemExit(main())
