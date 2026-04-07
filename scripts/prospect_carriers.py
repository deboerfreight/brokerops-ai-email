#!/usr/bin/env python3
"""
BrokerOps AI – Prospect Carrier Pipeline.

Reads enriched vendor DC locations (CSV), clusters into geographic lanes
destined for South FL / Key West, searches FMCSA for carriers servicing
those lanes, vets strictly, enriches with contact info, and stages them
for Sofia outreach.

Usage:
    python -m scripts.prospect_carriers [OPTIONS]

Options:
    --dry-run          Log without writing to Sheets
    --cluster NAME     Run one cluster only (SOUTH_FL, CENTRAL_FL, SOUTHEAST_US, MID_ATLANTIC, NATIONAL)
    --limit N          Max carriers per search target (default: 30)
    --min-score N      Minimum score threshold (default: 40)
    --min-fleet N      Minimum power units (default: 3)
    --source TYPE      Data source: csv or sheets (default: csv)
    --resume FILE      Resume from checkpoint JSON file
    --verbose          Debug logging
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

# Ensure project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import get_settings
from app.fmcsa import search_carriers as fmcsa_search, get_carrier_details, score_carrier, classify_onboarding_tier
from app.email_enrichment import enrich_carrier_email
from app.sheets import (
    find_carrier,
    get_all_carriers,
    insert_carrier,
    update_carrier_fields_by_key,
)

# Try importing vet_carrier_strict — being built by another developer in parallel.
# Falls back to basic scoring only if not yet available.
try:
    from app.fmcsa import vet_carrier_strict
except ImportError:
    vet_carrier_strict = None  # type: ignore[assignment]

logger = logging.getLogger("brokerops.prospect")

# ── Constants ──────────────────────────────────────────────────────────────────

# Default CSV paths for vendor DC data
DEFAULT_CSV_PATHS = [
    Path(r"C:\Users\Owner\Desktop\Claude Work\vendor_locations.csv"),
    Path(r"C:\Users\Owner\Desktop\Claude Work\building_supply_vendor_locations.csv"),
]

CHECKPOINT_DIR = Path(r"C:\Users\Owner\brokerops-ai\scripts\.checkpoints")

# Equipment types to search for each target
EQUIPMENT_SEARCH_TYPES = ["FLATBED", "DRY_VAN"]

# ── Lane Clusters ──────────────────────────────────────────────────────────────

# South FL ZIP prefixes (330-334, 339, 340-341, 346)
_SOUTH_FL_ZIP_PREFIXES = ("330", "331", "332", "333", "334", "339", "340", "341", "346")

# Central FL ZIP prefixes (everything else in FL)
_CENTRAL_FL_STATES = {"FL"}
_SOUTHEAST_US_STATES = {"GA", "AL", "SC", "NC", "TN", "MS", "VA"}
_MID_ATLANTIC_STATES = {"MD", "PA", "NY", "NJ", "CT", "MA", "OH", "WV"}

CLUSTER_PRIORITY = ["SOUTH_FL", "CENTRAL_FL", "SOUTHEAST_US", "MID_ATLANTIC", "NATIONAL"]


def classify_cluster(state: str, zip_code: str) -> str:
    """Assign a vendor DC to a lane cluster based on state and ZIP."""
    state = state.upper().strip()
    zip_code = str(zip_code).strip()

    if state == "FL":
        if zip_code and any(zip_code.startswith(p) for p in _SOUTH_FL_ZIP_PREFIXES):
            return "SOUTH_FL"
        return "CENTRAL_FL"
    if state in _SOUTHEAST_US_STATES:
        return "SOUTHEAST_US"
    if state in _MID_ATLANTIC_STATES:
        return "MID_ATLANTIC"
    return "NATIONAL"


# ── Vendor DC Loading ──────────────────────────────────────────────────────────


def load_vendor_dcs_csv(csv_paths: list[Path] | None = None) -> list[dict[str, str]]:
    """Load vendor DC records from one or more CSV files.

    Each CSV is expected to have: Company, Facility_Type, Address, City, State, ZIP, Phone, Notes
    """
    paths = csv_paths or DEFAULT_CSV_PATHS
    all_dcs: list[dict[str, str]] = []

    for csv_path in paths:
        if not csv_path.exists():
            logger.warning("CSV file not found: %s", csv_path)
            continue

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                # Skip rows without city/state (can't search FMCSA without location)
                city = (row.get("City") or "").strip()
                state = (row.get("State") or "").strip()
                if not city or not state:
                    continue
                all_dcs.append({
                    "Company": (row.get("Company") or "").strip(),
                    "Facility_Type": (row.get("Facility_Type") or "").strip(),
                    "City": city,
                    "State": state,
                    "ZIP": (row.get("ZIP") or "").strip(),
                })
                count += 1
            logger.info("Loaded %d DCs from %s", count, csv_path.name)

    logger.info("Total vendor DCs loaded: %d", len(all_dcs))
    return all_dcs


# ── Search Target Generation ───────────────────────────────────────────────────


def build_search_targets(dcs: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    """Cluster DCs into lane groups and deduplicate into unique search targets.

    For SOUTH_FL and CENTRAL_FL: city-level searches.
    For SOUTHEAST_US, MID_ATLANTIC, NATIONAL: state-level searches.

    Returns {cluster_name: [{"state": ..., "city": ... or None}, ...]}.
    """
    clusters: dict[str, list[dict[str, str]]] = {c: [] for c in CLUSTER_PRIORITY}

    # Group DCs by cluster
    for dc in dcs:
        cluster = classify_cluster(dc["State"], dc.get("ZIP", ""))
        clusters[cluster].append(dc)

    # Deduplicate into search targets
    targets: dict[str, list[dict[str, str]]] = {}
    for cluster_name in CLUSTER_PRIORITY:
        cluster_dcs = clusters[cluster_name]
        seen: set[str] = set()
        cluster_targets: list[dict[str, str]] = []

        if cluster_name in ("SOUTH_FL", "CENTRAL_FL"):
            # City-level search
            for dc in cluster_dcs:
                key = f"{dc['City'].upper()}|{dc['State'].upper()}"
                if key not in seen:
                    seen.add(key)
                    cluster_targets.append({"state": dc["State"], "city": dc["City"]})
        else:
            # State-level search
            for dc in cluster_dcs:
                key = dc["State"].upper()
                if key not in seen:
                    seen.add(key)
                    cluster_targets.append({"state": dc["State"], "city": None})

        targets[cluster_name] = cluster_targets
        logger.info(
            "Cluster %s: %d DCs → %d search targets",
            cluster_name, len(cluster_dcs), len(cluster_targets),
        )

    return targets


# ── Strict Vetting ─────────────────────────────────────────────────────────────


def _vet_strict_fallback(carrier: dict) -> tuple[bool, str]:
    """Fallback strict vetting when vet_carrier_strict is not yet available.

    Hard reject thresholds:
        - Authority must be ACTIVE
        - No OOS active flag
        - Unsatisfactory safety rating = REJECT
        - Vehicle OOS rate > 30% = REJECT
        - Driver OOS rate > 15% = REJECT
        - Crash rate > 30 per 100 units = REJECT
        - Fleet size < 3 power units = REJECT
        - 0 drivers with >0 units (shell/stale) = REJECT
    """
    name = carrier.get("Legal_Name", "unknown")

    if carrier.get("Authority_Status") != "ACTIVE":
        return False, f"authority={carrier.get('Authority_Status')}"

    if carrier.get("OOS_Active"):
        return False, "OOS active"

    if carrier.get("Safety_Rating") == "UNSATISFACTORY":
        return False, "unsatisfactory safety rating"

    veh_oos = carrier.get("Vehicle_OOS_Rate", 0)
    if veh_oos > 30:
        return False, f"vehicle OOS rate {veh_oos}% > 30%"

    drv_oos = carrier.get("Driver_OOS_Rate", 0)
    if drv_oos > 15:
        return False, f"driver OOS rate {drv_oos}% > 15%"

    # Fleet size: 3 trucks minimum
    units = carrier.get("Power_Units", 0)
    if units > 0 and units < 3:
        return False, f"fleet size {units} < 3 minimum"

    # Shell / stale carrier: 0 drivers with >0 units
    drivers = carrier.get("Driver_Count", 0)
    if units > 0 and drivers == 0:
        return False, f"0 drivers with {units} units (shell/stale)"

    # Crash rate > 30 per 100 power units
    crash_rate = carrier.get("Crash_Rate_Per100", 0)
    if crash_rate and crash_rate > 30:
        return False, f"crash rate {crash_rate} > 30 per 100 units"

    return True, "passed"


def vet_carrier(carrier: dict) -> tuple[bool, str]:
    """Vet a carrier using strict thresholds.

    Uses vet_carrier_strict from fmcsa.py if available, otherwise falls back
    to the local strict vetting logic.
    """
    if vet_carrier_strict is not None:
        return vet_carrier_strict(carrier)

    return _vet_strict_fallback(carrier)


# ── Checkpoint/Resume ──────────────────────────────────────────────────────────


def _checkpoint_path(run_id: str) -> Path:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    return CHECKPOINT_DIR / f"prospect_{run_id}.json"


def save_checkpoint(run_id: str, state: dict) -> None:
    """Save progress checkpoint for resume capability."""
    path = _checkpoint_path(run_id)
    state["last_saved"] = datetime.utcnow().isoformat()
    with open(path, "w") as f:
        json.dump(state, f, indent=2)
    logger.debug("Checkpoint saved: %s", path)


def load_checkpoint(path: str) -> dict:
    """Load a checkpoint file for resuming."""
    with open(path) as f:
        return json.load(f)


# ── Core Pipeline ──────────────────────────────────────────────────────────────


def search_cluster_carriers(
    target: dict[str, str | None],
    equipment_types: list[str],
    limit: int,
    min_score: int,
    min_fleet: int,
    seen_dots: set[str],
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Search FMCSA for carriers at a single target location.

    For each equipment type, searches FMCSA, fetches details, scores,
    vets strictly, and deduplicates across all prior results.

    Returns list of qualified carrier dicts.
    """
    state = target["state"]
    city = target.get("city")
    location_str = f"{city}, {state}" if city else state
    qualified: list[dict[str, Any]] = []

    for eq_type in equipment_types:
        logger.info("Searching %s carriers in %s ...", eq_type, location_str)

        try:
            raw_carriers = fmcsa_search(
                state=state,
                city=city,
                equipment_type=eq_type,
                limit=limit * 3,  # over-fetch to account for filtering
            )
        except Exception as exc:
            logger.error("FMCSA search failed for %s %s: %s", eq_type, location_str, exc)
            continue

        if not raw_carriers:
            logger.info("No results for %s in %s", eq_type, location_str)
            continue

        logger.info("FMCSA returned %d raw carriers for %s in %s", len(raw_carriers), eq_type, location_str)

        for carrier in raw_carriers:
            dot = carrier.get("DOT_Number", "")
            if not dot:
                continue

            # Deduplicate across all targets
            if dot in seen_dots:
                continue
            seen_dots.add(dot)

            # Fetch full details
            try:
                details = get_carrier_details(dot)
                if details:
                    carrier = details
            except Exception as exc:
                logger.debug("Detail fetch failed for DOT %s: %s", dot, exc)

            # Score
            score = score_carrier(carrier)
            if score < 0:
                logger.debug("Disqualified DOT %s: score=%d", dot, score)
                continue
            if score < min_score:
                logger.debug("Below min score DOT %s: score=%d < %d", dot, score, min_score)
                continue

            # Fleet size check
            power_units = carrier.get("Power_Units", 0)
            if power_units < min_fleet:
                logger.debug("Below min fleet DOT %s: %d units < %d", dot, power_units, min_fleet)
                continue

            # Strict vetting
            passed, reason = vet_carrier(carrier)
            if not passed:
                logger.info("REJECT DOT %s (%s): %s", dot, carrier.get("Legal_Name", "?"), reason)
                continue

            # Equipment match bonus
            eq_match = eq_type.upper() in (carrier.get("Equipment_Types", "") or "").upper()
            if eq_match:
                score += 15

            carrier["Carrier_Score"] = score
            carrier["_equipment_searched"] = eq_type
            carrier["_equipment_match"] = eq_match
            qualified.append(carrier)

            if len(qualified) >= limit:
                break

        # Brief pause between equipment type searches for rate limiting
        time.sleep(0.5)

    # Sort by score descending
    qualified.sort(key=lambda c: c.get("Carrier_Score", 0), reverse=True)
    return qualified[:limit]


def enrich_and_store(
    carrier: dict[str, Any],
    cluster_name: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Store a qualified carrier in Carrier_Master and run email enrichment.

    Tags with lane cluster in Preferred_Lanes and sets Onboarding_Status = PROSPECT.
    """
    mc = carrier.get("MC_Number", "")
    dot = carrier.get("DOT_Number", "")
    today = date.today().isoformat()
    carrier_key = mc or dot

    # Classify onboarding tier
    score = carrier.get("Carrier_Score", 0)
    tier_info = classify_onboarding_tier(carrier, score)
    tier = tier_info["tier"]
    load_cap = tier_info["load_cap"]
    reqs = ", ".join(tier_info["requirements"])
    tier_reason = tier_info["reason"]

    if dry_run:
        logger.info(
            "[DRY-RUN] Would store DOT %s (%s) — score=%d, tier=%s, cluster=%s, reason=%s",
            dot, carrier.get("Legal_Name", "?"), score, tier, cluster_name, tier_reason,
        )
        return carrier

    # Skip Sheets API call for duplicate check — seen_dots already handles this.
    # If we get here, the carrier passed the seen_dots filter in search_cluster_carriers.
    existing = None

    tier_note = f"Tier: {tier} | Load cap: ${load_cap:,}" if load_cap else f"Tier: {tier}"

    fields: dict[str, Any] = {
        "MC_Number": mc,
        "DOT_Number": dot,
        "Legal_Name": carrier.get("Legal_Name", ""),
        "DBA_Name": carrier.get("DBA_Name", ""),
        "Primary_Phone": carrier.get("Contact_Phone", ""),
        "Equipment_Type": carrier.get("Equipment_Types", ""),
        "Preferred_Lanes": cluster_name,
        "Authority_Status": carrier.get("Authority_Status", ""),
        "Authority_Verified_Date": today,
        "Authority_Source": "FMCSA",
        "On_Time_Score": str(score),
        "Active": "TRUE",
        "Onboarding_Status": "PROSPECT",
        "Onboarding_Tier": tier,
        "Onboarding_Requirements": reqs,
        "Load_Cap": str(load_cap) if load_cap else "",
        "Internal_Notes": f"Prospected via vendor DC pipeline ({cluster_name}). Score: {score}. {tier_note}. Reason: {tier_reason}.",
    }

    if existing:
        # Update existing carrier — merge lane cluster into Preferred_Lanes
        existing_lanes = existing.get("Preferred_Lanes", "")
        if cluster_name not in existing_lanes:
            new_lanes = f"{existing_lanes}, {cluster_name}" if existing_lanes else cluster_name
        else:
            new_lanes = existing_lanes

        update_carrier_fields_by_key(mc, dot, {
            "Authority_Status": fields["Authority_Status"],
            "Authority_Verified_Date": today,
            "Equipment_Type": fields["Equipment_Type"],
            "On_Time_Score": fields["On_Time_Score"],
            "Preferred_Lanes": new_lanes,
            "Onboarding_Status": "PROSPECT",
        })
        logger.info("Updated existing carrier %s (added to %s)", carrier_key, cluster_name)
        # Merge existing data for enrichment
        for k, v in existing.items():
            if k not in fields or not fields[k]:
                fields[k] = v
    else:
        # New carrier — insert
        insert_carrier(fields)
        logger.info("Inserted new carrier %s (%s) — cluster %s", carrier_key, fields["Legal_Name"], cluster_name)

    # Run email enrichment if no email on file
    current_email = fields.get("Primary_Email", "") or (existing or {}).get("Primary_Email", "")
    if not current_email or current_email == "PHONE_ONLY":
        try:
            enrichment = enrich_carrier_email({
                "DOT_Number": dot,
                "MC_Number": mc,
                "Legal_Name": fields.get("Legal_Name", ""),
                "City": carrier.get("City", ""),
                "State": carrier.get("State", ""),
            })

            email = enrichment.get("email")
            source = enrichment.get("source", "PHONE_ONLY")
            website = enrichment.get("website")

            updates: dict[str, str] = {"Contact_Email_Source": source}
            if email:
                updates["Primary_Email"] = email
                updates["Outreach_Method"] = "EMAIL"
            else:
                updates["Primary_Email"] = "PHONE_ONLY"
                updates["Outreach_Method"] = "PHONE"
            if website:
                updates["Website"] = website

            update_carrier_fields_by_key(mc, dot, updates)
            fields.update(updates)
            logger.info("Enriched %s: email=%s source=%s", carrier_key, email or "PHONE_ONLY", source)
        except Exception as exc:
            logger.warning("Enrichment failed for %s: %s", carrier_key, exc)

    # Throttle Sheets API calls to stay under 60 reads/min quota
    time.sleep(2)

    return fields


def queue_for_outreach(
    carrier: dict[str, Any],
    dry_run: bool = False,
) -> None:
    """Mark a PROSPECT carrier as ready for Sofia outreach by setting status to NEW."""
    mc = carrier.get("MC_Number", "")
    dot = carrier.get("DOT_Number", "")
    carrier_key = mc or dot

    tier = carrier.get("Onboarding_Tier", "STANDARD")

    # MANUAL_REVIEW carriers need Derek's approval before outreach
    if tier == "MANUAL_REVIEW":
        logger.info("Holding %s for manual review (score too low) — not queuing for outreach", carrier_key)
        return

    if dry_run:
        logger.info("[DRY-RUN] Would queue %s for outreach (tier=%s)", carrier_key, tier)
        return

    # Only queue carriers that have an email (skip PHONE_ONLY for now)
    email = carrier.get("Primary_Email", "")
    if not email or email == "PHONE_ONLY":
        logger.info("Skipping outreach queue for %s — no email (PHONE_ONLY)", carrier_key)
        return

    update_carrier_fields_by_key(mc, dot, {"Onboarding_Status": "NEW"})
    logger.info("Queued %s for outreach (PROSPECT → NEW, tier=%s)", carrier_key, tier)


# ── Main Pipeline ──────────────────────────────────────────────────────────────


def run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    """Execute the full prospect carrier pipeline.

    Returns a summary dict with counts.
    """
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # Stats tracking
    stats: dict[str, Any] = {
        "run_id": run_id,
        "started": datetime.utcnow().isoformat(),
        "dry_run": args.dry_run,
        "clusters": {},
        "total_searched": 0,
        "total_qualified": 0,
        "total_stored": 0,
        "total_queued": 0,
    }

    # Load resume checkpoint if provided
    resume_state: dict[str, Any] = {}
    completed_targets: set[str] = set()
    if args.resume:
        resume_state = load_checkpoint(args.resume)
        completed_targets = set(resume_state.get("completed_targets", []))
        logger.info("Resuming from checkpoint: %d targets already completed", len(completed_targets))

    # 1. Load vendor DCs
    logger.info("Loading vendor DC data (source=%s)...", args.source)
    if args.source == "csv":
        dcs = load_vendor_dcs_csv()
    else:
        logger.error("Sheets source not yet implemented — use --source csv")
        sys.exit(1)

    if not dcs:
        logger.error("No vendor DCs loaded — nothing to do.")
        return stats

    # 2. Build search targets
    targets_by_cluster = build_search_targets(dcs)

    # Filter to single cluster if requested
    clusters_to_run = CLUSTER_PRIORITY
    if args.cluster:
        cluster_name = args.cluster.upper()
        if cluster_name not in targets_by_cluster:
            logger.error("Unknown cluster: %s (valid: %s)", cluster_name, ", ".join(CLUSTER_PRIORITY))
            sys.exit(1)
        clusters_to_run = [cluster_name]

    total_targets = sum(len(targets_by_cluster[c]) for c in clusters_to_run)
    logger.info("Running %d clusters, %d total search targets", len(clusters_to_run), total_targets)

    # Pre-seed seen_dots with carriers already in the database
    seen_dots: set[str] = set(resume_state.get("seen_dots", []))
    try:
        existing_carriers = get_all_carriers()
        for c in existing_carriers:
            dot = c.get("DOT Number", "").strip()
            if dot:
                seen_dots.add(dot)
        logger.info("Pre-loaded %d existing carriers from database (will skip duplicates)", len(seen_dots))
    except Exception as e:
        logger.warning("Could not pre-load existing carriers: %s (continuing without dedup)", e)
    completed_list: list[str] = list(completed_targets)
    target_idx = 0

    # 3. Process each cluster in priority order
    for cluster_name in clusters_to_run:
        cluster_targets = targets_by_cluster[cluster_name]
        if not cluster_targets:
            continue

        cluster_stats = {
            "targets": len(cluster_targets),
            "searched": 0,
            "qualified": 0,
            "stored": 0,
            "queued": 0,
        }

        logger.info("=" * 60)
        logger.info("CLUSTER: %s (%d targets)", cluster_name, len(cluster_targets))
        logger.info("=" * 60)

        for target in cluster_targets:
            target_idx += 1
            target_key = f"{cluster_name}|{target.get('city', '')}|{target['state']}"

            # Skip already-completed targets (resume)
            if target_key in completed_targets:
                logger.info("Skipping completed target: %s", target_key)
                continue

            location_str = f"{target.get('city', '')}, {target['state']}" if target.get("city") else target["state"]
            logger.info(
                "Target %d/%d: %s [%s]",
                target_idx, total_targets, location_str, cluster_name,
            )

            # South FL: min 3 trucks, slightly relaxed score
            # Safety vetting (OOS, crash rate) stays strict regardless
            if cluster_name in ("SOUTH_FL", "CENTRAL_FL"):
                effective_min_score = min(args.min_score, 25)
                effective_min_fleet = max(args.min_fleet, 3) if args.min_fleet <= 3 else args.min_fleet
            else:
                effective_min_score = args.min_score
                effective_min_fleet = args.min_fleet

            # Search and vet
            qualified = search_cluster_carriers(
                target=target,
                equipment_types=EQUIPMENT_SEARCH_TYPES,
                limit=args.limit,
                min_score=effective_min_score,
                min_fleet=effective_min_fleet,
                seen_dots=seen_dots,
                dry_run=args.dry_run,
            )

            cluster_stats["searched"] += 1
            cluster_stats["qualified"] += len(qualified)

            # Enrich and store each qualified carrier
            for carrier in qualified:
                stored = enrich_and_store(carrier, cluster_name, dry_run=args.dry_run)
                cluster_stats["stored"] += 1

                # Queue for outreach
                queue_for_outreach(stored, dry_run=args.dry_run)
                if stored.get("Primary_Email") and stored.get("Primary_Email") != "PHONE_ONLY":
                    cluster_stats["queued"] += 1

            # Mark target as completed and checkpoint
            completed_list.append(target_key)
            completed_targets.add(target_key)

            save_checkpoint(run_id, {
                "completed_targets": completed_list,
                "seen_dots": list(seen_dots),
                "stats": stats,
            })

            logger.info(
                "Target done: %d qualified carriers from %s",
                len(qualified), location_str,
            )

            # Rate limiting between targets
            time.sleep(1)

        stats["clusters"][cluster_name] = cluster_stats
        stats["total_searched"] += cluster_stats["searched"]
        stats["total_qualified"] += cluster_stats["qualified"]
        stats["total_stored"] += cluster_stats["stored"]
        stats["total_queued"] += cluster_stats["queued"]

        logger.info(
            "Cluster %s complete: %d targets, %d qualified, %d stored, %d queued",
            cluster_name,
            cluster_stats["targets"],
            cluster_stats["qualified"],
            cluster_stats["stored"],
            cluster_stats["queued"],
        )

    # Final summary
    stats["finished"] = datetime.utcnow().isoformat()
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("  Clusters run:     %d", len(clusters_to_run))
    logger.info("  Targets searched: %d", stats["total_searched"])
    logger.info("  Carriers found:   %d", stats["total_qualified"])
    logger.info("  Carriers stored:  %d", stats["total_stored"])
    logger.info("  Queued outreach:  %d", stats["total_queued"])
    logger.info("  Dry run:          %s", stats["dry_run"])
    logger.info("  Checkpoint:       %s", _checkpoint_path(run_id))
    logger.info("=" * 60)

    return stats


# ── CLI ────────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="BrokerOps AI – Prospect Carrier Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m scripts.prospect_carriers --dry-run
  python -m scripts.prospect_carriers --cluster SOUTH_FL --limit 10
  python -m scripts.prospect_carriers --resume scripts/.checkpoints/prospect_20260407_120000.json
  python -m scripts.prospect_carriers --verbose --min-score 50 --min-fleet 10
        """,
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log without writing to Sheets",
    )
    parser.add_argument(
        "--cluster", type=str, default=None,
        help="Run one cluster only (SOUTH_FL, CENTRAL_FL, SOUTHEAST_US, MID_ATLANTIC, NATIONAL)",
    )
    parser.add_argument(
        "--limit", type=int, default=30,
        help="Max carriers per search target (default: 30)",
    )
    parser.add_argument(
        "--min-score", type=int, default=40,
        help="Minimum carrier score threshold (default: 40)",
    )
    parser.add_argument(
        "--min-fleet", type=int, default=3,
        help="Minimum power units (default: 3)",
    )
    parser.add_argument(
        "--source", type=str, default="csv", choices=["csv", "sheets"],
        help="Data source for vendor DCs (default: csv)",
    )
    parser.add_argument(
        "--resume", type=str, default=None,
        help="Resume from checkpoint JSON file",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(name)-25s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    # Warn if vet_carrier_strict is not available
    if vet_carrier_strict is None:
        logger.warning(
            "vet_carrier_strict not found in app.fmcsa — using fallback strict vetting. "
            "Install the updated fmcsa.py to get full crash-rate and reefer checks."
        )

    logger.info("Starting prospect carrier pipeline...")
    logger.info("  Dry run:    %s", args.dry_run)
    logger.info("  Cluster:    %s", args.cluster or "ALL")
    logger.info("  Limit:      %d per target", args.limit)
    logger.info("  Min score:  %d", args.min_score)
    logger.info("  Min fleet:  %d", args.min_fleet)
    logger.info("  Source:     %s", args.source)

    stats = run_pipeline(args)

    # Exit code: 0 if any carriers found, 1 if none
    if stats["total_qualified"] == 0:
        logger.warning("No carriers qualified — check search parameters or FMCSA API key.")
        sys.exit(1)


if __name__ == "__main__":
    main()
