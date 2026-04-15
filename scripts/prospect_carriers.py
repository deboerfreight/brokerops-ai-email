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
import re
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
from app.vetting.li_insurance_lookup import (
    SourcingCandidate,
    search_carriers_by_state,
)
from app.vetting.rules import RULES

# Try importing vet_carrier_strict — being built by another developer in parallel.
# Falls back to basic scoring only if not yet available.
try:
    from app.fmcsa import vet_carrier_strict
except ImportError:
    vet_carrier_strict = None  # type: ignore[assignment]

logger = logging.getLogger("brokerops.prospect")

# ── Service-type exclusion denylist ───────────────────────────────────────────
# Carriers whose legal or DBA name matches any of these patterns are skipped
# BEFORE insertion into the database. Runs in search_cluster_carriers() right
# after hydration and scoring, prior to enrich_and_store().
#
# Pattern policy: feedback_carrier_category_rules.md (memory)
#   - Hard-exclude: towing, passenger, HHG moving, excavating, waste, logging
#   - Keep (do NOT exclude): heavy haul, rigging, auto transport, fuel/propane
#     (these are handled by service-type tagging at the DB level, not exclusion)
#
# To add a new pattern: extend this list. To temporarily disable a pattern:
# comment it out — do not delete, so we preserve the policy rationale.
EXCLUDED_SERVICE_TYPE_PATTERNS = re.compile(
    r"""
    \b(
        tow(?:ing)?                     |  # towing / tow service
        wrecker                         |  # wrecker service
        recovery                        |  # vehicle recovery
        passenger                       |  # passenger transport
        bus\s+(?:lines?|co(?:mpany)?|services?)  |  # bus lines / bus co.
        coach                           |  # coach transport
        shuttle                         |  # shuttle service
        tours?                          |  # tours
        charter                         |  # charter
        excavat(?:ing|ion)?             |  # excavating
        grading                         |  # grading (earthwork)
        paving                          |  # paving
        concrete                        |  # concrete
        waste                           |  # waste / garbage
        garbage                         |  # garbage
        refuse                          |  # refuse
        disposal                        |  # disposal
        sanitation                      |  # sanitation
        septic                          |  # septic
        roll.?off                       |  # roll-off dumpsters
        landscap(?:e|ing)?              |  # landscaping
        lawn\s+(?:care|service|mow)     |  # lawn care
        arborist                        |  # arborist / tree
        oilfield                        |  # oilfield services
        frac(?:turing)?                 |  # fracking
        drilling                        |  # drilling
        logging                         |  # logging / timber
        timber                          |  # timber
        pulpwood                        |  # pulpwood
        livestock                       |  # livestock hauling
        cattle                          |  # cattle hauling
        equine                          |  # equine / horse
        van\s+lines                     |  # van lines (moving)
        movers?                         |  # movers
        moving                             # moving company
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

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

# Central FL ZIP prefixes (everything in FL that's NOT South FL).
# Used both for exclusion and — in the L&I sourcing query — as a positive
# allow-list so we can target the Central/North FL ZIPs directly.
_CENTRAL_FL_ZIP_PREFIXES = (
    "320", "321", "322", "323", "324", "325", "326", "327", "328", "329",  # NE/Central
    "335", "336", "337", "338",  # Central/Tampa/Polk
    "342", "344",  # Central
    "347", "349",  # Ocala/NW
)

_CENTRAL_FL_STATES = {"FL"}
_SOUTHEAST_US_STATES = {"GA", "AL", "SC", "NC", "TN", "MS", "VA"}
_MID_ATLANTIC_STATES = {"MD", "PA", "NY", "NJ", "CT", "MA", "OH", "WV"}
# NATIONAL cluster: the biggest freight states not already covered
_NATIONAL_STATES = {"TX", "IL", "IN", "MI", "KY", "MO", "AR", "LA", "CA", "AZ"}

CLUSTER_PRIORITY = ["SOUTH_FL", "CENTRAL_FL", "SOUTHEAST_US", "MID_ATLANTIC", "NATIONAL"]


# Cluster → list of (state, zip_prefixes) tuples for the L&I sourcing query.
# zip_prefixes=None means "full state". Each tuple results in one SQLite
# query to carriers_sourcing.
CLUSTER_SOURCING_QUERIES: dict[str, list[tuple[str, Optional[tuple[str, ...]]]]] = {
    "SOUTH_FL": [("FL", _SOUTH_FL_ZIP_PREFIXES)],
    "CENTRAL_FL": [("FL", _CENTRAL_FL_ZIP_PREFIXES)],
    "SOUTHEAST_US": [(s, None) for s in sorted(_SOUTHEAST_US_STATES)],
    "MID_ATLANTIC": [(s, None) for s in sorted(_MID_ATLANTIC_STATES)],
    "NATIONAL": [(s, None) for s in sorted(_NATIONAL_STATES)],
}


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

    Hard reject thresholds pulled from app/vetting/rules.py::RULES — the
    single source of truth for every vetting consumer.
    """
    name = carrier.get("Legal_Name", "unknown")

    if carrier.get("Authority_Status") != "ACTIVE":
        return False, f"authority={carrier.get('Authority_Status')}"

    if carrier.get("OOS_Active"):
        return False, "OOS active"

    if carrier.get("Safety_Rating") == "UNSATISFACTORY":
        return False, "unsatisfactory safety rating"

    veh_oos = carrier.get("Vehicle_OOS_Rate", 0)
    if veh_oos > RULES.vehicle_oos_max_pct:
        return False, f"vehicle OOS rate {veh_oos}% > {RULES.vehicle_oos_max_pct:.0f}%"

    drv_oos = carrier.get("Driver_OOS_Rate", 0)
    if drv_oos > RULES.driver_oos_max_pct:
        return False, f"driver OOS rate {drv_oos}% > {RULES.driver_oos_max_pct:.0f}%"

    # Fleet size minimum
    units = carrier.get("Power_Units", 0)
    if units > 0 and units < RULES.fleet_min:
        return False, f"fleet size {units} < {RULES.fleet_min} minimum"

    # Shell / stale carrier: 0 drivers with >0 units
    drivers = carrier.get("Driver_Count", 0)
    if units > 0 and drivers == 0:
        return False, f"0 drivers with {units} units (shell/stale)"

    # Crash rate threshold
    crash_rate = carrier.get("Crash_Rate_Per100", 0)
    if crash_rate and crash_rate > RULES.crash_rate_max_per_100:
        return False, f"crash rate {crash_rate} > {RULES.crash_rate_max_per_100:.0f} per 100 units"

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
    state: str,
    zip_prefixes: Optional[tuple[str, ...]],
    limit: int,
    min_score: int,
    min_fleet: int,
    seen_dots: set[str],
    dry_run: bool = False,
    cluster_name: str | None = None,
    min_bipd: int = RULES.liability_min,
) -> list[dict[str, Any]]:
    """Source carriers from the L&I bulk-file SQLite index, hydrate via
    QCMobile, vet strictly, and return qualified carriers.

    Replaces the old FMCSA /name/{term} search which ignored the city param
    and returned only 3-4 global hits per term. New flow:

        1. SQLite query against carriers_sourcing pre-filtered on:
             - state = ?
             - zip_prefix match (if supplied)
             - bipd_filed >= min_bipd
             - active motor-carrier authority (common or contract)
             - not broker-only
        2. For each candidate DOT: skip if seen, hydrate via QCMobile at
           1 req/sec, score, fleet-gate, vet_carrier, equipment bonus.
        3. Stop at `limit` qualified carriers or when candidates exhausted.

    The candidate pool is over-fetched at 3x the final limit to account for
    failures at the fleet-size / safety-vetting gates (~30-50% drop-off in
    practice).

    Returns list of qualified carrier dicts — same shape as before, so the
    downstream `enrich_and_store` path is unchanged.
    """
    state_norm = str(state or "").upper().strip()
    zip_list = list(zip_prefixes) if zip_prefixes else None
    location_str = (
        f"{state_norm} [zip~{','.join(zip_list)}]" if zip_list else state_norm
    )

    # Over-fetch candidates. Empirical pass rate through fleet + safety
    # vetting on the SOUTH_FL smoke test was ~13% (2/15) — a lot of small
    # owner-ops and single-truck operators filed $1M+ BIPD but fall under
    # the 3-truck floor. 8x over-fetch keeps us comfortably above the
    # target without burning QCMobile calls on the whole state.
    candidate_limit = max(limit * 8, 20)

    logger.info(
        "L&I sourcing query: state=%s zip_prefixes=%s min_bipd=%d limit=%d",
        state_norm, zip_list, min_bipd, candidate_limit,
    )
    try:
        candidates = search_carriers_by_state(
            state=state_norm,
            zip_prefixes=zip_list,
            min_bipd=min_bipd,
            exclude_broker_only=True,
            require_active_authority=True,
            limit=candidate_limit,
        )
    except Exception as exc:
        logger.error("L&I sourcing query failed for %s: %s", location_str, exc)
        return []

    logger.info(
        "L&I returned %d candidate DOTs for %s (pre-hydration)",
        len(candidates), location_str,
    )
    if not candidates:
        return []

    qualified: list[dict[str, Any]] = []

    for cand in candidates:
        # L&I stores DOTs as zero-padded 8-digit; QCMobile and the sheet
        # use unpadded. Strip leading zeros for all downstream lookups,
        # dedup keys, and the hydration call.
        dot = cand.dot.lstrip("0") or cand.dot
        if not dot:
            continue

        # Dedupe across all prior targets and the existing sheet.
        if dot in seen_dots:
            continue
        seen_dots.add(dot)

        # Hydrate via QCMobile (plus L&I overlay — already wired in
        # get_carrier_details). Rate-limited at 1 req/sec; the call itself
        # triggers multiple sub-requests so we pause generously afterwards.
        try:
            carrier = get_carrier_details(dot)
        except Exception as exc:
            logger.debug("Hydration failed for DOT %s: %s", dot, exc)
            carrier = None

        time.sleep(1)  # rate limit QCMobile detail calls to 1/sec

        if not carrier:
            logger.debug("Skip DOT %s: hydration returned empty", dot)
            continue

        # Fill in any missing business-location fields from the L&I index
        # so downstream (enrichment + writer) always has a city/state/zip
        # to work with even if QCMobile is sparse.
        if not carrier.get("Legal_Name"):
            carrier["Legal_Name"] = cand.legal_name
        if not carrier.get("DBA_Name"):
            carrier["DBA_Name"] = cand.dba_name
        if not carrier.get("City"):
            carrier["City"] = cand.bus_city
        if not carrier.get("State"):
            carrier["State"] = cand.bus_state
        if not carrier.get("Zip"):
            carrier["Zip"] = cand.bus_zip
        # L&I docket as MC_Number fallback (trimmed of MC prefix)
        if not carrier.get("MC_Number") and cand.docket:
            carrier["MC_Number"] = (
                cand.docket.replace("MC-", "").replace("MC", "").strip()
            )

        # Score
        score = score_carrier(carrier)
        if score < 0:
            logger.debug(
                "Disqualified DOT %s (%s): score=%d",
                dot, cand.legal_name, score,
            )
            continue
        if score < min_score:
            logger.debug(
                "Below min score DOT %s (%s): score=%d < %d",
                dot, cand.legal_name, score, min_score,
            )
            continue

        # Fleet size check
        power_units = carrier.get("Power_Units", 0) or 0
        if power_units < min_fleet:
            logger.debug(
                "Below min fleet DOT %s (%s): %s units < %d",
                dot, cand.legal_name, power_units, min_fleet,
            )
            continue

        # Strict vetting
        passed, reason = vet_carrier(carrier)
        if not passed:
            logger.info(
                "REJECT DOT %s (%s): %s",
                dot, carrier.get("Legal_Name", cand.legal_name), reason,
            )
            continue

        # Service-type denylist — check BEFORE insert (policy: feedback_carrier_category_rules.md)
        # Matches towing, passenger, moving, excavating, waste, logging, etc.
        # Does NOT exclude heavy haul, rigging, auto transport, or fuel/propane.
        legal_name = carrier.get("Legal_Name") or cand.legal_name or ""
        dba_name   = carrier.get("DBA_Name") or cand.dba_name or ""
        check_name = f"{legal_name} {dba_name}".strip()
        _deny_match = EXCLUDED_SERVICE_TYPE_PATTERNS.search(check_name)
        if _deny_match:
            logger.info(
                "DENYLIST DOT %s (%s): matched pattern '%s' in name '%s'",
                dot, legal_name, _deny_match.group(0), check_name,
            )
            continue

        carrier["Carrier_Score"] = score
        carrier["_l_i_source"] = True
        carrier["_l_i_bipd_filed"] = cand.bipd_filed
        qualified.append(carrier)

        if len(qualified) >= limit:
            logger.info(
                "Hit limit (%d qualified) for %s — stopping candidate scan",
                limit, location_str,
            )
            break

    # Sort by score descending for deterministic ordering
    qualified.sort(key=lambda c: c.get("Carrier_Score", 0), reverse=True)
    logger.info(
        "Sourcing complete for %s: %d qualified / %d hydrated / %d candidates",
        location_str, len(qualified), len(seen_dots), len(candidates),
    )
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
        # Geographic fields — pass through to sheet. `_map_fields_to_sheet`
        # in app/sheets.py accepts "City", "State", "ZIP" as literal column
        # names so these flow straight into the Carrier Database tab.
        "City": carrier.get("City", ""),
        "State": carrier.get("State", ""),
        "ZIP": carrier.get("Zip", ""),
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
        # Hydrated FMCSA fields — required by the vetting gate in
        # `app.sheets.insert_carrier -> vet_complete`. These come from the
        # /carriers/{dot} detail fetch via `get_carrier_details()` upstream.
        # Without them the gate sees blank fleet/insurance and quarantines
        # every record as "needs_review: fleet size missing/blank".
        "Power_Units": carrier.get("Power_Units", 0),
        "Driver_Count": carrier.get("Driver_Count", 0),
        "Insurance_Liability": carrier.get("Insurance_Liability", 0),
        # Cargo insurance: FMCSA only publishes cargo filings for HHG
        # carriers. General-freight cargo coverage is contractual and
        # verified at onboarding, so RULES.cargo_min is 0. The old sentinel
        # workaround (overwriting blank with 1) was removed 2026-04-14 after
        # the gate's blank-cargo handling was fixed — whatever FMCSA returns
        # (typically 0) now correctly passes the gate.
        "Insurance_Cargo": carrier.get("Insurance_Cargo", 0),
        "Safety_Rating": carrier.get("Safety_Rating", ""),
        "Vehicle_OOS_Rate": carrier.get("Vehicle_OOS_Rate", 0),
        "Driver_OOS_Rate": carrier.get("Driver_OOS_Rate", 0),
        "Crash_Rate_Per100": carrier.get("Crash_Rate_Per100", 0),
        "Vehicle_OOS_Insp": carrier.get("Vehicle_OOS_Insp", 0),
        "Equipment_Types": carrier.get("Equipment_Types", ""),
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

    # Sourcing is now driven by CLUSTER_SOURCING_QUERIES (L&I bulk SQLite),
    # not by vendor DC CSVs. Vendor DCs are no longer required at all for
    # the prospect pipeline — each cluster has a fixed (state, zip_prefix)
    # query recipe that hits carriers_sourcing directly.
    targets_by_cluster: dict[str, list[tuple[str, Optional[tuple[str, ...]]]]] = {}
    for cn, queries in CLUSTER_SOURCING_QUERIES.items():
        targets_by_cluster[cn] = list(queries)

    # Filter to single cluster if requested
    clusters_to_run = CLUSTER_PRIORITY
    if args.cluster:
        cluster_name = args.cluster.upper()
        if cluster_name not in targets_by_cluster:
            logger.error("Unknown cluster: %s (valid: %s)", cluster_name, ", ".join(CLUSTER_PRIORITY))
            sys.exit(1)
        clusters_to_run = [cluster_name]

    total_targets = sum(len(targets_by_cluster[c]) for c in clusters_to_run)
    logger.info("Running %d clusters, %d total L&I sourcing queries", len(clusters_to_run), total_targets)

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
            target_state, target_zips = target
            zip_key = ",".join(target_zips) if target_zips else "ALL"
            target_key = f"{cluster_name}|{target_state}|{zip_key}"

            # Skip already-completed targets (resume)
            if target_key in completed_targets:
                logger.info("Skipping completed target: %s", target_key)
                continue

            location_str = (
                f"{target_state} [{zip_key}]" if target_zips else target_state
            )
            logger.info(
                "Target %d/%d: %s [%s]",
                target_idx, total_targets, location_str, cluster_name,
            )

            # South FL / Central FL: slightly relaxed score floor.
            # Safety vetting (OOS, crash rate) stays strict regardless.
            if cluster_name in ("SOUTH_FL", "CENTRAL_FL"):
                effective_min_score = min(args.min_score, 25)
                effective_min_fleet = max(args.min_fleet, RULES.fleet_min) if args.min_fleet <= RULES.fleet_min else args.min_fleet
            else:
                effective_min_score = args.min_score
                effective_min_fleet = args.min_fleet

            # Source from L&I SQLite, hydrate via QCMobile, vet
            qualified = search_cluster_carriers(
                state=target_state,
                zip_prefixes=target_zips,
                limit=args.limit,
                min_score=effective_min_score,
                min_fleet=effective_min_fleet,
                seen_dots=seen_dots,
                dry_run=args.dry_run,
                cluster_name=cluster_name,
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
  python -m scripts.prospect_carriers --state MN --buckets flatbed,dry_van,box_truck --limit 5
  python -m scripts.prospect_carriers --state OH --buckets flatbed,dry_van,reefer,box_truck --limit 10
  python -m scripts.prospect_carriers --state TX --zip-prefixes 750,751,752 --limit 10
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
    # ── State-mode flags (replaces state-specific scripts like mn/oh/tx_carrier_search) ──
    parser.add_argument(
        "--state", type=str, default=None,
        help=(
            "Run a direct single-state search instead of cluster mode. "
            "Example: --state MN. Mutually exclusive with --cluster."
        ),
    )
    parser.add_argument(
        "--buckets", type=str, default=None,
        help=(
            "Comma-separated equipment buckets for --state mode. "
            "Valid values: flatbed,dry_van,box_truck,reefer. "
            "Default (when --state used): flatbed,dry_van,box_truck."
        ),
    )
    parser.add_argument(
        "--zip-prefixes", type=str, default=None,
        dest="zip_prefixes",
        help=(
            "Comma-separated ZIP code prefixes to narrow --state sourcing. "
            "Example: --zip-prefixes 330,331,332 (South FL). "
            "Default: all ZIPs for the state."
        ),
    )
    # ── Shared flags ──────────────────────────────────────────────────────────
    parser.add_argument(
        "--limit", type=int, default=30,
        help="Max carriers per bucket/target (default: 30; use 5-10 for state-mode)",
    )
    parser.add_argument(
        "--min-score", type=int, default=40,
        help="Minimum carrier score threshold (default: 40)",
    )
    parser.add_argument(
        "--min-fleet", type=int, default=RULES.fleet_min,
        help=f"Minimum power units (default: {RULES.fleet_min} — from RULES.fleet_min)",
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


# ── State-mode pipeline ────────────────────────────────────────────────────────
# Replaces mn_carrier_search_20260415.py, oh_carrier_search_20260415.py,
# tx_carrier_search_20260415.py. Those scripts called insert_carrier() directly
# without running EXCLUDED_SERVICE_TYPE_PATTERNS. This path runs the full
# search_cluster_carriers() flow which enforces the denylist at line ~515.


def _bucket_flags_for_carrier(carrier: dict, buckets: list[str]) -> list[str]:
    """Return which of the requested buckets this carrier qualifies for."""
    eq = (carrier.get("Equipment_Types") or "").upper()
    cargo_carried = ""
    raw = carrier.get("_raw") or {}
    if isinstance(raw, dict):
        cargo_carried = str(raw.get("cargoCarried") or "").upper()
    units = int(carrier.get("Power_Units") or 0)

    matched: list[str] = []
    if "flatbed" in buckets and "FLATBED" in eq:
        matched.append("flatbed")
    if "dry_van" in buckets and "DRY_VAN" in eq:
        matched.append("dry_van")
    if "reefer" in buckets and "REEFER" in eq:
        matched.append("reefer")
    if "box_truck" in buckets:
        is_general = "GENERAL FREIGHT" in cargo_carried or "GEN FREIGHT" in cargo_carried
        has_other = any(t in eq for t in ("FLATBED", "REEFER", "TANKER"))
        if is_general and 3 <= units <= 25 and not has_other:
            matched.append("box_truck")
    return matched


def run_state_search(args: argparse.Namespace) -> dict[str, Any]:
    """Execute a direct single-state search with per-bucket TOP-N logic.

    This is the canonical replacement for the deprecated per-state scripts
    (mn/oh/tx_carrier_search_20260415.py). Key contract:
      - Denylist (EXCLUDED_SERVICE_TYPE_PATTERNS) runs via search_cluster_carriers()
      - Vetting gate (vet_carrier, score_carrier) runs via search_cluster_carriers()
      - insert_carrier / enrich_and_store called via the same path as cluster mode
      - Preferred_Lanes tagged as {STATE}_{BUCKET_UPPER}
    """
    state = (args.state or "").upper().strip()
    buckets_raw = args.buckets or "flatbed,dry_van,box_truck"
    buckets = [b.strip().lower() for b in buckets_raw.split(",") if b.strip()]
    zip_prefixes: Optional[tuple[str, ...]] = None
    if args.zip_prefixes:
        zip_prefixes = tuple(z.strip() for z in args.zip_prefixes.split(",") if z.strip())

    valid_buckets = {"flatbed", "dry_van", "box_truck", "reefer"}
    unknown = set(buckets) - valid_buckets
    if unknown:
        logger.error("Unknown bucket(s): %s. Valid: %s", unknown, valid_buckets)
        sys.exit(1)

    top_n = args.limit  # --limit controls TOP-N per bucket in state mode
    # Overfetch so the vetting gate still fills the bucket after rejections.
    # 1.5x is conservative — can be raised if state yields thin results.
    sourcing_limit = max(top_n * 8, 50)

    logger.info(
        "State mode: state=%s buckets=%s zip_prefixes=%s top_n=%d sourcing_limit=%d dry_run=%s",
        state, buckets, zip_prefixes, top_n, sourcing_limit, args.dry_run,
    )

    # Pre-seed dedup from main tab + Quarantine (same pattern as state scripts)
    seen_dots: set[str] = set()
    try:
        for c in get_all_carriers():
            dot = (c.get("DOT Number") or c.get("DOT_Number") or "").strip()
            if dot:
                seen_dots.add(str(int(dot)) if dot.isdigit() else dot)
        logger.info("seen_dots pre-seeded from main tab: %d", len(seen_dots))
    except Exception as exc:
        logger.warning("main-tab dedup load failed: %s", exc)

    # Source candidates — denylist enforced inside search_cluster_carriers()
    qualified = search_cluster_carriers(
        state=state,
        zip_prefixes=zip_prefixes,
        limit=sourcing_limit,
        min_score=args.min_score,
        min_fleet=args.min_fleet,
        seen_dots=seen_dots,
        dry_run=args.dry_run,
        cluster_name=f"{state}_STATE",
        min_bipd=RULES.liability_min,
    )

    logger.info("State sourcing done: %d qualified before bucket split", len(qualified))

    # Partition into buckets
    bucket_entries: dict[str, list[dict]] = {b: [] for b in buckets}
    for c in qualified:
        for bucket in _bucket_flags_for_carrier(c, buckets):
            bucket_entries[bucket].append(c)

    stats: dict[str, Any] = {
        "state": state,
        "buckets_requested": buckets,
        "zip_prefixes": list(zip_prefixes) if zip_prefixes else None,
        "top_n": top_n,
        "qualified_pre_split": len(qualified),
        "bucket_counts": {b: len(v) for b, v in bucket_entries.items()},
        "written_dots": [],
        "insert_errors": [],
        "dry_run": args.dry_run,
    }

    logger.info("Bucket counts pre-rank: %s", stats["bucket_counts"])

    written_this_run: set[str] = set()

    for bucket_name, entries in bucket_entries.items():
        # Sort by score desc
        entries.sort(key=lambda c: c.get("Carrier_Score", 0), reverse=True)
        top = entries[:top_n]
        logger.info("Bucket %s: writing top %d of %d", bucket_name, len(top), len(entries))

        inserted_count = 0
        for c in top:
            if inserted_count >= top_n:
                break
            dot = str(c.get("DOT_Number", "") or "").strip()
            if not dot or dot in written_this_run:
                continue

            # Tag preferred lane as STATE_BUCKET
            c["_state_bucket"] = f"{state}_{bucket_name.upper()}"
            cluster_tag = f"{state}_{bucket_name.upper()}"

            try:
                stored = enrich_and_store(c, cluster_tag, dry_run=args.dry_run)
                written_this_run.add(dot)
                stats["written_dots"].append(dot)
                inserted_count += 1
                logger.info(
                    "Stored DOT %s (%s) → bucket %s",
                    dot, c.get("Legal_Name"), bucket_name,
                )
            except Exception as exc:
                stats["insert_errors"].append({
                    "dot": dot, "name": c.get("Legal_Name"),
                    "bucket": bucket_name, "error": str(exc),
                })
                logger.error("Store failed for DOT %s: %s", dot, exc)

    stats["total_written"] = len(stats["written_dots"])
    logger.info(
        "State search complete — state=%s written=%d errors=%d",
        state, stats["total_written"], len(stats["insert_errors"]),
    )
    return stats


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

    # Mutual exclusivity: --state and --cluster cannot be used together
    if args.state and args.cluster:
        logger.error("--state and --cluster are mutually exclusive. Pick one.")
        sys.exit(1)

    if args.state:
        # State mode — direct single-state search replacing deprecated per-state scripts
        logger.info("Running in state mode: --state %s", args.state)
        logger.info("  Buckets:    %s", args.buckets or "flatbed,dry_van,box_truck (default)")
        logger.info("  ZIP prefix: %s", args.zip_prefixes or "ALL")
        logger.info("  Limit:      %d per bucket", args.limit)
        logger.info("  Dry run:    %s", args.dry_run)
        stats = run_state_search(args)
        if stats["total_written"] == 0 and not args.dry_run:
            logger.warning("No carriers written — check state/bucket/sourcing parameters.")
            sys.exit(1)
    else:
        # Cluster mode (original behavior)
        logger.info("Starting prospect carrier pipeline (cluster mode)...")
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
