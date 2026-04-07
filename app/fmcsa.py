"""
BrokerOps AI – FMCSA Census API integration.

Searches for carriers by name/state/DOT, retrieves details, and scores them
using the weighted scoring model from the carrier phase spec.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, date
from typing import Any, Optional

import httpx

from app.config import get_settings

logger = logging.getLogger("brokerops.fmcsa")

_BASE_URL = "https://mobile.fmcsa.dot.gov/qc/services/carriers"

# Simple in-memory cache: {cache_key: (timestamp, data)}
_cache: dict[str, tuple[float, Any]] = {}
_CACHE_TTL = 3600  # 1 hour


def _cached_get(url: str, params: dict | None = None, ttl: int = _CACHE_TTL) -> dict:
    """GET with simple TTL cache to respect FMCSA rate limits."""
    import hashlib, json
    key = hashlib.md5(f"{url}|{json.dumps(params, sort_keys=True)}".encode()).hexdigest()
    now = time.time()
    if key in _cache:
        ts, data = _cache[key]
        if now - ts < ttl:
            return data

    settings = get_settings()
    api_key = settings.FMCSA_API_KEY
    if not api_key:
        raise ValueError("FMCSA_API_KEY not configured")

    full_params = {"webKey": api_key}
    if params:
        full_params.update(params)

    resp = httpx.get(url, params=full_params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    _cache[key] = (now, data)
    return data


# ── Search ──────────────────────────────────────────────────────────────────


def search_carriers(
    state: str,
    city: str | None = None,
    equipment_type: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Search FMCSA Census API for carriers by state (and optionally city).

    Uses freight-related search terms to find actual trucking/freight carriers
    rather than random single-letter searches that return passenger carriers.

    Returns raw carrier dicts from the API (normalized).
    """
    # Search freight-related company names to find actual trucking carriers.
    # Single-letter searches return too many passenger/non-freight carriers.
    search_terms = [
        "trucking", "freight", "transport", "logistics", "express",
        "hauling", "carrier", "moving", "delivery", "refrigerated",
        "cold", "reefer",
    ]

    # Add equipment-specific terms if searching for specific types
    if equipment_type:
        eq = equipment_type.upper()
        if "REEFER" in eq or "REFRIG" in eq:
            search_terms = ["refrigerated", "cold", "reefer", "frozen", "temp",
                           "freight", "trucking", "transport", "logistics", "express"]
        elif "FLAT" in eq:
            search_terms = ["flatbed", "steel", "heavy", "haul", "building",
                           "construction", "lumber", "materials", "freight",
                           "trucking", "transport", "logistics", "carrier"]

    seen_dots: set[str] = set()
    all_carriers: list[dict] = []
    per_term = max(limit // 3, 10)

    for term in search_terms:
        if len(all_carriers) >= limit:
            break

        url = f"{_BASE_URL}/name/{term}"
        params: dict[str, Any] = {"stateAbbrev": state.upper(), "size": str(per_term)}
        if city:
            params["city"] = city.upper()

        try:
            data = _cached_get(url, params)
        except Exception as exc:
            logger.debug("FMCSA search '%s' failed: %s", term, exc)
            continue

        content = data.get("content", [])
        if not content:
            continue

        for item in content:
            carrier_data = item.get("carrier", item)
            normalized = _normalize_carrier(carrier_data)
            if normalized:
                dot = normalized.get("DOT_Number", "")
                if dot and dot not in seen_dots:
                    seen_dots.add(dot)
                    all_carriers.append(normalized)

    logger.info(
        "FMCSA search: found %d carriers in %s %s (terms: %s)",
        len(all_carriers), city or "", state, ", ".join(search_terms[:3]),
    )
    return all_carriers


def get_carrier_details(dot_number: str) -> Optional[dict]:
    """Fetch full carrier profile from Census API by DOT number.

    Calls multiple FMCSA endpoints to get complete data:
    - /carriers/{dot} — basic info
    - /carriers/{dot}/cargo-carried — cargo types (for equipment detection)
    - /carriers/{dot}/docket-number — MC/docket numbers
    """
    url = f"{_BASE_URL}/{dot_number}"
    try:
        data = _cached_get(url)
    except Exception as exc:
        logger.error("FMCSA detail fetch failed for DOT %s: %s", dot_number, exc)
        return None

    content = data.get("content", data)
    if isinstance(content, list) and content:
        content = content[0]
    carrier_data = content.get("carrier", content) if isinstance(content, dict) else {}
    if not carrier_data:
        return None

    # Fetch cargo-carried data (separate endpoint)
    try:
        cargo_data = _cached_get(f"{_BASE_URL}/{dot_number}/cargo-carried")
        cargo_content = cargo_data.get("content", [])
        if isinstance(cargo_content, list):
            cargo_descriptions = [
                str(item.get("cargoCarriedDesc", item.get("cargoClassDesc", "")))
                for item in cargo_content
                if isinstance(item, dict)
            ]
            carrier_data["cargoCarried"] = ", ".join(d for d in cargo_descriptions if d)
            carrier_data["_cargoCarriedRaw"] = cargo_content
    except Exception as exc:
        logger.debug("Cargo-carried fetch failed for DOT %s: %s", dot_number, exc)

    # Fetch docket/MC numbers (separate endpoint)
    try:
        docket_data = _cached_get(f"{_BASE_URL}/{dot_number}/docket-number")
        docket_content = docket_data.get("content", [])
        if isinstance(docket_content, list):
            for item in docket_content:
                if isinstance(item, dict):
                    prefix = str(item.get("prefix", "")).upper()
                    docket_num = str(item.get("docketNumber", ""))
                    if prefix == "MC" and docket_num:
                        carrier_data["docketNumber"] = docket_num
                        break
    except Exception as exc:
        logger.debug("Docket-number fetch failed for DOT %s: %s", dot_number, exc)

    normalized = _normalize_carrier(carrier_data)

    # Fetch BASICS/safety data (crash counts, inspection details)
    if normalized:
        try:
            basics = get_carrier_basics(dot_number)
            if basics:
                normalized["Crash_Total"] = basics.get("Crash_Total", 0)
                normalized["Fatal_Crash"] = basics.get("Fatal_Crash", 0)
                normalized["Injury_Crash"] = basics.get("Injury_Crash", 0)
                normalized["Tow_Crash"] = basics.get("Tow_Crash", 0)
                normalized["Vehicle_Insp"] = basics.get("Vehicle_Insp", 0)
                normalized["Vehicle_OOS_Insp"] = basics.get("Vehicle_OOS_Insp", 0)
                normalized["Driver_Insp"] = basics.get("Driver_Insp", 0)
                normalized["Driver_OOS_Insp"] = basics.get("Driver_OOS_Insp", 0)
                # Recompute OOS rates from BASICS if available (more reliable)
                if basics.get("Vehicle_OOS_Rate") is not None:
                    normalized["Vehicle_OOS_Rate"] = basics["Vehicle_OOS_Rate"]
                if basics.get("Driver_OOS_Rate") is not None:
                    normalized["Driver_OOS_Rate"] = basics["Driver_OOS_Rate"]
                # Compute crash rate per 100 power units
                power_units = normalized.get("Power_Units", 0)
                crash_total = basics.get("Crash_Total", 0)
                if power_units > 0 and crash_total > 0:
                    normalized["Crash_Rate_Per100"] = round(
                        (crash_total / power_units) * 100, 2
                    )
                else:
                    normalized["Crash_Rate_Per100"] = 0.0
        except Exception as exc:
            logger.debug("BASICS fetch failed for DOT %s: %s", dot_number, exc)

    return normalized


# ── BASICS / Safety Data ───────────────────────────────────────────────────


def get_carrier_basics(dot_number: str) -> Optional[dict]:
    """Fetch BASICS safety data (crash counts, inspection stats) for a carrier.

    Queries: /carriers/{dot}/basics
    Returns a normalized dict with crash/inspection fields, or None on failure.
    """
    url = f"{_BASE_URL}/{dot_number}/basics"
    try:
        data = _cached_get(url)
    except Exception as exc:
        logger.error("FMCSA BASICS fetch failed for DOT %s: %s", dot_number, exc)
        return None

    content = data.get("content", data)
    if isinstance(content, list) and content:
        content = content[0]
    if not isinstance(content, dict):
        return None

    # The BASICS endpoint may nest data under "basics" or return flat
    basics_data = content.get("basics", content)

    # Some responses return a list of BASIC categories; merge them
    if isinstance(basics_data, list):
        merged: dict[str, Any] = {}
        for item in basics_data:
            if isinstance(item, dict):
                merged.update(item)
        basics_data = merged

    return {
        "Crash_Total": _safe_int(basics_data.get("crashTotal", 0)),
        "Fatal_Crash": _safe_int(basics_data.get("fatalCrash", 0)),
        "Injury_Crash": _safe_int(basics_data.get("injCrash", 0)),
        "Tow_Crash": _safe_int(basics_data.get("towCrash", 0)),
        "Vehicle_Insp": _safe_int(basics_data.get("vehicleInsp", 0)),
        "Vehicle_OOS_Insp": _safe_int(basics_data.get("vehicleOosInsp", 0)),
        "Driver_Insp": _safe_int(basics_data.get("driverInsp", 0)),
        "Driver_OOS_Insp": _safe_int(basics_data.get("driverOosInsp", 0)),
        "Vehicle_OOS_Rate": _safe_float(basics_data.get("vehicleOosRate",
                                         basics_data.get("vehicleOosRatePercent"))),
        "Driver_OOS_Rate": _safe_float(basics_data.get("driverOosRate",
                                        basics_data.get("driverOosRatePercent"))),
    }


def get_carrier_inspections(dot_number: str) -> Optional[dict]:
    """Fetch crash and inspection summary for a carrier.

    Convenience wrapper that returns crash rate per 100 power units
    alongside raw counts. Requires carrier details for power unit count.

    Returns dict with keys:
        crash_total, fatal_crash, injury_crash, tow_crash,
        vehicle_insp, vehicle_oos_insp, driver_insp, driver_oos_insp,
        power_units, crash_rate_per_100
    Or None if data unavailable.
    """
    basics = get_carrier_basics(dot_number)
    if not basics:
        return None

    # We need power units from the main carrier endpoint
    carrier_url = f"{_BASE_URL}/{dot_number}"
    power_units = 0
    try:
        carrier_data = _cached_get(carrier_url)
        content = carrier_data.get("content", carrier_data)
        if isinstance(content, list) and content:
            content = content[0]
        raw = content.get("carrier", content) if isinstance(content, dict) else {}
        power_units = _safe_int(raw.get("totalPowerUnits", 0))
    except Exception as exc:
        logger.debug("Power units fetch failed for DOT %s: %s", dot_number, exc)

    crash_total = basics.get("Crash_Total", 0)
    crash_rate = 0.0
    if power_units > 0 and crash_total > 0:
        crash_rate = round((crash_total / power_units) * 100, 2)

    return {
        "crash_total": crash_total,
        "fatal_crash": basics.get("Fatal_Crash", 0),
        "injury_crash": basics.get("Injury_Crash", 0),
        "tow_crash": basics.get("Tow_Crash", 0),
        "vehicle_insp": basics.get("Vehicle_Insp", 0),
        "vehicle_oos_insp": basics.get("Vehicle_OOS_Insp", 0),
        "driver_insp": basics.get("Driver_Insp", 0),
        "driver_oos_insp": basics.get("Driver_OOS_Insp", 0),
        "power_units": power_units,
        "crash_rate_per_100": crash_rate,
    }


def _normalize_carrier(raw: dict) -> Optional[dict]:
    """Normalize FMCSA API response fields to our internal format."""
    dot = str(raw.get("dotNumber", raw.get("dot_number", "")))
    if not dot:
        return None

    # Extract MC number from docket numbers if available
    mc_number = ""
    docket = raw.get("docketNumber", raw.get("mcNumber", ""))
    if docket:
        mc_number = str(docket).replace("MC-", "").replace("MC", "").strip()

    # Authority status
    auth_status_raw = (raw.get("authorizationStatus", "") or
                       raw.get("commonAuthorityStatus", "") or
                       raw.get("allowedToOperate", ""))
    auth_status = _normalize_authority_status(str(auth_status_raw))

    # Authority date
    auth_date = raw.get("statusDate", raw.get("authGrantDate", ""))

    # Insurance
    liability = _safe_int(raw.get("bipdInsuranceOnFile", raw.get("insuranceRequired", 0)))
    cargo = _safe_int(raw.get("cargoInsuranceOnFile", 0))

    # Safety
    safety_rating = raw.get("safetyRating", raw.get("ratingCode", ""))
    veh_oos = _safe_float(raw.get("vehicleOosRate", raw.get("vehicleOosRatePercent", 0)))
    drv_oos = _safe_float(raw.get("driverOosRate", raw.get("driverOosRatePercent", 0)))

    # Fleet
    power_units = _safe_int(raw.get("totalPowerUnits", 0))
    drivers = _safe_int(raw.get("totalDrivers", 0))

    # Contact
    phone = raw.get("phoneNumber", raw.get("telephone", ""))
    email = raw.get("emailAddress", "")

    # Equipment detection
    equipment_types = _detect_equipment(raw)

    return {
        "DOT_Number": dot,
        "MC_Number": mc_number,
        "Legal_Name": raw.get("legalName", raw.get("carrierName", "")),
        "DBA_Name": raw.get("dbaName", ""),
        "City": raw.get("phyCity", raw.get("city", "")),
        "State": raw.get("phyState", raw.get("state", "")),
        "Zip": raw.get("phyZipcode", raw.get("zipCode", "")),
        "Contact_Phone": phone,
        "Contact_Email": email,
        "Authority_Status": auth_status,
        "Authority_Date": str(auth_date),
        "Insurance_Liability": liability,
        "Insurance_Cargo": cargo,
        "Safety_Rating": _normalize_safety_rating(str(safety_rating)),
        "Vehicle_OOS_Rate": veh_oos,
        "Driver_OOS_Rate": drv_oos,
        "Power_Units": power_units,
        "Driver_Count": drivers,
        "Equipment_Types": ",".join(equipment_types) if equipment_types else "",
        "OOS_Active": raw.get("oosStatus", "") == "Y",
        # Raw data for scoring
        "_raw": raw,
    }


def _normalize_authority_status(status: str) -> str:
    status = status.upper().strip()
    if status in ("AUTHORIZED", "ACTIVE", "A", "Y"):
        return "ACTIVE"
    if status in ("REVOKED", "REVOKED-LOSS OF INSURANCE", "R"):
        return "REVOKED"
    if status in ("SUSPENDED", "S"):
        return "SUSPENDED"
    if status in ("NOT AUTHORIZED", "INACTIVE", "N"):
        return "INACTIVE"
    return status or "UNKNOWN"


def _normalize_safety_rating(rating: str) -> str:
    rating = rating.upper().strip()
    if rating in ("S", "SATISFACTORY"):
        return "SATISFACTORY"
    if rating in ("C", "CONDITIONAL"):
        return "CONDITIONAL"
    if rating in ("U", "UNSATISFACTORY"):
        return "UNSATISFACTORY"
    return rating or "NONE"


def _detect_equipment(raw: dict) -> list[str]:
    """Detect equipment types from FMCSA cargo/operation codes.

    The cargoCarried field is populated from the /cargo-carried endpoint
    as a comma-separated string of cargoClassDesc values like:
    "General Freight, Refrigerated Food, Fresh Produce"
    """
    types: set[str] = set()
    cargo_carried = str(raw.get("cargoCarried", "")).upper()
    classification = str(raw.get("operationClassification", "")).upper()

    # General freight → dry van
    if any(k in cargo_carried for k in ["GENERAL FREIGHT", "GEN FREIGHT",
                                         "HOUSEHOLD GOODS", "COMMODITIES"]):
        types.add("DRY_VAN")
    # Refrigerated / temperature-controlled
    if any(k in cargo_carried for k in ["REFRIGERATED", "TEMP CONTROLLED",
                                         "FRESH PRODUCE", "FROZEN", "MEAT",
                                         "BEVERAGES", "FOOD"]):
        types.add("REEFER")
    # Flatbed indicators
    if any(k in cargo_carried for k in ["METAL", "BUILDING MATERIAL", "MACHINERY",
                                         "LUMBER", "LARGE OBJECTS", "CONSTRUCTION",
                                         "INTERMODAL"]):
        types.add("FLATBED")
    # Oversize
    if "OVERSIZE" in cargo_carried or "OVERWEIGHT" in cargo_carried:
        types.add("FLATBED")
    # Tanker
    if any(k in cargo_carried for k in ["CHEMICALS", "LIQUIDS", "GASES"]):
        types.add("TANKER")

    # If nothing detected, assume dry van for authorized carriers
    if not types and "PASSENGER" not in cargo_carried:
        types.add("DRY_VAN")

    return sorted(types)


def _safe_int(val: Any) -> int:
    try:
        return int(float(str(val).replace(",", "").replace("$", "")))
    except (ValueError, TypeError):
        return 0


def _safe_float(val: Any) -> float:
    try:
        return float(str(val).replace(",", "").replace("%", ""))
    except (ValueError, TypeError):
        return 0.0


# ── Scoring ─────────────────────────────────────────────────────────────────


def score_carrier(carrier: dict) -> int:
    """Score a carrier 0-100 based on the weighted criteria model.

    Returns -1 for hard-disqualified carriers.

    Scoring weight priority (highest to lowest):
        1. Safety record — OOS rates, crash rates, safety rating  (35 pts)
        2. Equipment verification & fleet size                     (25 pts)
        3. Carrier age / authority history (forgiving to young)    (20 pts)
        4. Insurance above minimums (small tiebreaker bonus only)  (10 pts)
        5. Complaint history                                       (10 pts)
    """
    score = 0
    name = carrier.get("Legal_Name", "unknown")

    # ══════════════════════════════════════════════════════════════
    # HARD DISQUALIFIERS — any of these → immediate reject (-1)
    # ══════════════════════════════════════════════════════════════

    # Authority must be active
    if carrier.get("Authority_Status") != "ACTIVE":
        logger.debug("Disqualified %s: authority=%s", name, carrier.get("Authority_Status"))
        return -1

    # Not allowed to operate / OOS
    if carrier.get("OOS_Active"):
        logger.debug("Disqualified %s: OOS active", name)
        return -1

    # Unsatisfactory safety rating
    if carrier.get("Safety_Rating") == "UNSATISFACTORY":
        logger.debug("Disqualified %s: unsatisfactory safety", name)
        return -1

    # ── Insurance hard floors ──────────────────────────────────
    # $1,000,000 liability (BIPD) = minimum floor
    # $100,000 cargo = minimum floor
    # If insurance data is missing (0), skip rather than disqualify —
    # the name search endpoint doesn't always return insurance.
    liability = carrier.get("Insurance_Liability", 0)
    cargo = carrier.get("Insurance_Cargo", 0)
    if liability > 0 and liability < 1_000_000:
        logger.debug("Disqualified %s: liability=%d < $1M floor", name, liability)
        return -1
    if cargo > 0 and cargo < 100_000:
        logger.debug("Disqualified %s: cargo=%d < $100K floor", name, cargo)
        return -1

    # ── Fleet size hard floor: 3 power units minimum ──────────
    units = carrier.get("Power_Units", 0)
    if units > 0 and units < 3:
        logger.debug("Disqualified %s: fleet size %d < 3 minimum", name, units)
        return -1

    # ── Shell / stale carrier: 0 drivers with >0 units ────────
    drivers = carrier.get("Driver_Count", 0)
    if units > 0 and drivers == 0:
        logger.debug("Disqualified %s: 0 drivers with %d units (shell/stale)", name, units)
        return -1

    # ── Vehicle OOS rate > 30% ────────────────────────────────
    veh_oos = carrier.get("Vehicle_OOS_Rate", 0)
    if veh_oos > 30:
        logger.debug("Disqualified %s: vehicle OOS rate %.1f%% > 30%%", name, veh_oos)
        return -1

    # ── Driver OOS rate > 15% ─────────────────────────────────
    drv_oos = carrier.get("Driver_OOS_Rate", 0)
    if drv_oos > 15:
        logger.debug("Disqualified %s: driver OOS rate %.1f%% > 15%%", name, drv_oos)
        return -1

    # ── Crash rate > 30 per 100 power units ───────────────────
    crash_rate = _safe_float(carrier.get("Crash_Rate_Per100", 0))
    if crash_rate > 30:
        logger.debug("Disqualified %s: crash rate %.1f > 30 per 100 units", name, crash_rate)
        return -1

    # ══════════════════════════════════════════════════════════════
    # SCORING — weighted by priority
    # ══════════════════════════════════════════════════════════════

    # ── 1. Safety Record (35 pts) ──────────────────────────────
    # Safety rating component (20 pts)
    safety = carrier.get("Safety_Rating", "NONE")
    if safety == "SATISFACTORY":
        score += 20
    elif safety == "CONDITIONAL":
        score += 10
    elif safety == "NONE":
        score += 14  # no rating = neutral, slight benefit of doubt

    # OOS rate component (15 pts) — lower is better
    if veh_oos <= 5:
        score += 8
    elif veh_oos <= 15:
        score += 5
    elif veh_oos <= 25:
        score += 2
    # 25-30 = 0 pts (borderline, already close to hard reject)

    if drv_oos <= 5:
        score += 7
    elif drv_oos <= 10:
        score += 4
    elif drv_oos <= 14:
        score += 1
    # 14-15 = 0 pts (borderline)

    # ── 2. Equipment & Fleet Size (25 pts) ─────────────────────
    if units >= 51:
        score += 25
    elif units >= 21:
        score += 22
    elif units >= 11:
        score += 18
    elif units >= 6:
        score += 14
    elif units >= 3:
        score += 10
    # < 3 already hard-rejected above

    # ── 3. Authority Age / History (20 pts — forgiving) ────────
    # Don't auto-reject young authorities; just score them lower.
    auth_date_str = carrier.get("Authority_Date", "")
    auth_age_months = _authority_age_months(auth_date_str)
    if auth_age_months >= 36:
        score += 20
    elif auth_age_months >= 18:
        score += 16
    elif auth_age_months >= 12:
        score += 12
    elif auth_age_months >= 6:
        score += 8
    elif auth_age_months > 0:
        score += 4  # new but not penalized to zero — forgiving
    # auth_age_months == 0 (unknown) = 0 pts

    # ── 4. Insurance Above Minimums (10 pts — tiebreaker only) ─
    # $1M/$100K = full credit. Above = small bonus. This is NOT
    # a major differentiator — just a tiebreaker.
    if liability >= 1_000_000:
        score += 5  # meets floor = full base credit
    if liability >= 2_000_000:
        score += 2  # small bonus for extra coverage
    if cargo >= 100_000:
        score += 2  # meets floor = full base credit
    if cargo >= 250_000:
        score += 1  # small bonus for extra coverage

    # ── 5. Complaint History (10 pts) ──────────────────────────
    # FMCSA doesn't reliably return complaints; default to full credit
    complaints = _safe_int(carrier.get("_raw", {}).get("complaintCount", 0))
    if complaints == 0:
        score += 10
    elif complaints <= 2:
        score += 7
    elif complaints <= 5:
        score += 3
    # 6+ = 0 pts

    return max(score, 0)


def classify_onboarding_tier(carrier: dict, score: int) -> dict:
    """Classify a carrier into an onboarding tier based on score and risk signals.

    Returns dict with:
        tier: STANDARD | ENHANCED | MANUAL_REVIEW
        load_cap: max load value for first 3 loads (None = no cap)
        requirements: list of additional onboarding steps
        reason: why this tier was assigned
    """
    reasons = []
    requirements = []

    # Authority age risk
    auth_age = _authority_age_months(carrier.get("Authority_Date", ""))
    if auth_age < 6:
        reasons.append(f"authority {auth_age}mo (<6)")
    elif auth_age < 12:
        reasons.append(f"authority {auth_age}mo (<12)")

    # Insurance data missing — needs COI verification
    liability = carrier.get("Insurance_Liability", 0)
    cargo = carrier.get("Insurance_Cargo", 0)
    if liability == 0 or cargo == 0:
        reasons.append("insurance data missing from FMCSA")
        requirements.append("VERIFY_COI_WITH_INSURER")

    # Inspection confidence — no inspections means unknown risk
    inspections = _safe_int(carrier.get("_raw", {}).get("inspectionCount", 0))
    if inspections == 0:
        reasons.append("no inspection history")

    # Small fleet
    units = carrier.get("Power_Units", 0)
    if units <= 5:
        reasons.append(f"small fleet ({units} units)")

    # Tier assignment
    if score >= 60 and len(reasons) == 0:
        return {
            "tier": "STANDARD",
            "load_cap": None,
            "requirements": ["W9", "COI", "AUTHORITY_CHECK", "ACH"],
            "reason": "score 60+, no risk flags",
        }
    elif score >= 35:
        reqs = ["W9", "COI", "AUTHORITY_CHECK", "ACH", "VERIFY_COI_WITH_INSURER", "BROKER_REFERENCE"]
        for r in requirements:
            if r not in reqs:
                reqs.append(r)
        return {
            "tier": "ENHANCED",
            "load_cap": 15_000,
            "requirements": reqs,
            "reason": "; ".join(reasons) if reasons else "score 35-59",
        }
    else:
        reqs = ["W9", "COI", "AUTHORITY_CHECK", "ACH", "VERIFY_COI_WITH_INSURER",
                "BROKER_REFERENCE", "DEREK_APPROVAL"]
        for r in requirements:
            if r not in reqs:
                reqs.append(r)
        return {
            "tier": "MANUAL_REVIEW",
            "load_cap": 10_000,
            "requirements": reqs,
            "reason": "; ".join(reasons) if reasons else "score <35",
        }


def _authority_age_months(date_str: str) -> int:
    """Calculate months since authority was granted."""
    if not date_str:
        return 0
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%d-%b-%Y"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            delta = date.today() - dt.date()
            return max(delta.days // 30, 0)
        except ValueError:
            continue
    return 0


# ── Strict Vetting ─────────────────────────────────────────────────────────


def vet_carrier_strict(
    carrier: dict,
    equipment_types: list[str] | None = None,
) -> tuple[bool, str]:
    """Apply hard-reject vetting rules that go beyond score_carrier().

    This runs AFTER score_carrier (which handles authority, insurance, safety
    rating). It enforces the stricter operational thresholds:

        - Vehicle OOS rate > 30%  →  REJECT
        - Driver OOS rate  > 15%  →  REJECT
        - Crash rate > 30 per 100 power units  →  REJECT
        - Reefer equipment + ANY vehicle maintenance OOS  →  REJECT

    Args:
        carrier: Normalized carrier dict (from get_carrier_details / _normalize_carrier).
        equipment_types: Optional list of equipment type strings (e.g. ["REEFER"]).
                         If None, falls back to carrier["Equipment_Types"].

    Returns:
        (passed, rejection_reason) – passed=True means carrier cleared strict vetting.
        If passed=False, rejection_reason explains which rule triggered the reject.
    """
    name = carrier.get("Legal_Name", "unknown")

    # ── Fleet size < 3 power units ────────────────────────────
    units = _safe_int(carrier.get("Power_Units", 0))
    if units > 0 and units < 3:
        reason = (
            f"REJECT: Fleet size {units} power units below 3 minimum"
        )
        logger.info("Strict vet REJECT %s: %s", name, reason)
        return False, reason

    # ── Shell / stale carrier: 0 drivers with >0 units ────────
    drivers = _safe_int(carrier.get("Driver_Count", 0))
    if units > 0 and drivers == 0:
        reason = (
            f"REJECT: 0 drivers with {units} power units (shell/stale carrier)"
        )
        logger.info("Strict vet REJECT %s: %s", name, reason)
        return False, reason

    # ── Vehicle OOS rate > 30% ─────────────────────────────────
    veh_oos = _safe_float(carrier.get("Vehicle_OOS_Rate", 0))
    if veh_oos > 30:
        reason = (
            f"REJECT: Vehicle OOS rate {veh_oos:.1f}% exceeds 30% threshold"
        )
        logger.info("Strict vet REJECT %s: %s", name, reason)
        return False, reason

    # ── Driver OOS rate > 15% ──────────────────────────────────
    drv_oos = _safe_float(carrier.get("Driver_OOS_Rate", 0))
    if drv_oos > 15:
        reason = (
            f"REJECT: Driver OOS rate {drv_oos:.1f}% exceeds 15% threshold"
        )
        logger.info("Strict vet REJECT %s: %s", name, reason)
        return False, reason

    # ── Crash rate > 30 per 100 power units ────────────────────
    crash_rate = _safe_float(carrier.get("Crash_Rate_Per100", 0))
    if crash_rate > 30:
        reason = (
            f"REJECT: Crash rate {crash_rate:.1f} per 100 power units "
            f"exceeds 30 threshold"
        )
        logger.info("Strict vet REJECT %s: %s", name, reason)
        return False, reason

    # ── Reefer zero tolerance on vehicle maintenance OOS ───────
    # Determine equipment from explicit param or carrier data
    equip = equipment_types
    if equip is None:
        equip_str = carrier.get("Equipment_Types", "")
        equip = [e.strip().upper() for e in equip_str.split(",") if e.strip()]

    is_reefer = any("REEFER" in e.upper() for e in (equip or []))
    if is_reefer:
        veh_oos_insp = _safe_int(carrier.get("Vehicle_OOS_Insp", 0))
        if veh_oos_insp > 0:
            reason = (
                f"REJECT: Reefer carrier has {veh_oos_insp} vehicle "
                f"maintenance OOS inspection(s) — zero tolerance for reefer"
            )
            logger.info("Strict vet REJECT %s: %s", name, reason)
            return False, reason

    logger.debug("Strict vet PASSED for %s", name)
    return True, ""


def clear_cache() -> None:
    """Clear the FMCSA response cache."""
    _cache.clear()
