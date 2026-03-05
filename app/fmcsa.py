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

    Uses multiple single-letter name searches to get broad coverage since the
    FMCSA name endpoint requires a name path parameter.

    Returns raw carrier dicts from the API (normalized).
    """
    # FMCSA /carriers/name/{name} requires a name in the URL path.
    # We search several common starting letters to get broad coverage.
    search_letters = ["a", "c", "d", "e", "f", "g", "j", "l", "m", "n", "p", "r", "s", "t"]
    seen_dots: set[str] = set()
    all_carriers: list[dict] = []

    per_letter = max(limit // 3, 10)

    for letter in search_letters:
        if len(all_carriers) >= limit:
            break

        url = f"{_BASE_URL}/name/{letter}"
        params: dict[str, Any] = {"stateAbbrev": state.upper(), "size": str(per_letter)}
        if city:
            params["city"] = city.upper()

        try:
            data = _cached_get(url, params)
        except Exception as exc:
            logger.debug("FMCSA search letter '%s' failed: %s", letter, exc)
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

    logger.info("FMCSA search: found %d carriers in %s %s", len(all_carriers), city or "", state)
    return all_carriers


def get_carrier_details(dot_number: str) -> Optional[dict]:
    """Fetch full carrier profile from Census API by DOT number."""
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
    return _normalize_carrier(carrier_data) if carrier_data else None


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
    """Detect equipment types from FMCSA cargo/operation codes."""
    types: set[str] = set()
    cargo_carried = str(raw.get("cargoCarried", "")).upper()
    classification = str(raw.get("operationClassification", "")).upper()

    # General freight → dry van
    if any(k in cargo_carried for k in ["GENERAL FREIGHT", "GEN FREIGHT"]):
        types.add("DRY_VAN")
    # Refrigerated
    if any(k in cargo_carried for k in ["REFRIGERATED", "TEMP CONTROLLED"]):
        types.add("REEFER")
    # Flatbed indicators
    if any(k in cargo_carried for k in ["METAL", "BUILDING MATERIAL", "MACHINERY",
                                         "LUMBER", "LARGE OBJECTS", "CONSTRUCTION"]):
        types.add("FLATBED")
    # Oversize
    if "OVERSIZE" in cargo_carried or "OVERWEIGHT" in cargo_carried:
        types.add("FLATBED")

    # If nothing detected, assume dry van for authorized carriers
    if not types:
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
    """
    score = 0

    # ── Hard disqualifiers ──────────────────────────────────────
    if carrier.get("Authority_Status") != "ACTIVE":
        return -1
    if carrier.get("OOS_Active"):
        return -1
    if carrier.get("Safety_Rating") == "UNSATISFACTORY":
        return -1

    liability = carrier.get("Insurance_Liability", 0)
    cargo = carrier.get("Insurance_Cargo", 0)
    if liability < 1_000_000:
        return -1
    if cargo < 100_000:
        return -1

    # ── Operating Authority (25 pts) ────────────────────────────
    auth_date_str = carrier.get("Authority_Date", "")
    auth_age_months = _authority_age_months(auth_date_str)
    if auth_age_months >= 36:
        score += 25
    elif auth_age_months >= 18:
        score += 15
    # < 18 months = 0 pts

    # ── Insurance — Liability (20 pts) ──────────────────────────
    if liability >= 2_000_000:
        score += 20
    elif liability >= 1_000_000:
        score += 15

    # ── Insurance — Cargo (10 pts) ──────────────────────────────
    if cargo >= 250_000:
        score += 10
    elif cargo >= 100_000:
        score += 7

    # ── Safety Rating (20 pts) ──────────────────────────────────
    safety = carrier.get("Safety_Rating", "NONE")
    if safety == "SATISFACTORY":
        score += 20
    elif safety == "CONDITIONAL":
        score += 10
    elif safety == "NONE":
        score += 12  # no rating = neutral

    veh_oos = carrier.get("Vehicle_OOS_Rate", 0)
    drv_oos = carrier.get("Driver_OOS_Rate", 0)
    if veh_oos > 30:
        score -= 10
    if drv_oos > 20:
        score -= 5

    # ── Fleet Size (15 pts) ─────────────────────────────────────
    units = carrier.get("Power_Units", 0)
    if units >= 51:
        score += 15
    elif units >= 21:
        score += 13
    elif units >= 6:
        score += 10
    elif units >= 1:
        score += 5

    # ── Complaint History (10 pts) ──────────────────────────────
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


def clear_cache() -> None:
    """Clear the FMCSA response cache."""
    _cache.clear()
