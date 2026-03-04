"""
BrokerOps AI – Equipment Intelligence Module

Provides trailer specifications, equipment recommendation logic,
and load-to-trailer matching for optimal carrier sourcing.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("brokerops.equipment")

# ── Industry Standard Trailer Specifications ─────────────────────────────────

TRAILER_SPECS = {
    "DRY_VAN_53": {
        "type": "DRY_VAN", "subtype": "53ft",
        "interior_length_ft": 52.5, "interior_width_in": 99, "interior_height_in": 108,
        "door_width_in": 98, "door_height_in": 110,
        "cubic_ft": 4054, "pallets_48x40": 26,
        "max_payload_lbs": 45000,
        "tare_weight_lbs": 15000,
        "enclosed": True, "climate_control": False,
        "best_for": ["general merchandise", "packaged goods", "electronics", "consumer products"],
    },
    "DRY_VAN_48": {
        "type": "DRY_VAN", "subtype": "48ft",
        "interior_length_ft": 47.5, "interior_width_in": 98, "interior_height_in": 108,
        "door_width_in": 98, "door_height_in": 110,
        "cubic_ft": 3700, "pallets_48x40": 24,
        "max_payload_lbs": 45000,
        "tare_weight_lbs": 14000,
        "enclosed": True, "climate_control": False,
        "best_for": ["regional hauling", "urban deliveries"],
    },
    "FLATBED_53": {
        "type": "FLATBED", "subtype": "53ft",
        "deck_length_ft": 53, "deck_width_in": 102, "deck_height_in": 60,
        "max_freight_height_in": 102,  # 8.5 ft above deck
        "cubic_ft": None, "pallets_48x40": 26,
        "max_payload_lbs": 48000,
        "tare_weight_lbs": 14000,
        "enclosed": False, "climate_control": False,
        "best_for": ["steel", "lumber", "machinery", "equipment", "pipes", "beams", "construction materials"],
    },
    "FLATBED_48": {
        "type": "FLATBED", "subtype": "48ft",
        "deck_length_ft": 48, "deck_width_in": 102, "deck_height_in": 60,
        "max_freight_height_in": 102,
        "cubic_ft": None, "pallets_48x40": 20,
        "max_payload_lbs": 48000,
        "tare_weight_lbs": 13000,
        "enclosed": False, "climate_control": False,
        "best_for": ["steel", "lumber", "machinery", "equipment"],
    },
    "STEP_DECK": {
        "type": "FLATBED", "subtype": "step_deck",
        "well_length_ft": 29.5, "upper_deck_ft": 11.75,
        "deck_width_in": 102, "upper_height_in": 60, "lower_height_in": 42,
        "max_freight_height_lower_in": 120,  # 10 ft clearance on lower deck
        "cubic_ft": None, "pallets_48x40": 26,
        "max_payload_lbs": 45000,
        "tare_weight_lbs": 15000,
        "enclosed": False, "climate_control": False,
        "best_for": ["over-height loads", "heavy machinery", "construction equipment", "tall items"],
    },
    "REEFER_53": {
        "type": "REEFER", "subtype": "53ft",
        "interior_length_ft": 51.5, "interior_width_in": 97, "interior_height_in": 104,
        "door_width_in": 96, "door_height_in": 104,
        "cubic_ft": 3000, "pallets_48x40": 26,
        "max_payload_lbs": 43500,
        "tare_weight_lbs": 16500,  # heavier due to refrigeration unit + insulation
        "enclosed": True, "climate_control": True,
        "best_for": ["frozen food", "refrigerated cargo", "perishables", "dairy", "meat",
                      "produce", "seafood", "pharmaceuticals", "ice cream"],
    },
    "REEFER_48": {
        "type": "REEFER", "subtype": "48ft",
        "interior_length_ft": 46.5, "interior_width_in": 97, "interior_height_in": 104,
        "cubic_ft": 2500, "pallets_48x40": 24,
        "max_payload_lbs": 44000,
        "tare_weight_lbs": 16000,
        "enclosed": True, "climate_control": True,
        "best_for": ["refrigerated regional hauls", "perishable goods"],
    },
    "CONESTOGA": {
        "type": "CONESTOGA", "subtype": "53ft",
        "deck_length_ft": 53, "deck_width_in": 100, "interior_height_in": 96,
        "cubic_ft": None, "pallets_48x40": 26,
        "max_payload_lbs": 44000,
        "tare_weight_lbs": 16000,
        "enclosed": False, "climate_control": False,
        "sliding_tarp": True,
        "best_for": ["weather-sensitive flatbed loads", "electronics", "fabrics",
                      "items needing partial protection"],
    },
    "BOX_TRUCK_26": {
        "type": "BOX_TRUCK", "subtype": "26ft",
        "interior_length_ft": 24, "interior_width_in": 96, "interior_height_in": 96,
        "door_width_in": 93, "door_height_in": 82,
        "cubic_ft": 1800, "pallets_48x40": 4,
        "max_payload_lbs": 8000,  # with liftgate
        "tare_weight_lbs": 18000,
        "gvwr_lbs": 26000,
        "enclosed": True, "climate_control": False,
        "best_for": ["local deliveries", "furniture", "appliances", "residential delivery"],
    },
    "BOX_TRUCK_16": {
        "type": "BOX_TRUCK", "subtype": "16ft",
        "interior_length_ft": 16, "interior_width_in": 92, "interior_height_in": 96,
        "cubic_ft": 960, "pallets_48x40": 2,
        "max_payload_lbs": 7500,
        "tare_weight_lbs": 12500,
        "gvwr_lbs": 20000,
        "enclosed": True, "climate_control": False,
        "best_for": ["small deliveries", "last-mile", "urban delivery"],
    },
    "SPRINTER": {
        "type": "SPRINTER", "subtype": "cargo_van",
        "interior_length_ft": 14.5, "interior_width_in": 80, "interior_height_in": 79,
        "cubic_ft": 533, "pallets_48x40": 0,
        "max_payload_lbs": 5000,
        "tare_weight_lbs": 7000,
        "enclosed": True, "climate_control": False,
        "best_for": ["hot shot", "expedited LTL", "last-mile", "small urgent loads"],
    },
    "HOTSHOT": {
        "type": "HOTSHOT", "subtype": "40ft_gooseneck",
        "deck_length_ft": 40, "deck_width_in": 102, "deck_height_in": 34,
        "cubic_ft": None, "pallets_48x40": 0,
        "max_payload_lbs": 16500,  # non-CDL typical
        "tare_weight_lbs": 9500,
        "gvwr_lbs": 26000,
        "enclosed": False, "climate_control": False,
        "best_for": ["equipment hauling", "car transport", "construction loads", "expedited flatbed"],
    },
}

# Federal weight limits
FEDERAL_GVW_LIMIT = 80000  # lbs
TEAM_DRIVER_THRESHOLD_MILES = 1000  # generally recommended for 1000+ mile loads


# ── Commodity → Equipment Inference ──────────────────────────────────────────

# Commodities that require specific equipment
COMMODITY_EQUIPMENT_MAP = {
    # Reefer commodities
    "frozen": "REEFER", "refrigerated": "REEFER", "perishable": "REEFER",
    "ice cream": "REEFER", "seafood": "REEFER", "fish": "REEFER",
    "shrimp": "REEFER", "lobster": "REEFER", "crab": "REEFER",
    "meat": "REEFER", "poultry": "REEFER", "chicken": "REEFER", "pork": "REEFER", "beef": "REEFER",
    "produce": "REEFER", "fruit": "REEFER", "vegetables": "REEFER",
    "dairy": "REEFER", "milk": "REEFER", "cheese": "REEFER", "yogurt": "REEFER",
    "pharmaceutical": "REEFER", "vaccine": "REEFER",
    # Flatbed commodities
    "steel": "FLATBED", "rebar": "FLATBED", "pipe": "FLATBED", "pipes": "FLATBED",
    "lumber": "FLATBED", "timber": "FLATBED", "plywood": "FLATBED",
    "machinery": "FLATBED", "equipment": "FLATBED", "heavy equipment": "FLATBED",
    "beam": "FLATBED", "beams": "FLATBED", "i-beam": "FLATBED",
    "coil": "FLATBED", "steel coil": "FLATBED",
    "concrete": "FLATBED", "cinder block": "FLATBED",
    "roofing": "FLATBED", "shingles": "FLATBED",
    "solar panel": "FLATBED", "solar panels": "FLATBED",
}


# ── Equipment Recommendation Engine ──────────────────────────────────────────

def recommend_equipment(load: dict[str, Any]) -> dict[str, Any]:
    """
    Analyze a load and recommend the best equipment option(s).

    Returns:
        {
            "recommended": "REEFER_53",
            "alternatives": ["REEFER_48"],
            "warnings": ["Weight exceeds 43,500 lbs — verify with packing slip"],
            "requires_verification": True/False,
            "verification_reasons": ["Weight near capacity", "No dimensions provided"],
            "special_requirements_inferred": ["temperature monitoring", "tarp"],
            "cost_tier": "standard" | "premium" | "economy",
            "notes": "..."
        }
    """
    weight = _parse_weight(load.get("Weight_Lbs", ""))
    commodity = (load.get("Commodity", "") or "").lower()
    equipment_type = (load.get("Equipment_Type", "") or "").upper()
    dimensions = (load.get("Dimensions", "") or "").lower()
    special_req = (load.get("Special_Requirements", "") or "").lower()
    temp_control = (load.get("Temp_Control_Required", "") or "").upper() == "TRUE"

    warnings = []
    verification_reasons = []
    inferred_requirements = []
    alternatives = []

    # Step 1: Determine base equipment type from commodity if not specified
    if not equipment_type or equipment_type == "DRY_VAN":
        for keyword, equip in COMMODITY_EQUIPMENT_MAP.items():
            if keyword in commodity:
                if equip != equipment_type:
                    equipment_type = equip
                    logger.info("Inferred equipment %s from commodity '%s'", equip, commodity)
                break

    # Step 2: Force reefer for temperature-controlled loads
    if temp_control and equipment_type != "REEFER":
        warnings.append(f"Temp control required but equipment is {equipment_type} — switching to REEFER")
        equipment_type = "REEFER"

    # Step 3: Find best trailer size based on weight and dimensions
    recommended = _find_best_trailer(equipment_type, weight, dimensions, commodity)

    # Step 4: Check weight against trailer limits
    spec = TRAILER_SPECS.get(recommended)
    if spec and weight:
        max_payload = spec["max_payload_lbs"]
        if weight > max_payload:
            warnings.append(
                f"Weight {weight:,} lbs exceeds {recommended} max payload of {max_payload:,} lbs — "
                f"may need overweight permit or split shipment"
            )
            verification_reasons.append("Weight exceeds trailer capacity")
        elif weight > max_payload * 0.9:
            warnings.append(
                f"Weight {weight:,} lbs is near {recommended} max payload of {max_payload:,} lbs — "
                f"verify exact weight with packing slip"
            )
            verification_reasons.append("Weight near capacity — request packing slip")

    # Step 5: Check if we need weight/dimension verification
    if not weight:
        verification_reasons.append("No weight provided — request packing slip")
    if not dimensions and equipment_type in ("FLATBED", "CONESTOGA", "HOTSHOT"):
        verification_reasons.append("No dimensions for open-deck load — request packing slip")

    # Step 6: Infer special requirements
    if equipment_type == "REEFER":
        if "temperature monitoring" not in special_req:
            inferred_requirements.append("temperature monitoring")
    if equipment_type == "FLATBED":
        if "tarp" not in special_req and _needs_tarp(commodity):
            inferred_requirements.append("tarp required")
        if "straps" not in special_req and "chains" not in special_req:
            inferred_requirements.append("load securement (straps/chains)")
    if equipment_type in ("BOX_TRUCK", "SPRINTER"):
        if "lift gate" not in special_req and "liftgate" not in special_req:
            inferred_requirements.append("lift gate (verify dock availability)")

    # Step 7: Suggest alternatives for cost optimization
    alternatives = _find_alternatives(equipment_type, weight, commodity, recommended)

    # Step 8: Determine cost tier
    cost_tier = _cost_tier(equipment_type)

    # Step 9: Build notes
    notes = _build_notes(equipment_type, weight, commodity, spec)

    result = {
        "recommended": recommended,
        "recommended_type": equipment_type,
        "alternatives": alternatives,
        "warnings": warnings,
        "requires_verification": len(verification_reasons) > 0,
        "verification_reasons": verification_reasons,
        "special_requirements_inferred": inferred_requirements,
        "cost_tier": cost_tier,
        "notes": notes,
    }

    if spec:
        result["trailer_specs"] = {
            "max_payload_lbs": spec["max_payload_lbs"],
            "cubic_ft": spec.get("cubic_ft"),
            "pallets": spec.get("pallets_48x40"),
        }

    logger.info("Equipment recommendation for %s/%s lbs: %s (tier: %s, verify: %s)",
                commodity or "unknown", weight or "unknown", recommended,
                cost_tier, result["requires_verification"])

    return result


# ── Internal Helpers ─────────────────────────────────────────────────────────

def _parse_weight(weight_str: str) -> int:
    """Parse weight string to integer."""
    if not weight_str:
        return 0
    try:
        cleaned = weight_str.replace(",", "").replace(" ", "").lower()
        cleaned = cleaned.replace("lbs", "").replace("lb", "").replace("pounds", "")
        if "k" in cleaned:
            return int(float(cleaned.replace("k", "")) * 1000)
        return int(float(cleaned))
    except (ValueError, TypeError):
        return 0


def _find_best_trailer(equipment_type: str, weight: int, dimensions: str, commodity: str) -> str:
    """Select the optimal trailer spec key for the load."""
    if equipment_type == "REEFER":
        if weight and weight <= 43500:
            return "REEFER_53"
        elif weight and weight <= 44000:
            return "REEFER_48"
        return "REEFER_53"

    if equipment_type == "FLATBED":
        # Check if step deck is needed (over-height items)
        if "tall" in dimensions or "over-height" in dimensions or "overheight" in commodity:
            return "STEP_DECK"
        if weight and weight <= 45000:
            return "FLATBED_53"
        return "FLATBED_53"

    if equipment_type == "CONESTOGA":
        return "CONESTOGA"

    if equipment_type == "BOX_TRUCK":
        if weight and weight <= 7500:
            return "BOX_TRUCK_16"
        return "BOX_TRUCK_26"

    if equipment_type == "SPRINTER":
        return "SPRINTER"

    if equipment_type == "HOTSHOT":
        return "HOTSHOT"

    # Default: dry van
    if weight and weight <= 5000:
        return "SPRINTER"  # small loads → sprinter is cheapest
    return "DRY_VAN_53"


def _needs_tarp(commodity: str) -> bool:
    """Check if a flatbed commodity typically needs tarping."""
    tarp_commodities = [
        "lumber", "plywood", "rebar", "steel", "paper", "cardboard",
        "fabric", "electronics", "furniture", "solar",
    ]
    return any(t in commodity for t in tarp_commodities)


def _find_alternatives(equipment_type: str, weight: int, commodity: str, recommended: str) -> list[str]:
    """Find alternative trailer options for cost optimization."""
    alts = []
    if recommended == "DRY_VAN_53" and weight and weight <= 10000:
        alts.append("BOX_TRUCK_26")  # cheaper for light loads
    if recommended == "DRY_VAN_53":
        alts.append("DRY_VAN_48")
    if recommended == "FLATBED_53":
        alts.append("FLATBED_48")
        if _needs_tarp(commodity):
            alts.append("CONESTOGA")  # no tarping labor needed
    if recommended == "REEFER_53":
        alts.append("REEFER_48")
    if recommended == "FLATBED_53" and weight and weight <= 16500:
        alts.append("HOTSHOT")  # significantly cheaper for lighter flatbed loads
    # Remove the recommended from alternatives
    return [a for a in alts if a != recommended]


def _cost_tier(equipment_type: str) -> str:
    """Classify equipment into cost tiers for rate estimation."""
    if equipment_type in ("REEFER", "CONESTOGA"):
        return "premium"
    if equipment_type in ("FLATBED", "STEP_DECK"):
        return "standard_plus"
    if equipment_type in ("SPRINTER", "HOTSHOT"):
        return "economy"
    if equipment_type == "BOX_TRUCK":
        return "economy"
    return "standard"


def _build_notes(equipment_type: str, weight: int, commodity: str, spec: dict | None) -> str:
    """Build human-readable notes about the equipment recommendation."""
    notes = []

    if spec:
        if spec.get("max_payload_lbs") and weight:
            remaining = spec["max_payload_lbs"] - weight
            notes.append(f"Payload capacity remaining: {remaining:,} lbs")
        if spec.get("pallets_48x40"):
            notes.append(f"Fits up to {spec['pallets_48x40']} standard pallets (48x40)")

    if equipment_type == "REEFER":
        notes.append("Continuous temp monitoring required")
        notes.append("Verify required temp setting with shipper")
    elif equipment_type == "FLATBED":
        notes.append("Load securement per FMCSA §393.100-136 required")
        if _needs_tarp(commodity):
            notes.append("Tarping required — confirm tarp type (lumber/steel/smoke)")
    elif equipment_type == "BOX_TRUCK":
        notes.append("Confirm dock availability at pickup/delivery — liftgate adds cost")

    if weight and weight > 44000:
        notes.append("Near federal weight limit — weigh at certified scale before departure")

    return "; ".join(notes)
