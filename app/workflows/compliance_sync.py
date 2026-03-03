"""
Workflow: Compliance Sync

Verify carrier authority + insurance via CarrierOK API and update Carrier_Master.
Runs for:
  - All carriers assigned to active loads.
  - Optionally, all active carriers periodically.
"""
from __future__ import annotations

import logging
from datetime import date

from app.sheets import (
    get_all_carriers, get_loads_by_status, get_carrier,
    update_carrier_fields,
)
from app.carrierok import verify_carrier, derive_compliance_status

logger = logging.getLogger("brokerops.workflows.compliance_sync")


def sync_carrier(mc_number: str) -> bool:
    """
    Verify a single carrier and update Carrier_Master.
    Returns True if updated successfully.
    """
    carrier = get_carrier(mc_number)
    if not carrier:
        logger.warning("Carrier MC=%s not found.", mc_number)
        return False

    dot = carrier.get("DOT_Number", "")
    result = verify_carrier(mc_number, dot)
    if result is None:
        logger.error("CarrierOK API call failed for MC=%s", mc_number)
        return False

    w9 = carrier.get("W9_On_File", "").upper() in ("TRUE", "YES", "1")
    compliance = derive_compliance_status(
        authority_status=result["authority_status"],
        insurance_expiration=result["insurance_expiration"],
        auto_liability=result["auto_liability_coverage"],
        cargo=result["cargo_coverage"],
        w9_on_file=w9,
    )

    updates = {
        "Authority_Status": result["authority_status"],
        "Insurance_Expiration": result["insurance_expiration"],
        "Auto_Liability_Coverage": str(result["auto_liability_coverage"]),
        "Cargo_Coverage": str(result["cargo_coverage"]),
        "Authority_Verified_Date": date.today().isoformat(),
        "Authority_Source": "CARRIEROK_API",
        "Compliance_Status": compliance,
    }

    update_carrier_fields(mc_number, updates)
    logger.info(
        "Carrier MC=%s synced: authority=%s, compliance=%s",
        mc_number, result["authority_status"], compliance,
    )
    return True


def run_for_active_loads() -> list[str]:
    """Sync compliance for carriers assigned to any active load."""
    synced: list[str] = []
    active_statuses = [
        "NEW", "RFQ_SENT", "QUOTES_RECEIVED", "CARRIER_SELECTED",
        "ONBOARDING_REQUIRED", "DOCS_PENDING", "READY_FOR_APPROVAL",
    ]

    checked_mcs: set[str] = set()
    for status in active_statuses:
        for load in get_loads_by_status(status):
            mc = load.get("Assigned_Carrier_MC", "")
            if mc and mc not in checked_mcs:
                checked_mcs.add(mc)
                if sync_carrier(mc):
                    synced.append(mc)

    return synced
