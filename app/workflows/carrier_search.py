"""
BrokerOps AI – Carrier Search & Enrichment Workflow.

Searches FMCSA Census API for carriers, scores them, inserts/updates in
Carrier_Master, and runs email enrichment for carriers missing contact info.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

from app.fmcsa import search_carriers as fmcsa_search, get_carrier_details, score_carrier
from app.email_enrichment import enrich_carrier_email
from app.sheets import (
    get_carrier,
    insert_carrier,
    update_carrier_fields,
    CARRIER_MASTER_COLUMNS,
)

logger = logging.getLogger("brokerops.carrier_search")


def search_and_score(
    city: str,
    state: str,
    radius_miles: int = 50,
    equipment_type: str | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Search FMCSA for carriers, score, store in Carrier_Master, enrich emails.

    Returns top N carriers sorted by score (highest first).
    """
    # 1. Query FMCSA
    raw_carriers = fmcsa_search(state=state, city=city, equipment_type=equipment_type, limit=limit * 3)

    if not raw_carriers:
        logger.warning("No FMCSA results for %s, %s", city, state)
        return []

    # 2. Fetch full details for each carrier (name search returns limited data)
    detailed_carriers = []
    for c in raw_carriers:
        dot = c.get("DOT_Number", "")
        if dot:
            details = get_carrier_details(dot)
            if details:
                detailed_carriers.append(details)
            else:
                detailed_carriers.append(c)
        else:
            detailed_carriers.append(c)

    logger.info("Fetched details for %d carriers", len(detailed_carriers))

    # 3. Score and filter out disqualified; equipment match is a bonus, not a filter
    scored: list[tuple[int, dict]] = []
    eq_upper = equipment_type.upper() if equipment_type else ""
    for c in detailed_carriers:
        s = score_carrier(c)
        if s < 0:
            logger.info("Disqualified: %s (DOT %s) - score %d", c.get("Legal_Name"), c.get("DOT_Number"), s)
            continue
        # Equipment match bonus (+15) instead of hard filter
        if eq_upper and eq_upper in (c.get("Equipment_Types", "") or "").upper():
            s += 15
            c["Equipment_Match"] = True
        c["Carrier_Score"] = s
        scored.append((s, c))

    # Sort by score descending
    scored.sort(key=lambda x: x[0], reverse=True)
    top = [c for _, c in scored[:limit]]

    # 4. Insert/update in Carrier_Master + enrich
    results = []
    for carrier in top:
        stored = _upsert_carrier(carrier)
        results.append(stored)

    logger.info(
        "Carrier search: %s %s → %d results, %d qualified, %d stored",
        city, state, len(raw_carriers), len(scored), len(results),
    )
    return results


def search_by_lane(
    origin_city: str,
    origin_state: str,
    dest_city: str,
    dest_state: str,
    equipment_type: str | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Search carriers near both origin and destination, prefer those in both.

    Carriers appearing in both searches get a lane-match bonus.
    """
    origin_carriers = search_and_score(
        origin_city, origin_state, equipment_type=equipment_type, limit=limit * 2
    )
    dest_carriers = search_and_score(
        dest_city, dest_state, equipment_type=equipment_type, limit=limit * 2
    )

    # Merge by MC/DOT and flag lane matches
    by_key: dict[str, dict] = {}
    for c in origin_carriers:
        key = c.get("MC_Number") or c.get("DOT_Number", "")
        by_key[key] = c
        c["_lane_match"] = False

    for c in dest_carriers:
        key = c.get("MC_Number") or c.get("DOT_Number", "")
        if key in by_key:
            by_key[key]["_lane_match"] = True
        else:
            c["_lane_match"] = False
            by_key[key] = c

    # Sort: lane matches first, then by score
    all_carriers = list(by_key.values())
    all_carriers.sort(
        key=lambda c: (c.get("_lane_match", False), c.get("Carrier_Score", 0)),
        reverse=True,
    )

    return all_carriers[:limit]


def _upsert_carrier(carrier: dict) -> dict:
    """Insert or update a carrier in Carrier_Master, then run email enrichment."""
    mc = carrier.get("MC_Number", "")
    dot = carrier.get("DOT_Number", "")
    today = date.today().isoformat()

    # Check if carrier already exists
    existing = get_carrier(mc) if mc else None

    fields = {
        "MC_Number": mc,
        "DOT_Number": dot,
        "Legal_Name": carrier.get("Legal_Name", ""),
        "DBA_Name": carrier.get("DBA_Name", ""),
        "Primary_Phone": carrier.get("Contact_Phone", ""),
        "Equipment_Type": carrier.get("Equipment_Types", ""),
        "Authority_Status": carrier.get("Authority_Status", ""),
        "Authority_Verified_Date": today,
        "Authority_Source": "FMCSA",
        "On_Time_Score": str(carrier.get("Carrier_Score", 0)),
        "Active": "TRUE",
        "Last_Updated": today,
    }

    if existing:
        # Update existing carrier with fresh FMCSA data
        update_carrier_fields(mc, {
            "Authority_Status": fields["Authority_Status"],
            "Authority_Verified_Date": today,
            "Equipment_Type": fields["Equipment_Type"],
            "On_Time_Score": fields["On_Time_Score"],
            "Last_Updated": today,
        })
        logger.info("Updated existing carrier MC#%s", mc)
        # Merge existing data
        for k, v in existing.items():
            if k not in fields or not fields[k]:
                fields[k] = v
    else:
        # New carrier — insert
        fields["Created_Date"] = today
        fields["Onboarding_Status"] = "NEW"
        insert_carrier(fields)
        logger.info("Inserted new carrier MC#%s (%s)", mc, fields["Legal_Name"])

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

            if mc:
                update_carrier_fields(mc, updates)
            fields.update(updates)

            logger.info(
                "Enriched MC#%s: email=%s source=%s",
                mc, email or "PHONE_ONLY", source,
            )
        except Exception as exc:
            logger.warning("Enrichment failed for MC#%s: %s", mc, exc)

    return fields
