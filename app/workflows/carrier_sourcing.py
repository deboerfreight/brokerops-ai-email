"""
Workflow: Carrier Sourcing & RFQ

For each load with Load_Status=NEW (or RFQ expansion candidates):
  1. Build lane key = Origin_State-Destination_State.
  2. Filter & rank eligible carriers.
  3. Send RFQ emails to top N carriers.
  4. Update Load_Status=RFQ_SENT, RFQ_Count.
"""
from __future__ import annotations

import logging
from datetime import date, datetime

from app.config import get_settings
from app.gmail import send_email, add_label
from app.sheets import (
    get_all_carriers, get_loads_by_status,
    update_load_fields, is_carrier_dispatch_eligible,
)

logger = logging.getLogger("brokerops.workflows.carrier_sourcing")


def _rank_carriers(carriers: list[dict], lane_key: str) -> list[dict]:
    """
    Rank carriers for a given lane.

    Priority:
      1. Preferred_Lanes contains the lane key
      2. Higher On_Time_Score
      3. Lower Claims_Count
      4. More recent Last_Load_Date
    """
    def sort_key(c: dict):
        lanes = c.get("Preferred_Lanes", "")
        has_lane = 1 if lane_key in lanes else 0
        ot_score = int(c.get("On_Time_Score", "0") or "0")
        claims = int(c.get("Claims_Count", "999") or "999")
        last_load = c.get("Last_Load_Date", "1970-01-01") or "1970-01-01"
        return (-has_lane, -ot_score, claims, last_load)

    return sorted(carriers, key=sort_key)


def _build_rfq_body(load: dict) -> str:
    settings_data = {}  # Broker settings loaded lazily below
    from app.sheets import get_broker_settings
    try:
        settings_data = get_broker_settings()
    except Exception:
        pass

    broker_name = settings_data.get("Broker_Company_Name", "BrokerOps")
    broker_phone = settings_data.get("Broker_Company_Phone", "")
    broker_email_contact = settings_data.get("Broker_Company_Email", "")

    return f"""Hello,

We have a load available and would like to request a quote:

Load ID: {load['Load_ID']}
Origin: {load['Origin_City']}, {load['Origin_State']} {load.get('Origin_Zip', '')}
Destination: {load['Destination_City']}, {load['Destination_State']} {load.get('Destination_Zip', '')}
Pickup Date: {load['Pickup_Date']}
Pickup Window: {load.get('Pickup_Time_Window', 'OPEN')}
Delivery Date: {load.get('Delivery_Date', '')}
Delivery Window: {load.get('Delivery_Time_Window', 'OPEN')}
Equipment: {load['Equipment_Type']}
Commodity: {load.get('Commodity', '')}
Weight: {load.get('Weight_Lbs', '')} lbs
Temp Control Required: {load.get('Temp_Control_Required', 'FALSE')}
Hazmat: {load.get('Hazmat', 'FALSE')}

Please reply with the following information:
Rate: $
Availability: Yes/No
Transit Time:
Any restrictions:

Thank you,
{broker_name}
{broker_phone}
{broker_email_contact}
"""


def _build_rfq_subject(load: dict) -> str:
    pickup_date = load.get("Pickup_Date", "")
    # Format as MM/DD if possible
    try:
        dt = datetime.strptime(pickup_date, "%Y-%m-%d")
        date_str = dt.strftime("%m/%d")
    except (ValueError, TypeError):
        date_str = pickup_date

    return (
        f"RFQ | {load['Load_ID']} | "
        f"{load['Origin_City']},{load['Origin_State']} → "
        f"{load['Destination_City']},{load['Destination_State']} | "
        f"{date_str}"
    )


def source_carriers_for_load(load: dict, offset: int = 0) -> int:
    """
    Find and email top N carriers for a load.

    Args:
        load: Load dict from Load_Master.
        offset: Skip the first `offset` carriers (for expansion rounds).

    Returns:
        Number of RFQs sent.
    """
    settings = get_settings()
    batch_size = settings.RFQ_BATCH_SIZE

    lane_key = f"{load['Origin_State']}-{load['Destination_State']}"
    equip = load["Equipment_Type"]

    # Filter eligible carriers
    all_carriers = get_all_carriers()
    eligible = [
        c for c in all_carriers
        if c.get("Equipment_Type") == equip
        and is_carrier_dispatch_eligible(c)
    ]

    # Also include carriers that need onboarding (W9 missing but otherwise eligible)
    onboarding_candidates = [
        c for c in all_carriers
        if c.get("Equipment_Type") == equip
        and c.get("Active", "").upper() in ("TRUE", "YES", "1")
        and c.get("Authority_Status") == "ACTIVE"
        and c.get("Compliance_Status") == "CLEAR"
        and c.get("W9_On_File", "").upper() not in ("TRUE", "YES", "1")
    ]

    ranked = _rank_carriers(eligible + onboarding_candidates, lane_key)
    batch = ranked[offset: offset + batch_size]

    if not batch:
        logger.info("No more carriers to RFQ for load %s (offset=%d)", load["Load_ID"], offset)
        return 0

    subject = _build_rfq_subject(load)
    body = _build_rfq_body(load)
    sent_count = 0

    for carrier in batch:
        email = carrier.get("Primary_Email", "").strip()
        if not email:
            logger.warning("Carrier %s has no email, skipping.", carrier.get("MC_Number"))
            continue

        try:
            result = send_email(to=email, subject=subject, body_text=body)
            add_label(result["id"], "OPS/RFQ_SENT")
            sent_count += 1
            logger.info(
                "Sent RFQ to %s (%s) for load %s",
                carrier.get("Legal_Name"), email, load["Load_ID"],
            )
        except Exception:
            logger.exception("Failed to send RFQ to %s", email)

    return sent_count


def run() -> list[str]:
    """Process all NEW loads: send initial RFQ batch."""
    processed: list[str] = []
    new_loads = get_loads_by_status("NEW")
    logger.info("Found %d NEW load(s) for carrier sourcing.", len(new_loads))

    for load in new_loads:
        load_id = load["Load_ID"]
        try:
            count = source_carriers_for_load(load, offset=0)
            if count > 0:
                update_load_fields(load_id, {
                    "Load_Status": "RFQ_SENT",
                    "RFQ_Count": str(count),
                    "Last_Updated": date.today().isoformat(),
                })
                processed.append(load_id)
                logger.info("Load %s: sent %d RFQs.", load_id, count)
            else:
                logger.warning("Load %s: no eligible carriers found.", load_id)
        except Exception:
            logger.exception("Carrier sourcing failed for load %s", load_id)

    return processed


def run_expansion() -> list[str]:
    """
    For loads in RFQ_SENT status that have been waiting >= 2 hours,
    expand to the next batch of carriers.
    """
    settings = get_settings()
    expanded: list[str] = []
    rfq_loads = get_loads_by_status("RFQ_SENT")

    for load in rfq_loads:
        load_id = load["Load_ID"]
        last_updated = load.get("Last_Updated", "")
        if not last_updated:
            continue
        try:
            updated_dt = datetime.fromisoformat(last_updated)
            elapsed = (datetime.utcnow() - updated_dt).total_seconds()
        except (ValueError, TypeError):
            continue

        if elapsed < settings.RFQ_EXPANSION_DELAY_SECONDS:
            continue

        current_count = int(load.get("RFQ_Count", "0") or "0")
        new_count = source_carriers_for_load(load, offset=current_count)

        if new_count > 0:
            update_load_fields(load_id, {
                "RFQ_Count": str(current_count + new_count),
                "Last_Updated": datetime.utcnow().isoformat(),
            })
            expanded.append(load_id)
            logger.info("Load %s: expanded RFQ by %d (total %d).", load_id, new_count, current_count + new_count)

    return expanded
