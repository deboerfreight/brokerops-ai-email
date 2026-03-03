"""
BrokerOps AI – Google Sheets helpers for Carrier_Master, Load_Master, and
the idempotency / processed-message store.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Optional

from app.config import get_settings
from app.google_auth import get_sheets_service

logger = logging.getLogger("brokerops.sheets")

# ── Generic helpers ──────────────────────────────────────────────────────────

def _svc():
    return get_sheets_service().spreadsheets()


def read_range(sheet_id: str, range_: str) -> list[list[str]]:
    resp = _svc().values().get(spreadsheetId=sheet_id, range=range_).execute()
    return resp.get("values", [])


def write_range(sheet_id: str, range_: str, values: list[list], value_input: str = "USER_ENTERED"):
    _svc().values().update(
        spreadsheetId=sheet_id,
        range=range_,
        valueInputOption=value_input,
        body={"values": values},
    ).execute()


def append_row(sheet_id: str, range_: str, row: list, value_input: str = "USER_ENTERED"):
    _svc().values().append(
        spreadsheetId=sheet_id,
        range=range_,
        valueInputOption=value_input,
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()


# ── Load_Master Settings tab ────────────────────────────────────────────────

def get_next_load_id() -> str:
    """Read current year + next number from Settings tab, increment, return Load_ID."""
    settings = get_settings()
    sid = settings.LOAD_MASTER_SHEET_ID
    data = read_range(sid, "Settings!B1:B2")
    current_year = int(data[0][0])
    next_num = int(data[1][0])
    load_id = f"{current_year}-{next_num:04d}"
    # Increment counter
    write_range(sid, "Settings!B2", [[next_num + 1]])
    logger.info("Generated Load_ID=%s, next counter=%d", load_id, next_num + 1)
    return load_id


def get_broker_settings() -> dict[str, str]:
    """Read broker constants from Settings tab rows 5-9."""
    settings = get_settings()
    data = read_range(settings.LOAD_MASTER_SHEET_ID, "Settings!A5:B9")
    result = {}
    for row in data:
        if len(row) >= 2:
            result[row[0].strip()] = row[1].strip()
    return result


# ── Load_Master data tab ────────────────────────────────────────────────────

LOAD_MASTER_COLUMNS = [
    "Load_ID", "Customer_Name",
    "Origin_City", "Origin_State", "Origin_Zip",
    "Destination_City", "Destination_State", "Destination_Zip",
    "Pickup_Date", "Pickup_Time_Window", "Delivery_Date", "Delivery_Time_Window",
    "Equipment_Type", "Commodity", "Weight_Lbs",
    "Temp_Control_Required", "Hazmat",
    "Target_Buy_Rate", "Customer_Rate",
    "Assigned_Carrier_MC",
    "Load_Status", "Approval_Status",
    "RFQ_Count", "Created_Date", "Last_Updated", "Internal_Notes",
]


def insert_load(load: dict[str, Any]) -> None:
    """Append a new load row to Load_Master."""
    row = [load.get(c, "") for c in LOAD_MASTER_COLUMNS]
    append_row(get_settings().LOAD_MASTER_SHEET_ID, "Loads!A:Z", row)
    logger.info("Inserted load %s into Load_Master", load.get("Load_ID"))


def update_load_field(load_id: str, field: str, value: Any) -> None:
    """Update a single cell for a given Load_ID in Load_Master."""
    _update_row_field(
        get_settings().LOAD_MASTER_SHEET_ID, "Loads", LOAD_MASTER_COLUMNS,
        "Load_ID", load_id, field, value
    )


def update_load_fields(load_id: str, updates: dict[str, Any]) -> None:
    """Update multiple fields for a given Load_ID."""
    for field, value in updates.items():
        update_load_field(load_id, field, value)


def get_load(load_id: str) -> Optional[dict[str, str]]:
    """Fetch a single load row by Load_ID."""
    rows = read_range(get_settings().LOAD_MASTER_SHEET_ID, "Loads!A:Z")
    if not rows:
        return None
    headers = rows[0]
    for row in rows[1:]:
        padded = row + [""] * (len(headers) - len(row))
        if padded[0] == load_id:
            return dict(zip(headers, padded))
    return None


def get_loads_by_status(status: str) -> list[dict[str, str]]:
    """Return all loads matching a given Load_Status."""
    rows = read_range(get_settings().LOAD_MASTER_SHEET_ID, "Loads!A:Z")
    if not rows:
        return []
    headers = rows[0]
    status_idx = headers.index("Load_Status") if "Load_Status" in headers else None
    if status_idx is None:
        return []
    results = []
    for row in rows[1:]:
        padded = row + [""] * (len(headers) - len(row))
        if padded[status_idx] == status:
            results.append(dict(zip(headers, padded)))
    return results


# ── Carrier_Master ───────────────────────────────────────────────────────────

CARRIER_MASTER_COLUMNS = [
    "MC_Number", "DOT_Number", "Legal_Name", "DBA_Name",
    "Primary_Email", "Primary_Phone",
    "Equipment_Type", "Preferred_Lanes", "Max_Radius_Miles",
    "Insurance_Expiration", "Auto_Liability_Coverage", "Cargo_Coverage",
    "Authority_Status", "Authority_Verified_Date", "Authority_Source",
    "Compliance_Status",
    "W9_On_File", "Active",
    "Onboarding_Status", "Last_Load_Date", "On_Time_Score", "Claims_Count",
    "Internal_Notes",
]


def get_all_carriers() -> list[dict[str, str]]:
    rows = read_range(get_settings().CARRIER_MASTER_SHEET_ID, "Sheet1!A:W")
    if not rows:
        return []
    headers = rows[0]
    return [
        dict(zip(headers, r + [""] * (len(headers) - len(r))))
        for r in rows[1:]
    ]


def get_carrier(mc_number: str) -> Optional[dict[str, str]]:
    for c in get_all_carriers():
        if c.get("MC_Number") == mc_number:
            return c
    return None


def update_carrier_field(mc_number: str, field: str, value: Any) -> None:
    _update_row_field(
        get_settings().CARRIER_MASTER_SHEET_ID, "Sheet1", CARRIER_MASTER_COLUMNS,
        "MC_Number", mc_number, field, value
    )


def update_carrier_fields(mc_number: str, updates: dict[str, Any]) -> None:
    for field, value in updates.items():
        update_carrier_field(mc_number, field, value)


def is_carrier_dispatch_eligible(carrier: dict[str, str]) -> bool:
    """Check full dispatch-eligibility rules."""
    settings = get_settings()
    today_str = date.today().isoformat()
    try:
        ins_exp = carrier.get("Insurance_Expiration", "")
        auto_liab = int(float(carrier.get("Auto_Liability_Coverage", "0") or "0"))
        cargo = int(float(carrier.get("Cargo_Coverage", "0") or "0"))
    except (ValueError, TypeError):
        return False
    return (
        carrier.get("Authority_Status") == "ACTIVE"
        and carrier.get("Compliance_Status") == "CLEAR"
        and ins_exp >= today_str
        and auto_liab >= settings.MIN_AUTO_LIABILITY
        and cargo >= settings.MIN_CARGO_COVERAGE
        and carrier.get("W9_On_File", "").upper() in ("TRUE", "YES", "1")
        and carrier.get("Active", "").upper() in ("TRUE", "YES", "1")
    )


# ── Processed-message idempotency store ──────────────────────────────────────

def is_message_processed(message_id: str) -> bool:
    """Check if a Gmail message ID has already been processed."""
    settings = get_settings()
    try:
        rows = read_range(settings.LOAD_MASTER_SHEET_ID, f"{settings.PROCESSED_STORE_SHEET}!A:A")
        for row in rows:
            if row and row[0] == message_id:
                return True
    except Exception:
        # Sheet tab may not exist yet; treat as not processed
        pass
    return False


def mark_message_processed(message_id: str, context: str = "") -> None:
    """Record a message ID as processed."""
    settings = get_settings()
    try:
        append_row(
            settings.LOAD_MASTER_SHEET_ID,
            f"{settings.PROCESSED_STORE_SHEET}!A:C",
            [message_id, datetime.utcnow().isoformat(), context],
        )
    except Exception:
        # If the tab doesn't exist, create it
        _ensure_processed_tab()
        append_row(
            settings.LOAD_MASTER_SHEET_ID,
            f"{settings.PROCESSED_STORE_SHEET}!A:C",
            [message_id, datetime.utcnow().isoformat(), context],
        )


def _ensure_processed_tab():
    """Create the Processed tab if it doesn't exist."""
    settings = get_settings()
    body = {
        "requests": [{
            "addSheet": {
                "properties": {"title": settings.PROCESSED_STORE_SHEET}
            }
        }]
    }
    try:
        _svc().batchUpdate(
            spreadsheetId=settings.LOAD_MASTER_SHEET_ID, body=body
        ).execute()
    except Exception:
        pass  # already exists


# ── Internal helper ──────────────────────────────────────────────────────────

def _update_row_field(
    sheet_id: str, tab: str, columns: list[str],
    key_col: str, key_val: str, field: str, value: Any,
) -> None:
    """Find a row by key column value and update a specific field."""
    rows = read_range(sheet_id, f"{tab}!A:Z")
    if not rows:
        return
    headers = rows[0]
    key_idx = headers.index(key_col)
    field_idx = headers.index(field)
    col_letter = chr(ord("A") + field_idx) if field_idx < 26 else f"A{chr(ord('A') + field_idx - 26)}"
    for i, row in enumerate(rows[1:], start=2):
        padded = row + [""] * (len(headers) - len(row))
        if padded[key_idx] == key_val:
            cell = f"{tab}!{col_letter}{i}"
            write_range(sheet_id, cell, [[value]])
            logger.info("Updated %s.%s for %s=%s to %s", tab, field, key_col, key_val, value)
            return
    logger.warning("Row not found: %s=%s in %s", key_col, key_val, tab)
