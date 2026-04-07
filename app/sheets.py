"""
BrokerOps AI – Google Sheets helpers for Carrier_Master, Load_Master, and
the idempotency / processed-message store.
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Any, Optional

from app.config import get_settings
from app.google_auth import get_sheets_service

logger = logging.getLogger("brokerops.sheets")

# ── Generic helpers ──────────────────────────────────────────────────────────

def _svc():
    return get_sheets_service().spreadsheets()


def read_range(sheet_id: str, range_: str, retries: int = 3) -> list[list[str]]:
    for attempt in range(retries):
        try:
            resp = _svc().values().get(spreadsheetId=sheet_id, range=range_).execute()
            return resp.get("values", [])
        except Exception as e:
            if "429" in str(e) and attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                logger.warning("Sheets rate limit hit, retrying in %ds...", wait)
                time.sleep(wait)
            else:
                raise


def write_range(sheet_id: str, range_: str, values: list[list], value_input: str = "USER_ENTERED"):
    _svc().values().update(
        spreadsheetId=sheet_id,
        range=range_,
        valueInputOption=value_input,
        body={"values": values},
    ).execute()


def append_row(sheet_id: str, range_: str, row: list, value_input: str = "USER_ENTERED"):
    # Find the actual last row with data to avoid appending far below
    existing = _svc().values().get(spreadsheetId=sheet_id, range=range_).execute()
    existing_rows = existing.get("values", [])
    next_row = len(existing_rows) + 1
    # Extract tab name from range (e.g., "Loads!A:AF" -> "Loads")
    tab = range_.split("!")[0] if "!" in range_ else "Sheet1"

    # Auto-expand grid if the tab doesn't have enough rows
    _ensure_grid_rows(sheet_id, tab, next_row)

    target = f"{tab}!A{next_row}"
    _svc().values().update(
        spreadsheetId=sheet_id,
        range=target,
        valueInputOption=value_input,
        body={"values": [row]},
    ).execute()


def _ensure_grid_rows(sheet_id: str, tab_name: str, needed_row: int):
    """If the tab has fewer rows than needed, expand it automatically."""
    try:
        meta = _svc().get(spreadsheetId=sheet_id).execute()
        for sheet in meta.get("sheets", []):
            if sheet["properties"]["title"] == tab_name:
                current_rows = sheet["properties"]["gridProperties"]["rowCount"]
                if current_rows < needed_row:
                    rows_to_add = max(needed_row - current_rows, 100)
                    _svc().batchUpdate(spreadsheetId=sheet_id, body={
                        "requests": [{
                            "appendDimension": {
                                "sheetId": sheet["properties"]["sheetId"],
                                "dimension": "ROWS",
                                "length": rows_to_add,
                            }
                        }]
                    }).execute()
                    logger.info("Expanded '%s' grid by %d rows (was %d, needed %d)",
                                tab_name, rows_to_add, current_rows, needed_row)
                return
    except Exception as e:
        logger.warning("Could not check/expand grid for '%s': %s", tab_name, e)


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
    "Load_ID", "Customer_Email",
    "Pickup_Date", "Pickup_Time_Window",
    "Commodity",
    "Origin_City", "Origin_State", "Origin_Zip",
    "Pickup_Business_Name", "Pickup_Contact",
    "Delivery_Date", "Delivery_Time_Window",
    "Destination_City", "Destination_State", "Destination_Zip",
    "Delivery_Business_Name", "Delivery_Contact",
    "Equipment_Type", "Weight_Lbs", "Dimensions",
    "Special_Requirements",
    "Temp_Control_Required", "Hazmat",
    "Target_Buy_Rate", "Customer_Rate",
    "Assigned_Carrier_MC",
    "Load_Status", "Approval_Status",
    "RFQ_Count", "Created_Date", "Last_Updated", "Internal_Notes",
]


def insert_load(load: dict[str, Any]) -> None:
    """Append a new load row to Load_Master."""
    row = [load.get(c, "") for c in LOAD_MASTER_COLUMNS]
    append_row(get_settings().LOAD_MASTER_SHEET_ID, "Loads!A:AF", row)
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
    rows = read_range(get_settings().LOAD_MASTER_SHEET_ID, "Loads!A:AF")
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
    rows = read_range(get_settings().LOAD_MASTER_SHEET_ID, "Loads!A:AF")
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

CARRIER_DB_TAB = "'Carrier Database'"
CARRIER_DB_RANGE = f"{CARRIER_DB_TAB}!A:AE"

# Actual sheet columns (BrokerOps - Carrier Database)
CARRIER_MASTER_COLUMNS = [
    "Carrier ID", "Status", "Company Name", "MC Number", "DOT Number",
    "Contact Name", "Contact Email", "Contact Phone",
    "Dispatcher Name", "Dispatcher Email", "Dispatcher Phone",
    "Address", "City", "State", "ZIP",
    "Equipment Types", "Fleet Size",
    "Insurance Liability", "Insurance Cargo", "Insurance Expiry",
    "Authority Status", "Authority Date", "Safety Rating",
    "Has GPS", "GPS Provider",
    "Compliance Status", "Last Compliance Check",
    "Score", "Outreach Status", "Onboarded Date", "Notes",
]

# Map internal field names (used by fmcsa.py, carrier_search.py) to sheet columns
_FIELD_MAP = {
    "MC_Number": "MC Number",
    "DOT_Number": "DOT Number",
    "Legal_Name": "Company Name",
    "DBA_Name": "Company Name",  # fallback
    "Primary_Email": "Contact Email",
    "Contact_Email_Source": "Notes",  # append to notes
    "Primary_Phone": "Contact Phone",
    "Website": "Notes",  # append to notes
    "Equipment_Type": "Equipment Types",
    "Preferred_Lanes": "Notes",  # append to notes
    "Insurance_Expiration": "Insurance Expiry",
    "Auto_Liability_Coverage": "Insurance Liability",
    "Cargo_Coverage": "Insurance Cargo",
    "Authority_Status": "Authority Status",
    "Authority_Verified_Date": "Authority Date",
    "Compliance_Status": "Compliance Status",
    "Active": "Status",
    "Onboarding_Status": "Outreach Status",
    "On_Time_Score": "Score",
    "Internal_Notes": "Notes",
    "Power_Units": "Fleet Size",
    "Safety_Rating": "Safety Rating",
}


def _map_fields_to_sheet(fields: dict[str, Any]) -> dict[str, Any]:
    """Convert internal field names to actual sheet column names."""
    mapped = {}
    notes_parts = []

    # Resolve company name: prefer DBA_Name over Legal_Name if it looks like a real business name
    legal = fields.get("Legal_Name", "")
    dba = fields.get("DBA_Name", "")
    if dba and len(dba) > 2 and dba not in ("--", "0"):
        mapped["Company Name"] = dba
    elif legal and len(legal) > 2 and legal not in ("--", "0"):
        mapped["Company Name"] = legal

    for k, v in fields.items():
        if k in ("Legal_Name", "DBA_Name"):
            continue  # handled above
        sheet_col = _FIELD_MAP.get(k, k)
        if sheet_col == "Notes" and k != "Internal_Notes":
            if v:
                notes_parts.append(f"{k}: {v}")
        elif sheet_col in CARRIER_MASTER_COLUMNS:
            mapped[sheet_col] = v
    # Merge notes
    if notes_parts:
        existing_notes = mapped.get("Notes", "")
        if existing_notes:
            mapped["Notes"] = f"{existing_notes}; {'; '.join(notes_parts)}"
        else:
            mapped["Notes"] = "; ".join(notes_parts)
    if "Internal_Notes" in fields and fields["Internal_Notes"]:
        existing = mapped.get("Notes", "")
        if existing:
            mapped["Notes"] = f"{existing}; {fields['Internal_Notes']}"
        else:
            mapped["Notes"] = fields["Internal_Notes"]
    return mapped


def get_all_carriers() -> list[dict[str, str]]:
    rows = read_range(get_settings().CARRIER_MASTER_SHEET_ID, CARRIER_DB_RANGE)
    if not rows:
        return []
    headers = rows[0]
    return [
        dict(zip(headers, r + [""] * (len(headers) - len(r))))
        for r in rows[1:]
    ]


def get_carrier(mc_number: str) -> Optional[dict[str, str]]:
    """Look up a carrier by MC Number."""
    for c in get_all_carriers():
        if c.get("MC Number") == mc_number:
            return c
    return None


def get_carrier_by_dot(dot_number: str) -> Optional[dict[str, str]]:
    """Look up a carrier by DOT Number (fallback when MC Number is empty)."""
    for c in get_all_carriers():
        if c.get("DOT Number") == dot_number:
            return c
    return None


def find_carrier(mc_number: str, dot_number: str) -> Optional[dict[str, str]]:
    """Find a carrier by MC Number first, falling back to DOT Number."""
    if mc_number:
        result = get_carrier(mc_number)
        if result:
            return result
    if dot_number:
        return get_carrier_by_dot(dot_number)
    return None


def update_carrier_field(mc_number: str, field: str, value: Any) -> None:
    sheet_field = _FIELD_MAP.get(field, field)
    _update_row_field(
        get_settings().CARRIER_MASTER_SHEET_ID, CARRIER_DB_TAB, CARRIER_MASTER_COLUMNS,
        "MC Number", mc_number, sheet_field, value
    )


def update_carrier_field_by_dot(dot_number: str, field: str, value: Any) -> None:
    """Update a single cell for a carrier identified by DOT Number."""
    sheet_field = _FIELD_MAP.get(field, field)
    _update_row_field(
        get_settings().CARRIER_MASTER_SHEET_ID, CARRIER_DB_TAB, CARRIER_MASTER_COLUMNS,
        "DOT Number", dot_number, sheet_field, value
    )


def update_carrier_fields(mc_number: str, updates: dict[str, Any]) -> None:
    for field, value in updates.items():
        update_carrier_field(mc_number, field, value)


def update_carrier_fields_by_dot(dot_number: str, updates: dict[str, Any]) -> None:
    """Update multiple fields for a carrier identified by DOT_Number."""
    for field, value in updates.items():
        update_carrier_field_by_dot(dot_number, field, value)


def update_carrier_fields_by_key(mc_number: str, dot_number: str, updates: dict[str, Any]) -> None:
    """Update carrier fields using MC_Number if available, otherwise DOT_Number."""
    if mc_number:
        update_carrier_fields(mc_number, updates)
    elif dot_number:
        update_carrier_fields_by_dot(dot_number, updates)


def insert_carrier(fields: dict[str, Any]) -> None:
    """Append a new carrier row to Carrier Database."""
    mapped = _map_fields_to_sheet(fields)
    # Generate Carrier ID from DOT number
    dot = fields.get("DOT_Number", "") or fields.get("DOT Number", "")
    if not mapped.get("Carrier ID") and dot:
        mapped["Carrier ID"] = f"DOT-{dot}"
    if not mapped.get("Status"):
        mapped["Status"] = "prospect"
    row = [str(mapped.get(c, "")) for c in CARRIER_MASTER_COLUMNS]
    append_row(get_settings().CARRIER_MASTER_SHEET_ID, f"{CARRIER_DB_RANGE}", row)
    logger.info("Inserted carrier %s into Carrier Database", mapped.get("Company Name", dot))


def search_carriers_in_sheet(
    *,
    state: str | None = None,
    equipment_type: str | None = None,
    min_score: int | None = None,
    outreach_status: str | None = None,
) -> list[dict[str, str]]:
    """Query existing Carrier_Master rows by filters."""
    all_carriers = get_all_carriers()
    results = []
    for c in all_carriers:
        if state and c.get("Preferred_Lanes", "") and state.upper() not in c.get("Preferred_Lanes", "").upper():
            # If Preferred_Lanes is set, check it; otherwise don't filter by state
            pass  # allow through if no preferred lanes set
        if equipment_type and equipment_type.upper() not in c.get("Equipment_Type", "").upper():
            continue
        if min_score is not None:
            try:
                if int(float(c.get("On_Time_Score", "0") or "0")) < min_score:
                    continue
            except ValueError:
                continue
        if outreach_status and c.get("Onboarding_Status", "") != outreach_status:
            continue
        results.append(c)
    return results


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
    rows = read_range(sheet_id, f"{tab}!A:AE")
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
