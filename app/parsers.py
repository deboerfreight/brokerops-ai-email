"""
BrokerOps AI – Email parsing utilities.

Centralised parsing logic for:
  - Load-request emails (OPS/NEW_LOAD)
  - Carrier RFQ reply / quote emails
  - Broker approval replies
"""
from __future__ import annotations

import re
import logging
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger("brokerops.parsers")

# ── Time-window normalisation ────────────────────────────────────────────────

_TIME_RE = re.compile(
    r"(\d{1,2}):?(\d{2})?\s*(am|pm|AM|PM)?"
    r"\s*[-–to]+\s*"
    r"(\d{1,2}):?(\d{2})?\s*(am|pm|AM|PM)?",
)

_SINGLE_TIME_RE = re.compile(r"(\d{1,2}):?(\d{2})?\s*(am|pm|AM|PM)?")


def _to_24h(hour: int, minute: int, ampm: str | None) -> str:
    if ampm:
        ampm = ampm.upper()
        if ampm == "PM" and hour != 12:
            hour += 12
        elif ampm == "AM" and hour == 12:
            hour = 0
    return f"{hour:02d}:{minute:02d}"


def normalise_time_window(raw: str) -> str:
    """
    Convert a raw time-window string to HH:MM-HH:MM (24h) or OPEN.

    Handles patterns like:
      "8am-5pm", "08:00 - 17:00", "8:00AM to 5:00PM", "OPEN", "FCFS"
    """
    if not raw:
        return "OPEN"
    cleaned = raw.strip().upper()
    if cleaned in ("OPEN", "FCFS", "ANY", "FLEXIBLE", "TBD", "N/A", ""):
        return "OPEN"

    m = _TIME_RE.search(raw)
    if m:
        h1 = int(m.group(1))
        m1 = int(m.group(2) or 0)
        ap1 = m.group(3)
        h2 = int(m.group(4))
        m2 = int(m.group(5) or 0)
        ap2 = m.group(6)
        return f"{_to_24h(h1, m1, ap1)}-{_to_24h(h2, m2, ap2)}"

    return "OPEN"


# ── Date normalisation ───────────────────────────────────────────────────────

_DATE_FORMATS = [
    "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y",
    "%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y",
    "%d %B %Y", "%d %b %Y",
]


def normalise_date(raw: str) -> str:
    """Try to parse a date string into YYYY-MM-DD format."""
    raw = raw.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Fallback: return as-is
    return raw


# ── Equipment type normalisation ─────────────────────────────────────────────

_EQUIP_MAP = {
    "dry van": "DRY_VAN", "dryvan": "DRY_VAN", "van": "DRY_VAN",
    "flatbed": "FLATBED", "flat bed": "FLATBED", "fb": "FLATBED",
    "conestoga": "CONESTOGA",
    "reefer": "REEFER", "refrigerated": "REEFER", "temp controlled": "REEFER",
    "box truck": "BOX_TRUCK", "boxtruck": "BOX_TRUCK", "box": "BOX_TRUCK",
    "sprinter": "SPRINTER", "sprinter van": "SPRINTER", "cargo van": "SPRINTER",
    "hotshot": "HOTSHOT", "hot shot": "HOTSHOT",
}


def normalise_equipment(raw: str) -> str:
    raw_lower = raw.strip().lower()
    if raw_lower in _EQUIP_MAP:
        return _EQUIP_MAP[raw_lower]
    # Check if already in canonical form
    canonical = raw.strip().upper().replace(" ", "_")
    valid = {"DRY_VAN", "FLATBED", "CONESTOGA", "REEFER", "BOX_TRUCK", "SPRINTER", "HOTSHOT"}
    if canonical in valid:
        return canonical
    return raw.strip().upper()


# ── Load email parser ────────────────────────────────────────────────────────

def _extract(text: str, patterns: list[str], default: str = "") -> str:
    """Try multiple regex patterns; return first match group(1)."""
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return default


def _extract_bool(text: str, keyword: str) -> bool:
    """Check if a keyword is followed by yes/true/required."""
    m = re.search(rf"{keyword}\s*[:\-]?\s*(yes|true|required|y)", text, re.IGNORECASE)
    return bool(m)


def _extract_dollar(text: str, patterns: list[str]) -> str:
    """Extract a dollar amount, remove $ and commas."""
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return re.sub(r"[$,]", "", m.group(1).strip())
    return ""


def parse_load_email(body: str, subject: str = "") -> dict[str, Any]:
    """
    Parse a load request email body into Load_Master fields.
    Uses keyword-based extraction that handles common broker email formats.
    """
    full = f"{subject}\n{body}"

    customer = _extract(full, [
        r"(?:customer|shipper|account|company)\s*[:\-]\s*(.+?)(?:\n|$)",
    ])

    origin_city = _extract(full, [
        r"(?:origin|pickup|pick\s*up)\s*(?:city)?\s*[:\-]\s*([A-Za-z\s.]+?)(?:,|\n|$)",
        r"(?:from|origin)\s*[:\-]\s*([A-Za-z\s.]+?),",
    ])
    origin_state = _extract(full, [
        r"(?:origin|pickup|pick\s*up)\s*(?:state)?\s*[:\-]\s*[A-Za-z\s.]+?,\s*([A-Z]{2})",
        r"(?:from|origin)\s*[:\-]\s*[A-Za-z\s.]+?,\s*([A-Z]{2})",
    ])
    origin_zip = _extract(full, [
        r"(?:origin|pickup)\s*(?:zip)?\s*[:\-]?\s*\d*.*?(\d{5})",
    ])

    dest_city = _extract(full, [
        r"(?:destination|delivery|deliver|dest|consignee)\s*(?:city)?\s*[:\-]\s*([A-Za-z\s.]+?)(?:,|\n|$)",
        r"(?:to|destination)\s*[:\-]\s*([A-Za-z\s.]+?),",
    ])
    dest_state = _extract(full, [
        r"(?:destination|delivery|deliver|dest|consignee)\s*(?:state)?\s*[:\-]\s*[A-Za-z\s.]+?,\s*([A-Z]{2})",
        r"(?:to|destination)\s*[:\-]\s*[A-Za-z\s.]+?,\s*([A-Z]{2})",
    ])
    dest_zip = _extract(full, [
        r"(?:destination|delivery|dest)\s*(?:zip)?\s*[:\-]?\s*\d*.*?(\d{5})",
    ])

    pickup_date_raw = _extract(full, [
        r"(?:pickup|pick\s*up)\s*date\s*[:\-]\s*([^\n]+?)(?:\n|$)",
    ])
    pickup_window_raw = _extract(full, [
        r"(?:pickup|pick\s*up)\s*(?:time|window)\s*[:\-]\s*(.+?)(?:\n|$)",
    ])
    delivery_date_raw = _extract(full, [
        r"(?:delivery|deliver|del)\s*date\s*[:\-]\s*([^\n]+?)(?:\n|$)",
    ])
    delivery_window_raw = _extract(full, [
        r"(?:delivery|deliver|del)\s*(?:time|window)\s*[:\-]\s*(.+?)(?:\n|$)",
    ])

    equipment_raw = _extract(full, [
        r"(?:equipment|equip|trailer|truck)\s*(?:type)?\s*[:\-]\s*(.+?)(?:\n|$)",
    ])
    commodity = _extract(full, [
        r"(?:commodity|cargo|goods|product|freight)\s*[:\-]\s*(.+?)(?:\n|$)",
    ])
    weight = _extract(full, [
        r"(?:weight|lbs|pounds)\s*[:\-]\s*([\d,]+)",
        r"([\d,]+)\s*(?:lbs|pounds)",
    ])

    temp_control = _extract_bool(full, "temp")
    hazmat = _extract_bool(full, "hazmat")

    target_rate = _extract_dollar(full, [
        r"(?:target|buy|budget|max)\s*(?:rate|price)?\s*[:\-]\s*\$?([\d,.]+)",
        r"(?:rate|price)\s*[:\-]\s*\$?([\d,.]+)",
    ])

    return {
        "Customer_Name": customer,
        "Origin_City": origin_city,
        "Origin_State": origin_state.upper() if origin_state else "",
        "Origin_Zip": origin_zip,
        "Destination_City": dest_city,
        "Destination_State": dest_state.upper() if dest_state else "",
        "Destination_Zip": dest_zip,
        "Pickup_Date": normalise_date(pickup_date_raw) if pickup_date_raw else "",
        "Pickup_Time_Window": normalise_time_window(pickup_window_raw),
        "Delivery_Date": normalise_date(delivery_date_raw) if delivery_date_raw else "",
        "Delivery_Time_Window": normalise_time_window(delivery_window_raw),
        "Equipment_Type": normalise_equipment(equipment_raw) if equipment_raw else "",
        "Commodity": commodity,
        "Weight_Lbs": weight.replace(",", "") if weight else "",
        "Temp_Control_Required": str(temp_control).upper(),
        "Hazmat": str(hazmat).upper(),
        "Target_Buy_Rate": target_rate,
    }


# ── Quote / RFQ reply parser ────────────────────────────────────────────────

_DOLLAR_RE = re.compile(r"\$\s?([\d,]+(?:\.\d{1,2})?)")


def parse_quote_reply(body: str) -> dict[str, Any]:
    """
    Parse a carrier's RFQ reply.  The "first dollar amount" rule applies.

    Returns:
        rate: float or None
        availability: str
        transit_time: str
        restrictions: str
    """
    # Structured markers first
    rate_str = _extract(body, [
        r"Rate\s*[:\-]\s*\$?\s*([\d,]+(?:\.\d{1,2})?)",
    ])
    if not rate_str:
        # First dollar amount in the body
        m = _DOLLAR_RE.search(body)
        if m:
            rate_str = m.group(1)

    rate: Optional[float] = None
    if rate_str:
        try:
            rate = float(rate_str.replace(",", ""))
        except ValueError:
            pass

    availability = _extract(body, [r"Availability\s*[:\-]\s*(.+?)(?:\n|$)"])
    transit_time = _extract(body, [r"Transit\s*Time\s*[:\-]\s*(.+?)(?:\n|$)"])
    restrictions = _extract(body, [r"(?:restrictions?|notes?)\s*[:\-]\s*(.+?)(?:\n|$)"])

    return {
        "rate": rate,
        "availability": availability,
        "transit_time": transit_time,
        "restrictions": restrictions,
    }


# ── Approval reply parser ───────────────────────────────────────────────────

def parse_approval_reply(body: str) -> dict[str, Any]:
    """
    Look for exact phrases:
        APPROVE {Load_ID}
        REJECT {Load_ID}
    Returns: {"action": "APPROVE"|"REJECT"|None, "load_id": str|None}
    """
    # Check all lines in the body
    for line in body.splitlines():
        line = line.strip()
        m = re.match(r"(APPROVE|REJECT)\s+(\d{4}-\d{4})", line)
        if m:
            return {"action": m.group(1), "load_id": m.group(2)}
    return {"action": None, "load_id": None}
