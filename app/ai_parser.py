"""
BrokerOps AI – Gemini-based AI helpers.

- Email classification: distinguish load requests from carrier replies
- Fallback parser: extract structured load data from free-form emails
- Completeness check and auto-reply builder
"""
from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from app.config import get_settings

logger = logging.getLogger("brokerops.ai_parser")

# Fields we ask Gemini to extract – must match Load_Master column names
_FIELDS = [
    "Customer_Name", "Origin_City", "Origin_State", "Origin_Zip",
    "Destination_City", "Destination_State", "Destination_Zip",
    "Pickup_Date", "Pickup_Time_Window", "Delivery_Date", "Delivery_Time_Window",
    "Equipment_Type", "Commodity", "Weight_Lbs",
    "Temp_Control_Required", "Hazmat", "Special_Requirements",
    "Pickup_Business", "Delivery_Business",
    "Pickup_Contact", "Delivery_Contact", "Target_Buy_Rate",
]

_SYSTEM_PROMPT = """You are a freight brokerage data extraction assistant for De Boer Freight.
You are an expert in the trucking and logistics industry. Extract load shipment details
from the email below and return ONLY a JSON object.

CRITICAL RULES:
- Return ONLY valid JSON, no markdown, no explanation, no backticks.
- Use exactly these field names: {fields}

DATE & TIME RULES:
- For dates, use YYYY-MM-DD format. Today's date context will be provided.
- "Tomorrow" = the day after today. "Next Monday" = the upcoming Monday.
- "ASAP" or "today" = today's date.
- For time windows, use HH:MM-HH:MM 24h format, or "OPEN" if not specified.
- Interpret cutoff language into proper windows:
  - "at the latest 10am" or "no later than 10" or "must arrive by 10am" → end of window is 10:00
  - "opens at 8" or "no earlier than 8am" or "available starting 8" → start of window is 08:00
  - "between 8 and 10am" → "08:00-10:00"
  - "Theatre opens at 8. Driver can arrive at the latest 10am" → "08:00-10:00"
  - "must deliver by 3pm" → "06:00-15:00"
  - "deliver before noon" → "06:00-12:00"
  - "after 2pm" → "14:00-18:00"
- If only one time is given with no context: "8 AM" → "08:00-17:00"
- General terms: "morning" = "06:00-12:00", "afternoon" = "12:00-18:00",
  "first thing" = "06:00-09:00", "end of day" or "EOD" = "15:00-18:00"
- "FCFS" (first come first served) or "flexible" = "OPEN"

EQUIPMENT INFERENCE RULES (this is critical for freight):
- Use one of: DRY_VAN, FLATBED, REEFER, CONESTOGA, BOX_TRUCK, SPRINTER, HOTSHOT
- If commodity is frozen, refrigerated, cold, or perishable → REEFER
- "Frozen shrimp", "frozen anything", "ice cream", "produce", "meat", "dairy" → REEFER
- If commodity is steel, lumber, machinery, equipment, pipes, beams → FLATBED
- If no equipment mentioned and commodity doesn't suggest otherwise → DRY_VAN
- Set Temp_Control_Required to "TRUE" whenever Equipment_Type is REEFER

WEIGHT RULES:
- "30,000 pounds" or "30000 lbs" = 30000
- "44k lbs" or "44K" = 44000
- "30,000 pounds of frozen shrimp" → Weight_Lbs = "30000"

LOCATION INFERENCE:
- Always infer state from city when possible. "Key West" → FL, "Miami" → FL,
  "Houston" → TX, "Chicago" → IL, "Dallas" → TX, "Memphis" → TN, etc.
- Use 2-letter state abbreviations.
- Look up zip codes if you know them, otherwise leave blank.

RATE/PRICE RULES:
- "$800", "800 dollars", "budget of $800", "can you do it for 800" → Target_Buy_Rate = "800"
- Remove $ signs and commas from the number.

CUSTOMER NAME:
- "This is Derek with Atlantic Seafood" → Customer_Name = "Atlantic Seafood"
- "John from XYZ Logistics" → Customer_Name = "XYZ Logistics"
- Use the company name, not the person's name.

SPECIAL REQUIREMENTS (comma-separated list, or empty string if none):
Look for any of these and include ALL that apply:
- Equipment accessories: lift gate, pallet jack, dolly, tarps, straps, chains,
  coil racks, load bars, blanket wrap, edge protectors
- Handling: team drivers, white glove, inside delivery, inside pickup,
  residential delivery, limited access, appointment required, driver assist,
  no-touch freight, drop trailer
- Load specifics: oversize permit, overweight permit, escort required,
  TWIC card required, pilot car needed
- Commodity-specific: "tarp required" for exposed flatbed loads (steel, lumber, etc.),
  "temperature monitoring" for reefer loads
- Infer when appropriate: "lift gate" if loading/unloading at a location without a dock
  (theaters, residences, retail stores), "pallet jack" if mentioned, "tarp" if flatbed
  with weather-sensitive cargo
- Example: "lift gate, pallet jack, inside delivery"
- If nothing special mentioned, use empty string ""

PICKUP & DELIVERY BUSINESS NAMES:
- Pickup_Business: the business name at the pickup location (shipper/warehouse).
- Delivery_Business: the business name at the delivery location (consignee/receiver).
- These are the physical locations, NOT the customer/broker who booked the load.
- "Pick up at Johnson Cold Storage in Miami" → Pickup_Business = "Johnson Cold Storage"
- "Delivering to Walmart DC #4523 in Dallas" → Delivery_Business = "Walmart DC #4523"
- "Receiver is Atlantic Fresh Market" → Delivery_Business = "Atlantic Fresh Market"
- "Loading at the shipper, FreshCo Packing" → Pickup_Business = "FreshCo Packing"
- If no business name is mentioned for a location, use empty string "".

PICKUP & DELIVERY CONTACTS:
- Combine all contact info for each location into a single string.
- Format: "Name / phone / email" — include whichever pieces are available.
- "Ask for John at the dock, 305-555-1234" → Pickup_Contact = "John / 305-555-1234"
- "Receiving dept: Jane Smith jane@example.com" → Delivery_Contact = "Jane Smith / jane@example.com"
- "Call Mike at 786-555-0000 when 30 min out" → include in the relevant contact field
  with the note: "Mike / 786-555-0000 / call 30 min out"
- If the sender provides their own contact info for coordination, use it for the
  relevant contact field.
- If no contact info is given, use empty string "".

GENERAL:
- Be aggressive about extracting data. This is a freight brokerage inbox — assume
  every email is about moving freight unless clearly otherwise.
- Ignore email signatures, "Sent from my iPhone", legal disclaimers, etc.
- If a field cannot be determined, use an empty string "".
""".format(fields=", ".join(_FIELDS))


_gemini_api_key: str | None = None


def _get_gemini_api_key() -> str:
    """Retrieve the Gemini API key from Secret Manager (cached after first call)."""
    global _gemini_api_key
    if _gemini_api_key:
        return _gemini_api_key

    from google.cloud import secretmanager
    settings = get_settings()
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{settings.GCP_PROJECT_ID}/secrets/brokerops-gemini-api-key/versions/latest"
    response = client.access_secret_version(request={"name": name})
    _gemini_api_key = response.payload.data.decode("UTF-8")
    return _gemini_api_key


def _call_gemini(prompt: str, max_tokens: int = 1024) -> str:
    """
    Send a prompt to Gemini via the Google AI Studio API (API key auth).
    """
    api_key = _get_gemini_api_key()

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": max_tokens,
        },
    }

    models = ["gemini-2.5-flash", "gemini-2.5-pro"]
    errors = []

    for model in models:
        endpoint = (
            f"https://generativelanguage.googleapis.com/v1beta/"
            f"models/{model}:generateContent?key={api_key}"
        )

        try:
            resp = httpx.post(
                endpoint,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30.0,
            )
            if resp.status_code != 200:
                error_body = resp.text[:500]
                logger.warning("Gemini model '%s' returned %d: %s", model, resp.status_code, error_body)
                errors.append(f"{model}: {resp.status_code} - {error_body}")
                continue

            data = resp.json()
            text = (
                data.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
            )

            # Clean up markdown code fences Gemini sometimes adds
            text = text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[-1]
            if text.endswith("```"):
                text = text.rsplit("```", 1)[0]

            logger.info("Gemini call succeeded with model '%s'", model)
            return text.strip()

        except Exception as e:
            logger.warning("Gemini model '%s' failed: %s", model, e)
            errors.append(f"{model}: {str(e)}")
            continue

    raise RuntimeError(f"All Gemini models failed. Errors: {'; '.join(errors)}")


def _extract_json(text: str) -> dict:
    """Extract JSON from a Gemini response that may contain extra text."""
    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Find JSON object in the text
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        return json.loads(text[start:end])
    raise ValueError(f"No valid JSON found in response: {text[:200]}")


# ── Email classification ───────────────────────────────────────────────────

_CLASSIFY_PROMPT = """You are a freight brokerage email classifier for De Boer Freight.

This is a freight brokerage inbox. Almost every email is about moving goods from
point A to point B. Your DEFAULT answer should be NEW_LOAD.

Categories:

1. NEW_LOAD (DEFAULT) — Someone wants freight moved. This includes:
   - Formal load tenders with structured fields
   - Casual messages like "I need 30,000 pounds of frozen shrimp moved from Key West to Miami"
   - Quote requests like "what would you charge for a flatbed Dallas to Chicago?"
   - ANY mention of: moving goods, shipping, hauling, needing a truck, weight, lbs,
     pounds, commodity, frozen, reefer, flatbed, dry van, pickup, delivery, rate,
     price, budget, quote, cities that could be origin/destination
   - Even if the email is informal, from a phone ("Sent from my iPhone"), or poorly written
   - WHEN IN DOUBT, USE THIS CATEGORY

2. CARRIER_QUOTE — ONLY when a motor carrier is replying to an RFQ (Request for Quote)
   that WE (De Boer Freight) sent them. Must have clear indicators like:
   - Subject line starts with "Re: RFQ" or references Load_ID format YYYY-####
   - The email is explicitly responding to our outreach
   - Do NOT use this for customers asking US for quotes — that's NEW_LOAD

3. LOAD_UPDATE — Follow-up on an existing load. Must explicitly reference:
   - An existing Load_ID (format YYYY-####)
   - Words like "update to load", "correction to", "revised pickup"

4. OTHER — ONLY for emails with ZERO connection to freight/logistics:
   - Marketing spam, newsletters
   - IT system notifications
   - Personal emails completely unrelated to shipping
   - You should RARELY use this category

CRITICAL: An email saying "I need X pounds of Y moved from A to B for $Z" is
ALWAYS NEW_LOAD, no matter how casual the language is.

Return ONLY a JSON object:
- "category": one of "NEW_LOAD", "CARRIER_QUOTE", "LOAD_UPDATE", "OTHER"
- "confidence": a number 0.0 to 1.0
- "reason": a brief one-sentence explanation

No markdown, no backticks, just JSON.
"""


def classify_email(body: str, subject: str = "", from_addr: str = "") -> dict[str, Any]:
    """
    Classify an inbound email using Gemini.
    Returns {"category": str, "confidence": float, "reason": str}.
    """
    full_text = (
        f"From: {from_addr}\n"
        f"Subject: {subject}\n\n"
        f"{body}"
    )
    prompt = f"{_CLASSIFY_PROMPT}\n\nEmail:\n---\n{full_text}\n---\n\nJSON:"

    try:
        text = _call_gemini(prompt, max_tokens=256)
        logger.info("Classification raw response: %s", text[:300])
        result = _extract_json(text)
        logger.info("Email classified as %s (confidence: %s): %s",
                     result.get("category"), result.get("confidence"), result.get("reason"))
        return result
    except Exception as e:
        logger.error("Email classification failed: %s – defaulting to NEW_LOAD", e)
        return {"category": "NEW_LOAD", "confidence": 0.0, "reason": "Classification failed, defaulting"}


# ── Load data extraction ───────────────────────────────────────────────────

def parse_with_gemini(email_body: str, subject: str = "") -> dict[str, Any]:
    """
    Send the email text to Gemini and return structured fields.
    """
    from datetime import date, timedelta
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    day_of_week = today.strftime("%A")

    # Build a reference calendar so Gemini doesn't miscalculate days
    date_ref_lines = [f"Today: {day_of_week} {today_str}"]
    for i in range(1, 8):
        d = today + timedelta(days=i)
        label = "Tomorrow" if i == 1 else d.strftime("%A")
        date_ref_lines.append(f"{label}: {d.strftime('%A')} {d.strftime('%Y-%m-%d')}")

    date_reference = "\n".join(date_ref_lines)

    full_text = f"Subject: {subject}\n\n{email_body}" if subject else email_body
    prompt = (
        f"{_SYSTEM_PROMPT}\n\n"
        f"DATE REFERENCE (use these exact dates — do NOT calculate yourself):\n"
        f"{date_reference}\n\n"
        f"Email:\n---\n{full_text}\n---\n\n"
        f"Extract all load details from this email. Return ONLY a JSON object with "
        f"the field names listed above. Every value must be a string. Do not return "
        f"field names as values — return the ACTUAL DATA extracted from the email.\n\n"
        f"JSON:"
    )

    try:
        text = _call_gemini(prompt)
        logger.info("Gemini raw response: %s", text[:500])
        parsed = json.loads(text)

        # Validate: reject if values look like field names (a known Gemini failure mode)
        result = {}
        for k in _FIELDS:
            val = parsed.get(k, "")
            if val is None:
                val = ""
            val = str(val).strip()
            # Reject values that are just the field name or label-like
            if val.lower().replace("_", " ") in (k.lower().replace("_", " "), k.lower()):
                val = ""
            result[k] = val

        filled = sum(1 for v in result.values() if v)
        logger.info("Gemini parsed %d non-empty fields", filled)
        return result
    except Exception as e:
        logger.error("Gemini parsing failed: %s", e)
        return {}


# ── Required fields and completeness check ─────────────────────────────────

# Fields that MUST be present to progress the load through the pipeline
REQUIRED_FIELDS = [
    "Origin_City", "Origin_State",
    "Destination_City", "Destination_State",
    "Pickup_Date", "Equipment_Type",
]

# Fields we strongly want but can proceed without
PREFERRED_FIELDS = [
    "Customer_Name", "Commodity", "Weight_Lbs", "Target_Buy_Rate",
]


def check_completeness(fields: dict[str, Any]) -> dict[str, list[str]]:
    """
    Check which required and preferred fields are missing.
    Returns {"missing_required": [...], "missing_preferred": [...]}.
    """
    missing_req = [f for f in REQUIRED_FIELDS if not fields.get(f)]
    missing_pref = [f for f in PREFERRED_FIELDS if not fields.get(f)]
    return {"missing_required": missing_req, "missing_preferred": missing_pref}


def build_missing_fields_reply(
    missing_required: list[str],
    missing_preferred: list[str],
    load_id: str,
) -> str:
    """Build a friendly auto-reply asking the dispatcher for missing info."""

    # Human-friendly field names
    friendly = {
        "Origin_City": "origin city",
        "Origin_State": "origin state",
        "Destination_City": "destination city",
        "Destination_State": "destination state",
        "Pickup_Date": "pickup date",
        "Equipment_Type": "equipment/trailer type",
        "Customer_Name": "customer/shipper name",
        "Commodity": "commodity description",
        "Weight_Lbs": "weight (lbs)",
        "Target_Buy_Rate": "target rate/budget",
        "Origin_Zip": "origin zip code",
        "Destination_Zip": "destination zip code",
        "Delivery_Date": "delivery date",
    }

    lines = [
        f"Hi,\n",
        f"Thanks for submitting load {load_id}. We're working on getting carriers lined up, "
        f"but we need a few more details to proceed:\n",
    ]

    if missing_required:
        lines.append("**Required to move forward:**")
        for f in missing_required:
            lines.append(f"  - {friendly.get(f, f)}")
        lines.append("")

    if missing_preferred:
        lines.append("Helpful if you have it:")
        for f in missing_preferred:
            lines.append(f"  - {friendly.get(f, f)}")
        lines.append("")

    lines.append(
        "Just reply to this email with the missing info and we'll get it updated right away.\n\n"
        "Thanks,\nBrokerOps AI"
    )

    return "\n".join(lines)
