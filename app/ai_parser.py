"""
BrokerOps AI – Gemini-based fallback parser for free-form load emails.

When the regex parser returns too many empty fields, this module sends the
email text to Google Gemini (free tier on Vertex AI) and asks it to extract
structured load data.
"""
from __future__ import annotations

import json
import logging
from typing import Any

import google.auth
import google.auth.transport.requests
import httpx

from app.config import get_settings

logger = logging.getLogger("brokerops.ai_parser")

# Fields we ask Gemini to extract – must match Load_Master column names
_FIELDS = [
    "Customer_Name", "Origin_City", "Origin_State", "Origin_Zip",
    "Destination_City", "Destination_State", "Destination_Zip",
    "Pickup_Date", "Pickup_Time_Window", "Delivery_Date", "Delivery_Time_Window",
    "Equipment_Type", "Commodity", "Weight_Lbs",
    "Temp_Control_Required", "Hazmat", "Target_Buy_Rate",
]

_SYSTEM_PROMPT = """You are a freight brokerage data extraction assistant.
Extract load shipment details from the email below and return ONLY a JSON object.

Rules:
- Return ONLY valid JSON, no markdown, no explanation, no backticks.
- Use exactly these field names: {fields}
- For dates, use YYYY-MM-DD format. If only a day name is given (e.g. "next Thursday"),
  calculate the actual date based on today's context.
- For time windows, use HH:MM-HH:MM 24h format, or "OPEN" if not specified.
- For Equipment_Type, use one of: DRY_VAN, FLATBED, REEFER, CONESTOGA, BOX_TRUCK, SPRINTER, HOTSHOT
- For Weight_Lbs, return just the number (no commas).
- For Temp_Control_Required and Hazmat, return "TRUE" or "FALSE".
- For Target_Buy_Rate, return just the number (no $ sign, no commas).
- For states, use 2-letter abbreviations (e.g. TX, CA).
- For zip codes, use 5-digit format.
- If a field cannot be determined from the email, use an empty string "".
- Infer information when reasonable. E.g. if someone says "Houston" with no state,
  infer "TX". If they say "44k lbs", interpret as "44000".
""".format(fields=", ".join(_FIELDS))


def parse_with_gemini(email_body: str, subject: str = "") -> dict[str, Any]:
    """
    Send the email text to Gemini and return structured fields.
    Uses Application Default Credentials (the Cloud Run service account).
    """
    settings = get_settings()
    project = settings.GCP_PROJECT_ID
    region = settings.GCP_REGION

    # Build the request
    full_text = f"Subject: {subject}\n\n{email_body}" if subject else email_body
    prompt = f"{_SYSTEM_PROMPT}\n\nEmail:\n---\n{full_text}\n---\n\nJSON:"

    # Use Vertex AI Gemini endpoint with ADC
    endpoint = (
        f"https://{region}-aiplatform.googleapis.com/v1/"
        f"projects/{project}/locations/{region}/"
        f"publishers/google/models/gemini-2.0-flash:generateContent"
    )

    # Get credentials via ADC (works on Cloud Run automatically)
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 1024,
        },
    }

    try:
        resp = httpx.post(
            endpoint,
            json=payload,
            headers={
                "Authorization": f"Bearer {credentials.token}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()

        # Extract the text response
        text = (
            data.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "{}")
        )

        # Clean up – Gemini sometimes wraps in ```json ... ```
        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1]
        if text.endswith("```"):
            text = text.rsplit("```", 1)[0]
        text = text.strip()

        parsed = json.loads(text)
        logger.info("Gemini parsed %d non-empty fields", sum(1 for v in parsed.values() if v))
        return {k: str(v) if v else "" for k, v in parsed.items() if k in _FIELDS}

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
