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


def _call_gemini(prompt: str, max_tokens: int = 1024) -> str:
    """
    Send a prompt to Gemini via Vertex AI and return the raw text response.
    Uses Application Default Credentials (works automatically on Cloud Run).
    """
    settings = get_settings()
    project = settings.GCP_PROJECT_ID
    region = settings.GCP_REGION

    endpoint = (
        f"https://{region}-aiplatform.googleapis.com/v1/"
        f"projects/{project}/locations/{region}/"
        f"publishers/google/models/gemini-2.0-flash:generateContent"
    )

    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": max_tokens,
        },
    }

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
    return text.strip()


# ── Email classification ───────────────────────────────────────────────────

_CLASSIFY_PROMPT = """You are a freight brokerage email classifier for De Boer Freight.

This inbox receives emails primarily about freight loads and shipping. Your job is
to classify each email. When in doubt, lean toward NEW_LOAD — it's better to process
an email as a potential load than to miss one.

Categories:

1. NEW_LOAD — Any email that mentions moving freight, shipping, hauling, needing a
   truck/trailer, quote requests from shippers, load tenders, rate requests, or
   anything involving an origin, destination, commodity, or equipment type. Even
   casual or brief messages like "need a flatbed Dallas to Memphis" count. This is
   the DEFAULT category — use it unless another category clearly fits better.

2. CARRIER_QUOTE — ONLY use this when a motor carrier is replying to an RFQ that
   WE sent them. Key indicators: references to a Load_ID (format YYYY-####), "RFQ",
   "Re: RFQ", or the carrier quoting a rate in response to our outreach. The email
   must clearly be a RESPONSE to something we sent.

3. LOAD_UPDATE — A follow-up to an existing load: updated pickup times,
   address corrections, added details, weight changes, etc. Must reference
   an existing Load_ID or explicitly say "update", "correction", "revised".

4. OTHER — ONLY use this when the email has absolutely nothing to do with freight,
   shipping, or logistics. Examples: marketing spam, personal emails, IT notifications,
   invoice requests with no load context.

IMPORTANT: If the email mentions ANY freight/shipping concepts (cities, equipment,
weight, pickup, delivery, commodity, rate, truck, trailer, haul, ship, load, freight),
classify as NEW_LOAD.

Return ONLY a JSON object with these fields:
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
        result = json.loads(text)
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
    full_text = f"Subject: {subject}\n\n{email_body}" if subject else email_body
    prompt = f"{_SYSTEM_PROMPT}\n\nEmail:\n---\n{full_text}\n---\n\nJSON:"

    try:
        text = _call_gemini(prompt)
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
