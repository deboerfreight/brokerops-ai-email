"""
BrokerOps AI – CarrierOK API integration for authority + insurance verification.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import httpx

from app.config import get_settings
from app.google_auth import _get_secret

logger = logging.getLogger("brokerops.carrierok")


def _api_key() -> str:
    return _get_secret(get_settings().CARRIEROK_API_KEY_SECRET_NAME)


def _base_url() -> str:
    return get_settings().CARRIEROK_API_BASE_URL.rstrip("/")


def verify_carrier(mc_number: str, dot_number: str = "") -> Optional[dict]:
    """
    Call CarrierOK API to fetch carrier authority, insurance, and coverage info.

    Returns a dict with normalized fields or None on failure:
        authority_status: str  (ACTIVE | INACTIVE | OUT_OF_SERVICE | NOT_FOUND)
        insurance_expiration: str (YYYY-MM-DD)
        auto_liability_coverage: int
        cargo_coverage: int
    """
    settings = get_settings()
    params: dict[str, str] = {}
    if mc_number:
        params["mc"] = mc_number
    if dot_number:
        params["dot"] = dot_number

    try:
        resp = httpx.get(
            f"{_base_url()}/carriers/verify",
            params=params,
            headers={"Authorization": f"Bearer {_api_key()}"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            logger.warning("CarrierOK: carrier not found mc=%s dot=%s", mc_number, dot_number)
            return {"authority_status": "NOT_FOUND", "insurance_expiration": "", "auto_liability_coverage": 0, "cargo_coverage": 0}
        logger.error("CarrierOK API error: %s", exc)
        return None
    except Exception as exc:
        logger.error("CarrierOK request failed: %s", exc)
        return None

    # Normalize the response – exact field names depend on CarrierOK's schema;
    # we map the most common patterns.
    authority_raw = (
        data.get("authority_status")
        or data.get("authorityStatus")
        or data.get("status", "")
    ).upper()
    status_map = {
        "AUTHORIZED": "ACTIVE",
        "ACTIVE": "ACTIVE",
        "INACTIVE": "INACTIVE",
        "NOT AUTHORIZED": "INACTIVE",
        "OUT OF SERVICE": "OUT_OF_SERVICE",
        "REVOKED": "INACTIVE",
    }
    authority_status = status_map.get(authority_raw, "NOT_FOUND")

    ins_exp_raw = data.get("insurance_expiration") or data.get("insuranceExpiration") or ""
    auto_liab = int(data.get("auto_liability_coverage") or data.get("autoLiabilityCoverage") or 0)
    cargo = int(data.get("cargo_coverage") or data.get("cargoCoverage") or 0)

    return {
        "authority_status": authority_status,
        "insurance_expiration": ins_exp_raw[:10] if ins_exp_raw else "",
        "auto_liability_coverage": auto_liab,
        "cargo_coverage": cargo,
    }


def derive_compliance_status(
    authority_status: str,
    insurance_expiration: str,
    auto_liability: int,
    cargo: int,
    w9_on_file: bool,
) -> str:
    """
    Derive Compliance_Status based on carrier data.
    Returns one of: CLEAR, INSURANCE_EXPIRED, INSURANCE_NOT_FOUND,
                     AUTHORITY_INACTIVE, PENDING_REVIEW, BLOCKED.
    """
    settings = get_settings()
    if authority_status != "ACTIVE":
        return "AUTHORITY_INACTIVE"
    if not insurance_expiration:
        return "INSURANCE_NOT_FOUND"
    try:
        exp_date = date.fromisoformat(insurance_expiration)
    except ValueError:
        return "INSURANCE_NOT_FOUND"
    if exp_date < date.today():
        return "INSURANCE_EXPIRED"
    if auto_liability < settings.MIN_AUTO_LIABILITY or cargo < settings.MIN_CARGO_COVERAGE:
        return "PENDING_REVIEW"
    # W9 not required for compliance status itself (handled separately in dispatch eligibility)
    return "CLEAR"
