"""
BrokerOps AI – Configuration via environment variables.
All Google resource IDs and secret references are configured here.
"""
from __future__ import annotations

import os
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # ── GCP ──────────────────────────────────────────────────────
    GCP_PROJECT_ID: str = ""
    GCP_REGION: str = "us-central1"

    # ── Secret Manager secret names (not the values themselves) ──
    OAUTH_CLIENT_SECRET_NAME: str = "brokerops-oauth-client"
    OAUTH_REFRESH_TOKEN_SECRET_NAME: str = "brokerops-oauth-refresh-token"
    CARRIEROK_API_KEY_SECRET_NAME: str = "brokerops-carrierok-api-key"

    # ── Google Sheets IDs ────────────────────────────────────────
    CARRIER_MASTER_SHEET_ID: str = ""
    LOAD_MASTER_SHEET_ID: str = ""
    MDL_VENDOR_SHEET_ID: str = ""

    # ── Google Drive folder IDs ──────────────────────────────────
    BROKEROPS_ROOT_FOLDER_ID: str = ""
    LOADS_FOLDER_ID: str = ""
    CARRIERS_FOLDER_ID: str = ""
    TEMPLATES_FOLDER_ID: str = ""

    # ── Google Doc template ID ───────────────────────────────────
    RATE_CONFIRMATION_TEMPLATE_ID: str = ""

    # ── FMCSA ────────────────────────────────────────────────────
    FMCSA_API_KEY: str = ""

    # ── CarrierOK API ────────────────────────────────────────────
    CARRIEROK_API_BASE_URL: str = "https://api.carrierok.com/v1"

    # ── Gmail ────────────────────────────────────────────────────
    BROKER_EMAIL: str = ""                # the Gmail address used for ops
    GMAIL_USER_ID: str = "me"

    # ── Apollo.io ────────────────────────────────────────────────
    APOLLO_API_KEY: str = ""

    # ── Google Custom Search Engine (dormant — dropped 2026-04-15, CSE deprecated) ──
    GOOGLE_CSE_API_KEY: str = ""
    GOOGLE_CSE_CX: str = ""

    # ── Brave Search API ─────────────────────────────────────────
    BRAVE_SEARCH_API_KEY: str = ""

    # ── Google Maps (Directions API for route mileage in quotes) ─
    GOOGLE_MAPS_API_KEY: str = ""

    # ── Slack notifications (replaces _notify_slack stub) ────────
    # Webhook URL from https://api.slack.com/apps — posts to the
    # channel configured in the Slack app. Blank = fall back to
    # logger-only (the original stub behavior).
    SLACK_WEBHOOK_URL: str = ""

    # ── Polling / behaviour ──────────────────────────────────────
    RFQ_BATCH_SIZE: int = 5
    RFQ_EXPANSION_DELAY_SECONDS: int = 7200   # 2 hours
    # MIN_AUTO_LIABILITY / MIN_CARGO_COVERAGE removed 2026-04-14 — duplicated
    # the canonical thresholds in app/vetting/rules.py::RULES. Every caller now
    # imports `from app.vetting.rules import RULES` and reads
    # `RULES.liability_min` / `RULES.cargo_min` directly.

    # ── Auto-reply kill switch ───────────────────────────────────
    # Master feature flag. When False, Sofia does not auto-reply to
    # carrier outreach replies, and Nina does not auto-ack inbound
    # shipper RFQs. Classification/logging still happens; sends are
    # skipped. Set to True only when ready to re-enable autonomous
    # email responses. Default: OFF. Flipped to False by Derek 2026-04-14
    # as part of the vetting rebuild pause. Does NOT affect the MDL
    # vendor first-touch (that path is gated by an explicit per-row
    # checkbox, not this flag).
    OUTREACH_AUTO_REPLY_ENABLED: bool = False

    # ── OAuth scopes required ────────────────────────────────────
    OAUTH_SCOPES: list[str] = [
        "https://www.googleapis.com/auth/gmail.modify",
        "https://www.googleapis.com/auth/gmail.send",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]

    # ── Twilio ──────────────────────────────────────────────────
    TWILIO_ACCOUNT_SID: str = ""
    TWILIO_AUTH_TOKEN: str = ""
    TWILIO_PHONE_NUMBER: str = ""
    TWILIO_PHONE_NUMBER_DIRECT: str = ""

    # ── Retell AI ────────────────────────────────────────────────
    RETELL_API_KEY: str = ""

    # ── Cloud Run service URL (for OAuth redirect) ───────────────
    SERVICE_URL: str = "http://localhost:8000"

    # ── Processed-message store sheet name ───────────────────────
    PROCESSED_STORE_SHEET: str = "Processed"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
