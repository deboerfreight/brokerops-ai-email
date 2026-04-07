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

    # ── Google Custom Search Engine ──────────────────────────────
    GOOGLE_CSE_API_KEY: str = ""
    GOOGLE_CSE_CX: str = ""

    # ── Polling / behaviour ──────────────────────────────────────
    RFQ_BATCH_SIZE: int = 5
    RFQ_EXPANSION_DELAY_SECONDS: int = 7200   # 2 hours
    MIN_AUTO_LIABILITY: int = 1_000_000
    MIN_CARGO_COVERAGE: int = 100_000

    # ── OAuth scopes required ────────────────────────────────────
    OAUTH_SCOPES: list[str] = [
        "https://www.googleapis.com/auth/gmail.modify",
        "https://www.googleapis.com/auth/gmail.send",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]

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
