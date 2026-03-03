"""
BrokerOps AI – Google authentication helpers.

Strategy
--------
1. **Primary (recommended)**: OAuth2 web-flow with refresh-token stored in
   Secret Manager.  The FastAPI app exposes ``/oauth/start`` and
   ``/oauth/callback`` to bootstrap the token once; after that the stored
   refresh token is used automatically.
2. **Fallback for local dev**: If a ``token.json`` file exists in the working
   directory it will be loaded directly (never committed to Git).

Secrets
-------
* ``OAUTH_CLIENT_SECRET_NAME`` – full OAuth client JSON
* ``OAUTH_REFRESH_TOKEN_SECRET_NAME`` – the refresh token string

Both are read from Google Secret Manager.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.cloud import secretmanager
from googleapiclient.discovery import build, Resource

from app.config import get_settings

logger = logging.getLogger("brokerops.auth")

_credentials: Optional[Credentials] = None
_services: dict[str, Resource] = {}


# ── Secret Manager helpers ──────────────────────────────────────────────────

def _sm_client() -> secretmanager.SecretManagerServiceClient:
    return secretmanager.SecretManagerServiceClient()


def _get_secret(secret_name: str, version: str = "latest") -> str:
    """Fetch a secret payload from Secret Manager."""
    settings = get_settings()
    name = f"projects/{settings.GCP_PROJECT_ID}/secrets/{secret_name}/versions/{version}"
    resp = _sm_client().access_secret_version(request={"name": name})
    return resp.payload.data.decode("utf-8")


def _get_oauth_client_config() -> dict:
    raw = _get_secret(get_settings().OAUTH_CLIENT_SECRET_NAME)
    return json.loads(raw)


def _get_refresh_token() -> Optional[str]:
    try:
        return _get_secret(get_settings().OAUTH_REFRESH_TOKEN_SECRET_NAME)
    except Exception:
        return None


def _store_refresh_token(token: str) -> None:
    """Store (or update) the refresh token in Secret Manager."""
    settings = get_settings()
    client = _sm_client()
    parent = f"projects/{settings.GCP_PROJECT_ID}/secrets/{settings.OAUTH_REFRESH_TOKEN_SECRET_NAME}"
    client.add_secret_version(
        request={"parent": parent, "payload": {"data": token.encode("utf-8")}}
    )
    logger.info("Stored new refresh token in Secret Manager.")


# ── Credential bootstrap ────────────────────────────────────────────────────

def _load_local_token() -> Optional[Credentials]:
    """Try loading token.json from CWD (local dev only)."""
    p = Path("token.json")
    if p.exists():
        creds = Credentials.from_authorized_user_file(str(p), get_settings().OAUTH_SCOPES)
        logger.info("Loaded credentials from local token.json")
        return creds
    return None


def get_credentials() -> Credentials:
    """Return valid Google OAuth2 credentials, refreshing if needed."""
    global _credentials
    if _credentials and _credentials.valid:
        return _credentials

    settings = get_settings()

    # 1. Try local dev file
    creds = _load_local_token()

    # 2. Try Secret Manager refresh token
    if creds is None:
        refresh_token = _get_refresh_token()
        if refresh_token:
            client_config = _get_oauth_client_config()
            web = client_config.get("web", client_config.get("installed", {}))
            creds = Credentials(
                token=None,
                refresh_token=refresh_token,
                token_uri=web["token_uri"],
                client_id=web["client_id"],
                client_secret=web["client_secret"],
                scopes=settings.OAUTH_SCOPES,
            )

    if creds is None:
        raise RuntimeError(
            "No credentials available. Run /oauth/start to bootstrap, "
            "or place a token.json in the working directory for local dev."
        )

    # Refresh if expired
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        logger.info("Refreshed OAuth token.")

    _credentials = creds
    return _credentials


# ── OAuth web-flow helpers (used by FastAPI routes) ─────────────────────────

def build_oauth_flow(redirect_uri: str) -> Flow:
    client_config = _get_oauth_client_config()
    flow = Flow.from_client_config(
        client_config,
        scopes=get_settings().OAUTH_SCOPES,
        redirect_uri=redirect_uri,
    )
    return flow


def exchange_code(code: str, redirect_uri: str) -> Credentials:
    """Exchange authorization code for credentials; persist refresh token."""
    flow = build_oauth_flow(redirect_uri)
    flow.fetch_token(code=code)
    creds = flow.credentials
    if creds.refresh_token:
        _store_refresh_token(creds.refresh_token)
    global _credentials
    _credentials = creds
    return creds


# ── Service builders ────────────────────────────────────────────────────────

def get_gmail_service() -> Resource:
    if "gmail" not in _services:
        _services["gmail"] = build("gmail", "v1", credentials=get_credentials())
    return _services["gmail"]


def get_sheets_service() -> Resource:
    if "sheets" not in _services:
        _services["sheets"] = build("sheets", "v4", credentials=get_credentials())
    return _services["sheets"]


def get_drive_service() -> Resource:
    if "drive" not in _services:
        _services["drive"] = build("drive", "v3", credentials=get_credentials())
    return _services["drive"]


def reset_services() -> None:
    """Clear cached services (useful after credential refresh)."""
    global _credentials
    _credentials = None
    _services.clear()
