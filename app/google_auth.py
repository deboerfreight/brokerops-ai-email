"""
BrokerOps AI – Google authentication helpers.

Auth modes (controlled by GMAIL_AUTH_MODE env var)
---------------------------------------------------
1. ``user`` (default / local dev):
   - Tries token.json from CWD first.
   - Falls back to GCP Secret Manager refresh token (existing production path).
   - Requires interactive OAuth bootstrap via /oauth/start the first time.

2. ``service_account`` (Cloud Run / headless) — WORKLOAD IDENTITY (2026-04-15):
   - SA key creation is blocked by org policy (constraints/iam.disableServiceAccountKeyCreation).
   - Uses Workload Identity: Cloud Run's metadata server mints short-lived tokens on demand.
   - No key file ever exists; nothing in the vault; no rotation needed.
   - google.auth.default() picks up the Cloud Run metadata server credentials automatically.
   - Uses google.auth.impersonated_credentials.Credentials + .with_subject() for
     domain-wide delegation to GMAIL_DELEGATE_EMAIL (default: sales@deboerfreight.com).
   - Requires domain-wide delegation granted by Derek in Google Workspace Admin.
   - No interactive step; works fully headless.

Set GMAIL_AUTH_MODE=service_account in Cloud Run environment to activate
the headless path.  Local dev continues to use GMAIL_AUTH_MODE=user (default).

IMPORTANT: Do NOT test service_account mode until Derek has granted domain-wide
delegation in Google Workspace Admin Console.  See docs/cloud_run_audit_20260415.md
section 3 for exact instructions.

Secrets
-------
user mode:
  * OAUTH_CLIENT_SECRET_NAME – full OAuth client JSON (Secret Manager)
  * OAUTH_REFRESH_TOKEN_SECRET_NAME – the refresh token string (Secret Manager)

service_account mode (Workload Identity — no key file):
  * GMAIL_DELEGATE_EMAIL – Gmail address to impersonate (default: sales@deboerfreight.com)
  * No GOOGLE_SERVICE_ACCOUNT_JSON needed or expected — DO NOT look for it.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional

from google.auth import default as google_auth_default
from google.auth import impersonated_credentials
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.cloud import secretmanager
from googleapiclient.discovery import build, Resource

from app.config import get_settings

logger = logging.getLogger("brokerops.auth")

_credentials: Optional[Credentials] = None
_services: dict[str, Resource] = {}

# ---------------------------------------------------------------------------
# Service account (Workload Identity + domain-wide delegation) path
# ---------------------------------------------------------------------------
# Pivot 2026-04-15: org policy blocks SA key creation
# (constraints/iam.disableServiceAccountKeyCreation).  No key file exists
# anywhere — not on disk, not in the vault.  Cloud Run's metadata server
# mints short-lived tokens for the attached SA (brokerops-gmail) on demand.
# google.auth.default() picks up those metadata-server credentials; we then
# use impersonated_credentials.Credentials + .with_subject() to get
# user-delegated tokens via domain-wide delegation.
# ---------------------------------------------------------------------------

SA_EMAIL = "brokerops-gmail@wide-decoder-489023-p1.iam.gserviceaccount.com"


def _get_service_account_credentials() -> impersonated_credentials.Credentials:
    """
    Build delegated credentials using Workload Identity (Cloud Run metadata server).

    Does NOT read a key file.  Does NOT look in the vault.
    Calls google.auth.default() to obtain the Cloud Run SA's metadata-server
    credentials, then wraps them with impersonated_credentials.Credentials and
    calls .with_subject() for domain-wide delegation to GMAIL_DELEGATE_EMAIL.

    Prerequisites:
      1. Cloud Run service must have brokerops-gmail SA attached as runtime identity.
      2. Domain-wide delegation must be granted in Google Workspace Admin Console
         for the service account's Client ID against the required Gmail/Drive scopes.

    Do NOT call this until Derek has completed the domain-wide delegation setup.
    """
    settings = get_settings()
    delegate_email = os.environ.get("GMAIL_DELEGATE_EMAIL", "sales@deboerfreight.com")

    # On Cloud Run, google.auth.default() returns Compute Engine credentials
    # backed by the metadata server — no key file needed.
    source_credentials, _ = google_auth_default()

    # Wrap with impersonated_credentials.Credentials to get user-delegated tokens
    # via domain-wide delegation.  subject= kwarg added to this constructor in
    # google-auth >=2.38.0 (confirmed present in 2.49.1, absent in 2.37.0).
    # requirements.txt pins 2.49.1 for this reason.
    delegated = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=SA_EMAIL,
        target_scopes=settings.OAUTH_SCOPES,
        delegates=[],
        subject=delegate_email,
        lifetime=3600,
    )

    logger.info(
        "Workload Identity credentials built for delegation to %s via SA %s",
        delegate_email,
        SA_EMAIL,
    )
    return delegated


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
    """
    Return valid Google credentials.

    Dispatches based on GMAIL_AUTH_MODE env var:
      - "service_account": headless path via domain-wide delegation (Cloud Run)
      - "user" or unset: OAuth2 user credentials (local dev + existing prod path)
    """
    global _credentials
    if _credentials and _credentials.valid:
        return _credentials

    auth_mode = os.environ.get("GMAIL_AUTH_MODE", "user").lower()

    if auth_mode == "service_account":
        logger.info("Auth mode: service_account (domain-wide delegation)")
        _credentials = _get_service_account_credentials()
        return _credentials

    # user mode — original path preserved exactly
    logger.info("Auth mode: user (OAuth2)")
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
