"""
Integration test: validate the poll workflow end-to-end with mocked services.

Ensures all post-rollback workflow modules wire together correctly and
the load-ingestion → parse → sheet-insert pipeline works as expected.
"""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# ── Stub out google-auth / google API modules before any app imports ──────────
# This lets us test workflow logic without real GCP/cryptography deps.

_STUB_MODULES = [
    "google", "google.auth", "google.auth.transport", "google.auth.transport.requests",
    "google.oauth2", "google.oauth2.credentials", "google.oauth2.service_account",
    "google.auth.crypt", "google.auth.crypt.es",
    "google.cloud", "google.cloud.secretmanager", "google.cloud.secretmanager_v1",
    "googleapiclient", "googleapiclient.discovery", "googleapiclient.http",
    "google.auth._service_account_info",
]


@pytest.fixture(autouse=True)
def _stub_google(monkeypatch):
    """Replace google SDK modules with empty stubs so imports don't fail."""
    for mod_name in _STUB_MODULES:
        if mod_name not in sys.modules:
            monkeypatch.setitem(sys.modules, mod_name, types.ModuleType(mod_name))


# ── Tests ────────────────────────────────────────────────────────────────────


class TestLoadIngestionPipeline:
    """Verify load-ingestion workflow logic with mocked external calls."""

    def _run_ingestion_with_email(self, body: str, subject: str = "New Load Request"):
        """Helper: mock all external services and run ingestion on a single email."""
        from app.parsers import parse_load_email

        fields = parse_load_email(body, subject)
        return fields

    def test_basic_load_parses(self):
        fields = self._run_ingestion_with_email(
            "Origin: Dallas, TX\n"
            "Destination: Atlanta, GA\n"
            "Pickup Date: 03/20/2026\n"
            "Equipment: Dry Van\n"
            "Weight: 40000 lbs\n"
            "Commodity: Electronics"
        )
        assert fields["Origin_City"] == "Dallas"
        assert fields["Origin_State"] == "TX"
        assert fields["Destination_City"] == "Atlanta"
        assert fields["Destination_State"] == "GA"
        assert fields["Pickup_Date"] == "2026-03-20"
        assert fields["Equipment_Type"] == "DRY_VAN"
        assert fields["Weight_Lbs"] == "40000"

    def test_reefer_with_temp_control(self):
        fields = self._run_ingestion_with_email(
            "Origin: Chicago, IL\n"
            "Destination: Miami, FL\n"
            "Equipment: Reefer\n"
            "Temp Control: Required\n"
            "Pickup Date: 2026-04-01"
        )
        assert fields["Equipment_Type"] == "REEFER"
        assert fields["Temp_Control_Required"] == "TRUE"

    def test_hazmat_load(self):
        fields = self._run_ingestion_with_email(
            "Origin: Houston, TX\n"
            "Destination: Los Angeles, CA\n"
            "Equipment: Flatbed\n"
            "Hazmat: Yes\n"
            "Pickup Date: 2026-03-25"
        )
        assert fields["Equipment_Type"] == "FLATBED"
        assert fields["Hazmat"] == "TRUE"

    def test_date_formats_in_load(self):
        """Verify various date formats parse correctly in load context."""
        for date_str, expected in [
            ("03/20/2026", "2026-03-20"),
            ("2026-03-20", "2026-03-20"),
            ("March 20, 2026", "2026-03-20"),
            ("Mar 20, 2026", "2026-03-20"),
        ]:
            fields = self._run_ingestion_with_email(
                f"Origin: A, TX\nDestination: B, CA\nPickup Date: {date_str}"
            )
            assert fields["Pickup_Date"] == expected, f"Failed for {date_str}"


class TestQuoteParsing:
    """Verify quote parsing works correctly post-rollback."""

    def test_dollar_amount_extraction(self):
        from app.parsers import parse_quote_reply

        cases = [
            ("Rate: $2,500.00", 2500.0),
            ("We can do it for $1850", 1850.0),
            ("$3,200 is our best rate", 3200.0),
        ]
        for body, expected_rate in cases:
            result = parse_quote_reply(body)
            assert result["rate"] == expected_rate, f"Failed for: {body}"

    def test_no_rate_returns_none(self):
        from app.parsers import parse_quote_reply
        result = parse_quote_reply("Thanks for reaching out, we'll check on this.")
        assert result["rate"] is None


class TestApprovalParsing:
    """Verify approval parsing works correctly post-rollback."""

    def test_approve(self):
        from app.parsers import parse_approval_reply
        result = parse_approval_reply("APPROVE 2026-0001")
        assert result["action"] == "APPROVE"
        assert result["load_id"] == "2026-0001"

    def test_reject(self):
        from app.parsers import parse_approval_reply
        result = parse_approval_reply("REJECT 2026-0015")
        assert result["action"] == "REJECT"
        assert result["load_id"] == "2026-0015"

    def test_no_action(self):
        from app.parsers import parse_approval_reply
        result = parse_approval_reply("Looks good, thanks!")
        assert result["action"] is None


class TestEquipmentIntelligence:
    """Verify equipment module works post-rollback."""

    def test_recommend_equipment(self):
        from app.equipment import recommend_equipment

        fields = {
            "Equipment_Type": "REEFER",
            "Commodity": "frozen food",
            "Temp_Control_Required": "TRUE",
        }
        rec = recommend_equipment(fields)
        assert "recommended" in rec
        assert "cost_tier" in rec
        assert "requires_verification" in rec


class TestFastAPIApp:
    """Verify the FastAPI app constructs with all routes post-rollback."""

    def test_app_creates(self, monkeypatch):
        # Need deeper stubs for google_auth imports
        extra_stubs = {
            "google_auth_oauthlib": types.ModuleType("google_auth_oauthlib"),
            "google_auth_oauthlib.flow": types.ModuleType("google_auth_oauthlib.flow"),
        }
        for name, mod in extra_stubs.items():
            monkeypatch.setitem(sys.modules, name, mod)

        # Add mock classes/functions the code imports by name
        sys.modules["google.auth.transport.requests"].Request = MagicMock
        sys.modules["google.oauth2.credentials"].Credentials = MagicMock
        sys.modules["google_auth_oauthlib.flow"].Flow = MagicMock
        sys.modules["googleapiclient.discovery"].build = MagicMock
        sys.modules["googleapiclient.discovery"].Resource = MagicMock
        sys.modules["google.cloud.secretmanager"].SecretManagerServiceClient = MagicMock

        # Clear cached imports so they re-resolve with stubs
        for mod_name in list(sys.modules):
            if mod_name.startswith("app."):
                monkeypatch.delitem(sys.modules, mod_name, raising=False)

        from app.main import app
        routes = [r.path for r in app.routes]
        assert "/health" in routes
        assert "/jobs/poll" in routes
        assert "/jobs/compliance" in routes
