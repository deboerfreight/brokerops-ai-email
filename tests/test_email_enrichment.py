"""Tests for the email enrichment pipeline."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.email_enrichment import (
    _extract_emails_from_text,
    _pick_best_email,
    enrich_carrier_email,
)


class TestPickBestEmail:
    def test_dispatch_wins(self):
        emails = ["info@acme.com", "dispatch@acme.com", "admin@acme.com"]
        assert _pick_best_email(emails) == "dispatch@acme.com"

    def test_freight_over_info(self):
        emails = ["info@acme.com", "freight@acme.com"]
        assert _pick_best_email(emails) == "freight@acme.com"

    def test_filters_noreply(self):
        emails = ["noreply@acme.com", "dispatch@acme.com"]
        assert _pick_best_email(emails) == "dispatch@acme.com"

    def test_all_noreply_returns_none(self):
        assert _pick_best_email(["noreply@acme.com", "no-reply@x.com"]) is None

    def test_empty_returns_none(self):
        assert _pick_best_email([]) is None

    def test_single_email(self):
        assert _pick_best_email(["bob@acme.com"]) == "bob@acme.com"

    def test_deduplicates(self):
        emails = ["Dispatch@ACME.com", "dispatch@acme.com"]
        assert _pick_best_email(emails) == "dispatch@acme.com"


class TestExtractEmails:
    def test_basic(self):
        text = "Contact us at dispatch@acme.com or call 555-1234"
        assert _extract_emails_from_text(text) == ["dispatch@acme.com"]

    def test_multiple(self):
        text = "Email dispatch@acme.com or info@acme.com"
        result = _extract_emails_from_text(text)
        assert "dispatch@acme.com" in result
        assert "info@acme.com" in result

    def test_no_emails(self):
        assert _extract_emails_from_text("No emails here!") == []


class TestEnrichCarrierEmail:
    @patch("app.email_enrichment._scrape_safer_website")
    @patch("app.email_enrichment._scrape_website_for_email")
    def test_safer_success_stops_pipeline(self, mock_scrape, mock_safer):
        mock_safer.return_value = "https://acmetrucking.com"
        mock_scrape.return_value = ["dispatch@acmetrucking.com", "info@acmetrucking.com"]

        carrier = {"DOT_Number": "12345", "MC_Number": "67890",
                    "Legal_Name": "Acme Trucking", "City": "Miami", "State": "FL"}
        result = enrich_carrier_email(carrier)

        assert result["email"] == "dispatch@acmetrucking.com"
        assert result["source"] == "SAFER_WEBSITE"
        assert result["website"] == "https://acmetrucking.com"

    @patch("app.email_enrichment._scrape_safer_website", return_value=None)
    @patch("app.email_enrichment._google_search_email")
    def test_falls_through_to_google(self, mock_google, mock_safer):
        mock_google.return_value = ["freight@bigrig.com"]

        carrier = {"DOT_Number": "12345", "MC_Number": "67890",
                    "Legal_Name": "Big Rig LLC", "City": "Dallas", "State": "TX"}
        result = enrich_carrier_email(carrier)

        assert result["email"] == "freight@bigrig.com"
        assert result["source"] == "GOOGLE"

    @patch("app.email_enrichment._scrape_safer_website", return_value=None)
    @patch("app.email_enrichment._google_search_email", return_value=[])
    @patch("app.email_enrichment._apollo_lookup")
    def test_falls_through_to_apollo(self, mock_apollo, mock_google, mock_safer):
        mock_apollo.return_value = ("ops@fleet.com", "apollo-123")

        carrier = {"DOT_Number": "12345", "MC_Number": "67890",
                    "Legal_Name": "Fleet Co", "City": "Chicago", "State": "IL"}
        result = enrich_carrier_email(carrier)

        assert result["email"] == "ops@fleet.com"
        assert result["source"] == "APOLLO"
        assert result["apollo_id"] == "apollo-123"

    @patch("app.email_enrichment._scrape_safer_website", return_value=None)
    @patch("app.email_enrichment._google_search_email", return_value=[])
    @patch("app.email_enrichment._apollo_lookup", return_value=(None, None))
    def test_phone_only_fallback(self, mock_apollo, mock_google, mock_safer):
        carrier = {"DOT_Number": "12345", "MC_Number": "67890",
                    "Legal_Name": "Ghost Carrier", "City": "Nowhere", "State": "AK"}
        result = enrich_carrier_email(carrier)

        assert result["email"] is None
        assert result["source"] == "PHONE_ONLY"
