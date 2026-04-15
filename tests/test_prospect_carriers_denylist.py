"""
Tests for EXCLUDED_SERVICE_TYPE_PATTERNS denylist in scripts/prospect_carriers.py.

Focused regression for Amendment 3: coach(es) plural fix + existing patterns.
"""
from __future__ import annotations

import pytest
from scripts.prospect_carriers import EXCLUDED_SERVICE_TYPE_PATTERNS


def _matches(name: str) -> bool:
    return bool(EXCLUDED_SERVICE_TYPE_PATTERNS.search(name))


class TestDenylistCoachFix:
    """Amendment 3: 'coaches' plural must now match."""

    def test_a_family_coaches_matches(self):
        """Regression: 'A Family Coaches' was leaking through before the fix."""
        assert _matches("A Family Coaches")

    def test_coach_singular_matches(self):
        assert _matches("Sunshine Coach Transport")

    def test_motorcoach_matches(self):
        assert _matches("Premier Motorcoach Services")

    def test_motorcoaches_plural_matches(self):
        assert _matches("FL Motorcoaches Inc")


class TestDenylistExistingPatterns:
    """Sanity-check other critical exclusion patterns still fire correctly."""

    def test_towing_matches(self):
        assert _matches("Bob's Towing LLC")

    def test_tow_matches(self):
        assert _matches("Tow Bros LLC")

    def test_passenger_matches(self):
        assert _matches("Passenger Transit Co")

    def test_excavating_matches(self):
        assert _matches("Coastal Excavating")

    def test_excavation_matches(self):
        assert _matches("Sunbelt Excavation Group")

    def test_logging_matches(self):
        assert _matches("Southern Logging LLC")

    def test_waste_matches(self):
        assert _matches("Metro Waste Haulers")

    def test_garbage_matches(self):
        assert _matches("City Garbage Service")

    def test_shuttle_matches(self):
        assert _matches("Airport Shuttle Express")

    def test_moving_matches(self):
        assert _matches("Two Men Moving Co")

    def test_van_lines_matches(self):
        assert _matches("Allied Van Lines")


class TestDenylistFalsePositives:
    """Legitimate freight carriers that must NOT match."""

    def test_dry_van_carrier_no_match(self):
        assert not _matches("Sunstate Freight LLC")

    def test_flatbed_carrier_no_match(self):
        assert not _matches("Gulf Coast Flatbed Transport")

    def test_heavy_haul_no_match(self):
        assert not _matches("Southern Heavy Haul")

    def test_auto_transport_no_match(self):
        assert not _matches("AutoCarrier Express")


class TestDenylistTankerPatterns:
    """Tanker / bulk liquid exclusion patterns added 2026-04-15."""

    def test_volume_tank_lines_matches(self):
        """'Volume Tank Lines INC' — actual carrier dropped from Manley batch."""
        assert _matches("Volume Tank Lines INC")

    def test_xyz_tanker_services_matches(self):
        assert _matches("XYZ Tanker Services")

    def test_bulk_liquid_carriers_matches(self):
        assert _matches("Bulk Liquid Carriers Inc")

    def test_tank_creek_trucking_no_match(self):
        """'Tank Creek Trucking' — location name, NOT a tanker operator."""
        assert not _matches("Tank Creek Trucking")

    def test_palmetto_propane_fuels_no_match(self):
        """Fuel/propane carriers handled by Service Type tag, not name denylist."""
        assert not _matches("Palmetto Propane Fuels")
