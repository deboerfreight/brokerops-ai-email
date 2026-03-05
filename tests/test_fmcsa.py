"""Tests for FMCSA integration and carrier scoring."""
from __future__ import annotations

import pytest
from app.fmcsa import score_carrier, _normalize_authority_status, _normalize_safety_rating, _detect_equipment


class TestScoreCarrier:
    def _base_carrier(self, **overrides) -> dict:
        base = {
            "Authority_Status": "ACTIVE",
            "Authority_Date": "2020-01-01",
            "Insurance_Liability": 1_000_000,
            "Insurance_Cargo": 100_000,
            "Safety_Rating": "SATISFACTORY",
            "Vehicle_OOS_Rate": 5.0,
            "Driver_OOS_Rate": 3.0,
            "Power_Units": 10,
            "OOS_Active": False,
            "_raw": {"complaintCount": 0},
        }
        base.update(overrides)
        return base

    def test_perfect_carrier(self):
        c = self._base_carrier(
            Authority_Date="2018-01-01",
            Insurance_Liability=2_000_000,
            Insurance_Cargo=250_000,
            Power_Units=60,
        )
        score = score_carrier(c)
        assert score == 100  # 25+20+10+20+15+10

    def test_hard_disqualify_inactive(self):
        c = self._base_carrier(Authority_Status="REVOKED")
        assert score_carrier(c) == -1

    def test_hard_disqualify_low_insurance(self):
        c = self._base_carrier(Insurance_Liability=500_000)
        assert score_carrier(c) == -1

    def test_hard_disqualify_low_cargo(self):
        c = self._base_carrier(Insurance_Cargo=50_000)
        assert score_carrier(c) == -1

    def test_hard_disqualify_unsatisfactory(self):
        c = self._base_carrier(Safety_Rating="UNSATISFACTORY")
        assert score_carrier(c) == -1

    def test_hard_disqualify_oos_active(self):
        c = self._base_carrier(OOS_Active=True)
        assert score_carrier(c) == -1

    def test_minimum_viable_carrier(self):
        """Young authority, min insurance, no rating, 1 truck, some complaints."""
        c = self._base_carrier(
            Authority_Date="2025-06-01",  # < 18 months
            Insurance_Liability=1_000_000,
            Insurance_Cargo=100_000,
            Safety_Rating="NONE",
            Power_Units=2,
            _raw={"complaintCount": 4},
        )
        score = score_carrier(c)
        # 0 (auth) + 15 (liab) + 7 (cargo) + 12 (no rating) + 5 (fleet) + 3 (complaints) = 42
        assert score == 42

    def test_high_oos_penalty(self):
        c = self._base_carrier(Vehicle_OOS_Rate=35.0, Driver_OOS_Rate=25.0)
        base_score = score_carrier(self._base_carrier())
        penalized = score_carrier(c)
        assert penalized == base_score - 15  # -10 for veh, -5 for driver


class TestNormalizeAuthority:
    def test_authorized(self):
        assert _normalize_authority_status("AUTHORIZED") == "ACTIVE"

    def test_active(self):
        assert _normalize_authority_status("ACTIVE") == "ACTIVE"

    def test_revoked(self):
        assert _normalize_authority_status("REVOKED") == "REVOKED"

    def test_empty(self):
        assert _normalize_authority_status("") == "UNKNOWN"


class TestNormalizeSafety:
    def test_satisfactory(self):
        assert _normalize_safety_rating("S") == "SATISFACTORY"

    def test_conditional(self):
        assert _normalize_safety_rating("CONDITIONAL") == "CONDITIONAL"

    def test_none(self):
        assert _normalize_safety_rating("") == "NONE"


class TestDetectEquipment:
    def test_general_freight(self):
        types = _detect_equipment({"cargoCarried": "General Freight"})
        assert "DRY_VAN" in types

    def test_refrigerated(self):
        types = _detect_equipment({"cargoCarried": "Refrigerated Food"})
        assert "REEFER" in types

    def test_flatbed_indicators(self):
        types = _detect_equipment({"cargoCarried": "Building Material, Machinery"})
        assert "FLATBED" in types

    def test_default_dry_van(self):
        types = _detect_equipment({})
        assert "DRY_VAN" in types
