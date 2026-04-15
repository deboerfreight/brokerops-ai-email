"""Regression tests for the carrier vetting gates.

These tests pin the 3 non-negotiable hard-reject rules:
  1. Fleet size (Power_Units) >= 3
  2. Insurance Liability >= $1,000,000
  3. Insurance Cargo >= $100,000

and the canonical sheet-level vetting helper `is_carrier_vetted`.
"""
from __future__ import annotations

from app.fmcsa import vet_carrier_strict
from app.sheets import is_carrier_dispatch_eligible, is_carrier_vetted


# ─── vet_carrier_strict — insurance hard rejects ──────────────────────────────


def _base_clean_carrier() -> dict:
    return {
        "Legal_Name": "Clean Co",
        "Power_Units": 5,
        "Insurance_Liability": 1_000_000,
        "Insurance_Cargo": 100_000,
        "Vehicle_OOS_Rate": 10,
        "Driver_OOS_Rate": 5,
        "Crash_Rate_Per100": 5,
        "Driver_Count": 10,
        "Equipment_Types": "DRY_VAN",
    }


def test_vet_carrier_strict_rejects_low_liability():
    carrier = {
        "Legal_Name": "Test",
        "Power_Units": 10,
        "Insurance_Liability": 500_000,
        "Insurance_Cargo": 100_000,
    }
    passed, reason = vet_carrier_strict(carrier)
    assert passed is False
    assert "liability" in reason.lower() or "1m" in reason.lower()


def test_vet_carrier_strict_low_cargo_no_longer_rejects():
    """As of 2026-04-14, RULES.cargo_min is 0 because FMCSA does not publish
    cargo insurance filings for general-freight carriers. Low cargo values
    (e.g. $50K) must NOT trigger a hard reject on the cargo gate — cargo
    verification moved to onboarding via COI collection. See rules.py."""
    from app.vetting.rules import RULES
    assert RULES.cargo_min == 0, (
        "Test assumes cargo_min=0 per 2026-04-14 rule change. If cargo_min "
        "is ever raised, this test must be updated to match."
    )
    carrier = _base_clean_carrier()
    carrier["Insurance_Cargo"] = 50_000
    passed, reason = vet_carrier_strict(carrier)
    assert passed is True, f"expected pass, got: {reason}"


def test_vet_carrier_strict_rejects_low_fleet_size():
    carrier = {
        "Legal_Name": "Test",
        "Power_Units": 2,
        "Insurance_Liability": 1_000_000,
        "Insurance_Cargo": 100_000,
    }
    passed, reason = vet_carrier_strict(carrier)
    assert passed is False
    assert "fleet" in reason.lower() or "power units" in reason.lower()


def test_vet_carrier_strict_accepts_clean_carrier():
    passed, reason = vet_carrier_strict(_base_clean_carrier())
    assert passed is True
    assert reason == ""


def test_vet_carrier_strict_accepts_blank_insurance():
    """Blank/0 insurance is treated as needs_review, not hard-reject —
    matches the data-conditional behavior of score_carrier()."""
    carrier = _base_clean_carrier()
    carrier["Insurance_Liability"] = 0
    carrier["Insurance_Cargo"] = 0
    passed, reason = vet_carrier_strict(carrier)
    assert passed is True


# ─── is_carrier_dispatch_eligible — fleet size gate ───────────────────────────


def test_is_carrier_dispatch_eligible_rejects_2_truck_carrier():
    """A carrier that would otherwise pass dispatch eligibility must be
    rejected when fleet size is 2 (below the 3-truck minimum)."""
    carrier = {
        "Authority_Status": "ACTIVE",
        "Compliance_Status": "CLEAR",
        "Insurance_Expiration": "2099-12-31",
        "Auto_Liability_Coverage": "1000000",
        "Cargo_Coverage": "100000",
        "W9_On_File": "TRUE",
        "Active": "TRUE",
        "Power_Units": "2",
    }
    assert is_carrier_dispatch_eligible(carrier) is False


def test_is_carrier_dispatch_eligible_accepts_3_truck_carrier():
    carrier = {
        "Authority_Status": "ACTIVE",
        "Compliance_Status": "CLEAR",
        "Insurance_Expiration": "2099-12-31",
        "Auto_Liability_Coverage": "1000000",
        "Cargo_Coverage": "100000",
        "W9_On_File": "TRUE",
        "Active": "TRUE",
        "Power_Units": "3",
    }
    assert is_carrier_dispatch_eligible(carrier) is True


def test_is_carrier_dispatch_eligible_reads_fleet_size_header_alias():
    """Sheet rows from get_all_carriers carry the 'Fleet Size' header key
    in addition to the 'Power_Units' alias — verify either path works."""
    carrier = {
        "Authority_Status": "ACTIVE",
        "Compliance_Status": "CLEAR",
        "Insurance_Expiration": "2099-12-31",
        "Auto_Liability_Coverage": "1000000",
        "Cargo_Coverage": "100000",
        "W9_On_File": "TRUE",
        "Active": "TRUE",
        "Fleet Size": "1",
    }
    assert is_carrier_dispatch_eligible(carrier) is False


# ─── is_carrier_vetted — col AG canonical gate ────────────────────────────────


def test_is_carrier_vetted_reads_col_ag():
    assert is_carrier_vetted({"Vetting_Status": "pass_basic"}) is True
    assert is_carrier_vetted({"Vetting_Status": "fail_fleet_size"}) is False
    assert is_carrier_vetted({"Vetting_Status": ""}) is False
    assert is_carrier_vetted({}) is False


def test_is_carrier_vetted_reads_sheet_header_key():
    """The raw sheet-header key 'Vetting Status' (with space) must also
    be honored, since some callers may not have aliasing applied."""
    assert is_carrier_vetted({"Vetting Status": "pass_basic"}) is True
    assert is_carrier_vetted({"Vetting Status": "fail_insurance_liability"}) is False


def test_is_carrier_vetted_case_insensitive_and_trimmed():
    assert is_carrier_vetted({"Vetting_Status": "  PASS_BASIC  "}) is True
    assert is_carrier_vetted({"Vetting_Status": "Pass_Basic"}) is True
