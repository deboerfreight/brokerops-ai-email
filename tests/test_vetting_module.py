"""Unit tests for the new app.vetting module.

Covers:
  - vet_complete() against every rule
  - is_carrier_vetted() with cached + inline-compute paths
  - fetch_fresh_fmcsa() mocked
  - validate_before_write() split behavior
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from app.vetting.gate import (
    vet_complete,
    is_carrier_vetted,
    VettingResult,
    PASS_BASIC,
    NEEDS_REVIEW,
    FAIL_FLEET,
    FAIL_LIABILITY,
    FAIL_CARGO,
    FAIL_SAFETY,
    FAIL_VEHICLE_OOS,
    FAIL_DRIVER_OOS,
    FAIL_CRASH,
    FAIL_REEFER,
    FAIL_SHELL,
)
from app.vetting.rules import RULES
from app.vetting.writer import validate_before_write


def _clean() -> dict:
    return {
        "Power_Units": 5,
        "Insurance_Liability": 1_000_000,
        "Insurance_Cargo": 100_000,
        "Safety_Rating": "SATISFACTORY",
        "Vehicle_OOS_Rate": 10.0,
        "Driver_OOS_Rate": 5.0,
        "Crash_Rate_Per100": 5.0,
        "Driver_Count": 8,
        "Equipment_Types": "DRY_VAN",
    }


# ── vet_complete: pass ─────────────────────────────────────────────────────


def test_vet_complete_passes_clean_carrier():
    r = vet_complete(_clean())
    assert isinstance(r, VettingResult)
    assert r.passed is True
    assert r.status == PASS_BASIC


# ── vet_complete: needs_review on missing fields ──────────────────────────


def test_vet_complete_blank_fleet_is_needs_review():
    c = _clean()
    c["Power_Units"] = 0
    r = vet_complete(c)
    assert r.passed is False
    assert r.status == NEEDS_REVIEW


def test_vet_complete_blank_liability_is_needs_review():
    c = _clean()
    c["Insurance_Liability"] = 0
    r = vet_complete(c)
    assert r.passed is False
    assert r.status == NEEDS_REVIEW


def test_vet_complete_blank_cargo_needs_review_when_rule_active():
    """Blank cargo only trips needs_review when RULES.cargo_min > 0.
    Default is 0 (FMCSA doesn't publish cargo for general freight), so this
    test monkeypatches a stricter rule to exercise the branch."""
    from unittest.mock import patch
    from app.vetting.rules import VettingRules
    stricter = VettingRules(cargo_min=100_000)
    with patch("app.vetting.gate.RULES", stricter):
        c = _clean()
        c["Insurance_Cargo"] = 0
        r = vet_complete(c)
        assert r.passed is False
        assert r.status == NEEDS_REVIEW


def test_vet_complete_empty_dict_is_needs_review():
    r = vet_complete({})
    assert r.passed is False
    assert r.status == NEEDS_REVIEW


# ── vet_complete: hard rejects ────────────────────────────────────────────


def test_vet_complete_fail_fleet_size():
    c = _clean()
    c["Power_Units"] = 2
    r = vet_complete(c)
    assert r.status == FAIL_FLEET
    assert "2" in r.reason


def test_vet_complete_fail_liability():
    c = _clean()
    c["Insurance_Liability"] = 500_000
    r = vet_complete(c)
    assert r.status == FAIL_LIABILITY


def test_vet_complete_fail_cargo_when_rule_active():
    """Cargo check only fires when RULES.cargo_min > 0. Current default is 0
    because FMCSA doesn't publish cargo filings for general freight — see
    app/vetting/rules.py for the full rationale. This test verifies the
    threshold behavior by temporarily raising cargo_min via monkeypatch.
    """
    from unittest.mock import patch
    from app.vetting.rules import VettingRules
    stricter = VettingRules(cargo_min=100_000)
    with patch("app.vetting.gate.RULES", stricter):
        c = _clean()
        c["Insurance_Cargo"] = 50_000
        r = vet_complete(c)
        assert r.status == FAIL_CARGO


def test_vet_complete_blank_cargo_passes_when_rule_zero():
    """With default cargo_min=0, a blank/zero cargo value should NOT trip
    needs_review. Regression test against the hard-coded cargo==0 check bug
    fixed 2026-04-14."""
    c = _clean()
    c["Insurance_Cargo"] = 0
    r = vet_complete(c)
    assert r.passed is True
    assert r.status == "pass_basic"


def test_vet_complete_fail_safety_rating():
    c = _clean()
    c["Safety_Rating"] = "Unsatisfactory"
    r = vet_complete(c)
    assert r.status == FAIL_SAFETY


def test_vet_complete_fail_vehicle_oos():
    c = _clean()
    c["Vehicle_OOS_Rate"] = 35.0
    r = vet_complete(c)
    assert r.status == FAIL_VEHICLE_OOS


def test_vet_complete_fail_driver_oos():
    c = _clean()
    c["Driver_OOS_Rate"] = 16.0
    r = vet_complete(c)
    assert r.status == FAIL_DRIVER_OOS


def test_vet_complete_fail_crash_rate():
    c = _clean()
    c["Crash_Rate_Per100"] = 31.0
    r = vet_complete(c)
    assert r.status == FAIL_CRASH


def test_vet_complete_fail_reefer_over_rate_threshold():
    """Rule revised 2026-04-15: binary 'any OOS = reject' replaced with
    rate-based rule. Reefer at 15% vehicle OOS rate (above 10% threshold)
    should now reject with fail_reefer_maintenance."""
    c = _clean()
    c["Equipment_Types"] = "REEFER,DRY_VAN"
    c["Vehicle_Insp"] = 50   # enough inspections to trust the rate
    c["Vehicle_OOS_Rate"] = 15.0
    r = vet_complete(c)
    assert r.status == FAIL_REEFER


def test_vet_complete_reefer_below_threshold_passes():
    """Reefer at 8% OOS rate with 50 inspections passes under the new
    rate-based rule. Under the old binary rule this would have been
    rejected if any Vehicle_OOS_Insp was > 0."""
    c = _clean()
    c["Equipment_Types"] = "REEFER"
    c["Vehicle_Insp"] = 50
    c["Vehicle_OOS_Rate"] = 8.0
    c["Vehicle_OOS_Insp"] = 4   # 4 of 50 = 8% — under 10% reefer threshold
    r = vet_complete(c)
    assert r.status == PASS_BASIC


def test_vet_complete_fail_shell_carrier():
    c = _clean()
    c["Driver_Count"] = 0
    r = vet_complete(c)
    assert r.status == FAIL_SHELL


# ── tolerant key reading ──────────────────────────────────────────────────


def test_vet_complete_reads_sheet_header_keys():
    c = {
        "Fleet Size": "5",
        "Insurance Liability": "$1,000,000",
        "Insurance Cargo": "100000",
        "Safety Rating": "SATISFACTORY",
    }
    r = vet_complete(c)
    assert r.passed is True


def test_vet_complete_parses_money_with_dollar_signs_and_commas():
    c = _clean()
    c["Insurance_Liability"] = "$2,500,000"
    c["Insurance_Cargo"] = "$250,000"
    r = vet_complete(c)
    assert r.passed is True


# ── is_carrier_vetted: cached vs inline ───────────────────────────────────


def test_is_carrier_vetted_uses_cached_pass():
    assert is_carrier_vetted({"Vetting_Status": "pass_basic"}) is True


def test_is_carrier_vetted_uses_cached_fail():
    assert is_carrier_vetted({"Vetting_Status": "fail_fleet_size"}) is False


def test_is_carrier_vetted_inline_compute_when_no_status():
    assert is_carrier_vetted(_clean()) is True


def test_is_carrier_vetted_inline_compute_fails_for_bad_carrier():
    bad = _clean()
    bad["Power_Units"] = 1
    assert is_carrier_vetted(bad) is False


def test_is_carrier_vetted_empty_dict_is_false():
    assert is_carrier_vetted({}) is False


# ── fetch_fresh_fmcsa mocked ───────────────────────────────────────────────


def test_fetch_fresh_fmcsa_returns_normalized():
    fake = {
        "DOT_Number": "1234567",
        "Power_Units": 5,
        "Insurance_Liability": 1_000_000,
        "Insurance_Cargo": 100_000,
        "Safety_Rating": "SATISFACTORY",
    }
    with patch("app.vetting.data_sync.get_carrier_details", return_value=fake):
        from app.vetting.data_sync import fetch_fresh_fmcsa
        result = fetch_fresh_fmcsa("1234567")
    assert result == fake


def test_fetch_fresh_fmcsa_returns_none_on_error():
    with patch("app.vetting.data_sync.get_carrier_details",
               side_effect=Exception("network down")):
        from app.vetting.data_sync import fetch_fresh_fmcsa
        result = fetch_fresh_fmcsa("1234567")
    assert result is None


def test_fetch_fresh_fmcsa_empty_result_returns_none():
    with patch("app.vetting.data_sync.get_carrier_details", return_value=None):
        from app.vetting.data_sync import fetch_fresh_fmcsa
        assert fetch_fresh_fmcsa("9999999") is None


# ── validate_before_write split ───────────────────────────────────────────


def test_validate_before_write_splits_passes_and_quarantines():
    rows = [
        _clean(),
        {**_clean(), "Power_Units": 1},
        {**_clean(), "Insurance_Liability": 250_000},
        _clean(),
    ]
    passes, quarantines = validate_before_write(rows)
    assert len(passes) == 2
    assert len(quarantines) == 2
    statuses = sorted(r.status for _, r in quarantines)
    assert statuses == sorted([FAIL_FLEET, FAIL_LIABILITY])
    # All passes carry the stamped Vetting Status
    for p in passes:
        assert p["Vetting Status"] == PASS_BASIC


def test_validate_before_write_empty_input():
    passes, quarantines = validate_before_write([])
    assert passes == []
    assert quarantines == []


# ── RULES sanity check ────────────────────────────────────────────────────


def test_rules_thresholds_locked():
    assert RULES.fleet_min == 3
    assert RULES.liability_min == 1_000_000
    # cargo_min = 0 is intentional: FMCSA doesn't publish cargo filings for
    # general-freight carriers. Cargo verification moves to onboarding from COI.
    # See app/vetting/rules.py for the rationale. If you raise this above 0,
    # the blank-cargo needs_review branch in gate.py reactivates automatically.
    assert RULES.cargo_min == 0
    assert RULES.vehicle_oos_max_pct == 30.0
    assert RULES.driver_oos_max_pct == 15.0
    assert RULES.crash_rate_max_per_100 == 30.0
    # Reefer-specific rules (revised 2026-04-15). See rules.py for rationale.
    assert RULES.reefer_vehicle_oos_max_pct == 10.0
    assert RULES.reefer_min_inspection_count == 10


def _clean_reefer():
    """Reefer carrier with clean stats — 50 inspections, 8% OOS rate (below
    reefer 10% threshold). Expected to PASS under the new rate-based rule."""
    return {
        "Legal_Name": "Clean Reefer Co",
        "Power_Units": 20, "Driver_Count": 25,
        "Insurance_Liability": 1_000_000, "Insurance_Cargo": 100_000,
        "Safety_Rating": "Satisfactory",
        "Vehicle_OOS_Rate": 8.0, "Driver_OOS_Rate": 3.0,
        "Crash_Rate_Per100": 1.0,
        "Equipment_Types": "REEFER",
        "Vehicle_Insp": 50, "Vehicle_OOS_Insp": 4,
    }


def test_reefer_passes_under_threshold_with_enough_inspections():
    """New rate-based rule: reefer at 8% OOS and 50 inspections should pass.
    Under the OLD binary rule this would have been rejected (any OOS > 0)."""
    c = _clean_reefer()
    r = vet_complete(c)
    assert r.passed, f"expected pass, got {r.status}: {r.reason}"
    assert r.status == "pass_basic"


def test_reefer_rejected_over_threshold():
    """Reefer at 15% OOS rate exceeds the 10% reefer-specific threshold."""
    c = _clean_reefer()
    c["Vehicle_OOS_Rate"] = 15.0
    r = vet_complete(c)
    assert not r.passed
    assert r.status == "fail_reefer_maintenance"


def test_reefer_needs_review_under_inspection_floor():
    """Reefer with only 5 inspections is data-insufficient regardless of rate."""
    c = _clean_reefer()
    c["Vehicle_Insp"] = 5
    r = vet_complete(c)
    assert not r.passed
    assert r.status == "needs_review"
    assert "inspection" in r.reason.lower()


def test_non_reefer_not_subject_to_reefer_rule():
    """Dry van at 20% OOS should still pass (general-freight rule is 30%)."""
    c = _clean_reefer()
    c["Equipment_Types"] = "DRY_VAN"
    c["Vehicle_OOS_Rate"] = 20.0
    r = vet_complete(c)
    assert r.passed, f"dry van at 20% OOS should pass, got {r.status}: {r.reason}"
