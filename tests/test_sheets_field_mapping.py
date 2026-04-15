"""Tests for the sheet-header -> python-alias read-path augmentation.

The live Carrier Database returns rows keyed by human-readable headers,
but workflow code reads them via underscored python keys. These tests
pin the alias contract without touching the live sheet.
"""
from __future__ import annotations

from app.sheets import _augment_with_aliases, _READ_ALIAS_MAP


def _fake_row() -> dict[str, str]:
    return {
        "Carrier ID": "DOT-123456",
        "Status": "active",
        "Company Name": "Acme Trucking LLC",
        "MC Number": "MC-987654",
        "DOT Number": "123456",
        "Contact Name": "Jane Doe",
        "Contact Email": "jane@acme.example",
        "Contact Phone": "555-0100",
        "Equipment Types": "Reefer",
        "Fleet Size": "12",
        "Insurance Liability": "1000000",
        "Insurance Cargo": "100000",
        "Insurance Expiry": "2027-01-01",
        "Authority Status": "ACTIVE",
        "Authority Date": "2022-06-15",
        "Safety Rating": "SATISFACTORY",
        "Compliance Status": "CLEAR",
        "Score": "95",
        "Outreach Status": "OUTREACH_SENT",
        "Notes": "prefers dry van",
    }


def test_outreach_status_alias():
    row = _augment_with_aliases(_fake_row())
    assert row["Outreach Status"] == "OUTREACH_SENT"
    assert row["Onboarding_Status"] == "OUTREACH_SENT"
    assert row.get("Onboarding_Status") == row.get("Outreach Status")


def test_mc_number_alias():
    row = _augment_with_aliases(_fake_row())
    assert row["MC Number"] == "MC-987654"
    assert row["MC_Number"] == "MC-987654"


def test_primary_email_alias():
    row = _augment_with_aliases(_fake_row())
    assert row["Contact Email"] == "jane@acme.example"
    assert row["Primary_Email"] == "jane@acme.example"


def test_equipment_type_alias():
    row = _augment_with_aliases(_fake_row())
    assert row["Equipment Types"] == "Reefer"
    assert row["Equipment_Type"] == "Reefer"


def test_full_alias_coverage():
    row = _augment_with_aliases(_fake_row())
    # Every mapped header should have its python alias resolve to the same value.
    for header, alias in _READ_ALIAS_MAP.items():
        if header in row:
            assert row.get(alias) == row.get(header), (
                f"alias {alias!r} should mirror header {header!r}"
            )


def test_dot_number_alias():
    row = _augment_with_aliases(_fake_row())
    assert row["DOT_Number"] == "123456"
    assert row["DOT Number"] == "123456"


def test_legal_name_alias():
    row = _augment_with_aliases(_fake_row())
    assert row["Legal_Name"] == "Acme Trucking LLC"
    assert row["Company Name"] == "Acme Trucking LLC"


def test_augmentation_is_non_destructive():
    original = _fake_row()
    before_keys = set(original.keys())
    augmented = _augment_with_aliases(original)
    # Original dict untouched (defensive copy).
    assert set(original.keys()) == before_keys
    # Augmented contains at least every original key.
    assert before_keys.issubset(set(augmented.keys()))


def test_missing_header_does_not_create_alias():
    partial = {"MC Number": "MC-1"}
    augmented = _augment_with_aliases(partial)
    assert augmented["MC_Number"] == "MC-1"
    # Headers that weren't present should not be synthesized as empty.
    assert "Primary_Email" not in augmented
    assert "Onboarding_Status" not in augmented


def test_existing_alias_key_not_overwritten():
    # If a python-key already exists (e.g. a fake test row), augmentation
    # must not clobber it.
    row = {"MC Number": "MC-1", "MC_Number": "MC-pre-existing"}
    augmented = _augment_with_aliases(row)
    assert augmented["MC_Number"] == "MC-pre-existing"
