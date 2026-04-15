"""Gate 3 — outreach dedup tests.

Covers three cases:
(a) carrier with no prior contact -> allowed
(b) carrier at OUTREACH_SENT sheet status -> blocked
(c) carrier with existing Gmail thread but sheet status still NEW -> blocked
    (hardening added in _has_any_prior_gmail_thread)

These tests monkeypatch sheets + gmail to avoid any network call.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from app.workflows import carrier_outreach


def _make_carrier(**overrides) -> dict:
    base = {
        "MC_Number": "MC-1",
        "DOT_Number": "DOT-1",
        "Legal_Name": "Test Carrier LLC",
        "DBA_Name": "Test Carrier",
        "Primary_Email": "ops@test.example",
        "Onboarding_Status": "NEW",
        "Equipment_Type": "Dry Van",
        "Preferred_Lanes": "",
        "Internal_Notes": "",
        # Sheet-level vetting gate (col AG); these dedup tests are orthogonal
        # to the vetting rules so default fixtures pre-pass the gate.
        "Vetting_Status": "pass_basic",
    }
    base.update(overrides)
    return base


# ── Case (a): NEW, no prior contact → allowed ─────────────────────────────────

def test_new_carrier_no_prior_contact_is_allowed():
    carrier = _make_carrier()

    with patch("app.workflows.carrier_outreach.get_all_carriers", return_value=[carrier]), \
         patch("app.workflows.carrier_outreach._has_any_prior_gmail_thread", return_value=False), \
         patch("app.workflows.carrier_outreach._verify_gmail_ready", return_value=True):
        stats = carrier_outreach.run(dry_run=True, batch_limit=5, send_delay=0)

    assert stats["initial_sent"] == 1, (
        f"NEW carrier with no prior contact should be sent; stats={stats}"
    )
    assert stats.get("skipped_existing_thread", 0) == 0


# ── Case (b): OUTREACH_SENT status → blocked by status gate ───────────────────

def test_outreach_sent_carrier_is_blocked():
    # OUTREACH_SENT with recent Last_Load_Date (today) → follow-up #1 window (3d) not met,
    # so nothing should be sent at all.
    from datetime import date
    carrier = _make_carrier(
        Onboarding_Status="OUTREACH_SENT",
        Internal_Notes=f"[Outreach {date.today().isoformat()}] threadId=t123",
        Last_Load_Date=date.today().isoformat(),
    )

    with patch("app.workflows.carrier_outreach.get_all_carriers", return_value=[carrier]), \
         patch("app.workflows.carrier_outreach._has_any_prior_gmail_thread", return_value=False), \
         patch("app.workflows.carrier_outreach._verify_gmail_ready", return_value=True):
        stats = carrier_outreach.run(dry_run=True, batch_limit=5, send_delay=0)

    assert stats["initial_sent"] == 0, (
        f"OUTREACH_SENT carriers should never get initial outreach; stats={stats}"
    )
    assert stats["followup_1_sent"] == 0  # too soon
    assert stats["followup_2_sent"] == 0


# ── Case (c): NEW status but existing Gmail thread → blocked by hardening ─────

def test_new_carrier_with_existing_thread_is_blocked():
    carrier = _make_carrier()

    with patch("app.workflows.carrier_outreach.get_all_carriers", return_value=[carrier]), \
         patch("app.workflows.carrier_outreach._has_any_prior_gmail_thread", return_value=True), \
         patch("app.workflows.carrier_outreach._verify_gmail_ready", return_value=True):
        stats = carrier_outreach.run(dry_run=True, batch_limit=5, send_delay=0)

    assert stats["initial_sent"] == 0, (
        "NEW carrier with existing Gmail thread should be blocked by "
        f"dedup hardening; stats={stats}"
    )
    assert stats.get("skipped_existing_thread", 0) == 1


# ── Bonus: the hardening helper itself ─────────────────────────────────────────

def test_has_any_prior_gmail_thread_fails_open_on_error():
    """If Gmail errors out, default to allow (sheet-status is still primary gate)."""
    with patch("app.google_auth.get_gmail_service", side_effect=RuntimeError("boom")):
        assert carrier_outreach._has_any_prior_gmail_thread("x@y.com") is False


def test_has_any_prior_gmail_thread_rejects_invalid_email():
    assert carrier_outreach._has_any_prior_gmail_thread("") is False
    assert carrier_outreach._has_any_prior_gmail_thread("not-an-email") is False
