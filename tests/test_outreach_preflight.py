"""Persistent preflight harness.

Runs the dedup gate + initial-outreach render against a small fixture set
and asserts that any carrier with prior outbound history is filtered out
BEFORE reaching the render/send step.
"""
from __future__ import annotations

from unittest.mock import patch

from app.workflows import carrier_outreach
from app.workflows.carrier_outreach import build_initial_outreach


FIXTURES = [
    {
        "MC_Number": "MC-1001",
        "DOT_Number": "1001",
        "Legal_Name": "Clean Carrier LLC",
        "DBA_Name": "Clean Carrier",
        "Primary_Email": "ops@clean.example",
        "Onboarding_Status": "NEW",
        "Equipment_Type": "Dry Van",
        "Preferred_Lanes": "FL-GA",
        "Internal_Notes": "",
        "Vetting_Status": "pass_basic",
        "_expect_included": True,
        "_prior_thread": False,
    },
    {
        "MC_Number": "MC-1002",
        "DOT_Number": "1002",
        "Legal_Name": "Prior Outbound Inc",
        "DBA_Name": "Prior Outbound",
        "Primary_Email": "ops@prior.example",
        "Onboarding_Status": "NEW",
        "Equipment_Type": "Reefer",
        "Preferred_Lanes": "",
        "Internal_Notes": "",
        "Vetting_Status": "pass_basic",
        "_expect_included": False,
        "_prior_thread": True,
    },
    {
        "MC_Number": "MC-1003",
        "DOT_Number": "1003",
        "Legal_Name": "Already Sent Trucking",
        "DBA_Name": "",
        "Primary_Email": "dispatch@already.example",
        "Onboarding_Status": "OUTREACH_SENT",
        "Equipment_Type": "Flatbed",
        "Preferred_Lanes": "",
        "Internal_Notes": "threadId=t999",
        # Recent Last_Load_Date → follow-up #1 window (3d) not met, so no send.
        "Last_Load_Date": __import__("datetime").date.today().isoformat(),
        "Vetting_Status": "pass_basic",
        "_expect_included": False,
        "_prior_thread": False,
    },
    {
        "MC_Number": "MC-1004",
        "DOT_Number": "1004",
        "Legal_Name": "Phone Only LLC",
        "DBA_Name": "",
        "Primary_Email": "PHONE_ONLY",
        "Onboarding_Status": "NEW",
        "Equipment_Type": "",
        "Preferred_Lanes": "",
        "Internal_Notes": "",
        "Vetting_Status": "pass_basic",
        "_expect_included": False,
        "_prior_thread": False,
    },
]


def _prior_thread_lookup(email: str) -> bool:
    for f in FIXTURES:
        if f["Primary_Email"].lower() == email.lower():
            return f["_prior_thread"]
    return False


def test_preflight_filters_prior_outbound_and_status():
    """Gate 2+3 combined: run the real pipeline against fixtures, confirm
    only MC-1001 (clean, NEW, no prior thread) gets initial outreach rendered.
    """
    fixtures = [dict(f) for f in FIXTURES]

    with patch("app.workflows.carrier_outreach.get_all_carriers", return_value=fixtures), \
         patch("app.workflows.carrier_outreach._has_any_prior_gmail_thread",
               side_effect=_prior_thread_lookup), \
         patch("app.workflows.carrier_outreach._verify_gmail_ready", return_value=True):
        stats = carrier_outreach.run(dry_run=True, batch_limit=20, send_delay=0)

    # Only MC-1001 should have been sent.
    assert stats["initial_sent"] == 1, (
        f"Expected 1 clean carrier rendered, got {stats}"
    )
    # MC-1002 should have been skipped by the thread-existence guard.
    assert stats.get("skipped_existing_thread", 0) == 1
    # MC-1004 (PHONE_ONLY) should be in phone_only_skipped.
    assert stats["phone_only_skipped"] == 1
    # MC-1003 (OUTREACH_SENT) should not yield any send (follow-up window not met anyway).
    assert stats["followup_1_sent"] == 0
    assert stats["followup_2_sent"] == 0


def test_render_initial_outreach_contains_required_fields():
    """Any carrier passed to build_initial_outreach must produce a complete
    email: non-empty subject, non-empty body, deBoer MC number, Sofia signature.
    """
    carrier = FIXTURES[0]
    subject, body = build_initial_outreach(carrier)

    assert subject, "Subject must not be empty"
    assert "deBoer Freight" in subject
    assert "MC#" in subject
    assert "Sofia Reyes" in body
    assert carrier["DBA_Name"] in body or carrier["Legal_Name"] in body
    assert "safer.fmcsa.dot.gov" in body


def test_fail_loudly_if_prior_outbound_reaches_render():
    """Meta-guarantee: if someone disables the hardening, the test explodes.
    We simulate that by calling the internal _send_initial_outreach path
    on a fixture with a prior thread — it should not be reachable when
    run() is invoked with the hardening in place. This test locks in the
    behavior: the hardening must live in the run() dispatch, not only in
    the lower helpers.
    """
    fixture_prior = [dict(FIXTURES[1])]  # MC-1002, prior thread

    with patch("app.workflows.carrier_outreach.get_all_carriers", return_value=fixture_prior), \
         patch("app.workflows.carrier_outreach._has_any_prior_gmail_thread", return_value=True), \
         patch("app.workflows.carrier_outreach._verify_gmail_ready", return_value=True):
        stats = carrier_outreach.run(dry_run=True, batch_limit=5, send_delay=0)

    assert stats["initial_sent"] == 0
    assert stats.get("skipped_existing_thread", 0) == 1
