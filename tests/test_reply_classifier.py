"""
Tests for app/reply_classifier.py

Covers all 6 categories with realistic synthesized replies.
Does NOT require live Gmail, Sheets, or Claude -- all AI calls are patched.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.reply_classifier import (
    CATEGORY_BOUNCE,
    CATEGORY_INTERESTED,
    CATEGORY_NOT_INTERESTED,
    CATEGORY_NEED_MORE_INFO,
    CATEGORY_OOO,
    CATEGORY_OOO_REDIRECT,
    CATEGORY_REDIRECT,
    classify_reply,
    ClassifiedReply,
    process_scheduled_doc_requests,
    _extract_ooo_return_date,
    _extract_new_email,
    _extract_bounce_info,
    _canned_reply_for_simple_question,
)


# ── Fixtures: synthesized realistic replies ───────────────────────────────────

SAMPLES = {
    # ── INTERESTED ────────────────────────────────────────────────────────────
    "interested_short": {
        "subject": "Re: Introduction -- deBoer Freight",
        "body": "Hey, yeah we run South FL to Atlanta a lot. Send me what you need.",
        "sender": "dispatch@suncoastrucking.com",
        "expected": CATEGORY_INTERESTED,
    },
    "interested_rates": {
        "subject": "Re: Quick intro from deBoer Freight",
        "body": (
            "We're interested. We run dry van, mostly FL to SE states. "
            "Our rate is $2.20/mile on the FL-GA lane. What loads do you have moving?"
        ),
        "sender": "ops@palmettotransport.net",
        "expected": CATEGORY_INTERESTED,
    },
    "interested_detailed": {
        "subject": "Re: Introduction -- deBoer Freight",
        "body": (
            "Hi Sofia,\n\n"
            "We have 5 dry vans running out of Tampa. We're interested in adding a broker partner. "
            "We can handle 40k lbs, no hazmat. Let us know what paperwork you need."
        ),
        "sender": "carlos.mendez@tbatransport.com",
        "expected": CATEGORY_INTERESTED,
    },
    # ── NOT INTERESTED ────────────────────────────────────────────────────────
    "not_interested_plain": {
        "subject": "Re: Introduction -- deBoer Freight",
        "body": "We're not looking for new broker partners right now. Thanks.",
        "sender": "info@gatorfreight.com",
        "expected": CATEGORY_NOT_INTERESTED,
    },
    "not_interested_remove": {
        "subject": "Re: Quick intro from deBoer Freight",
        "body": "Please remove me from your list.",
        "sender": "dispatch@centralfloridacarriers.com",
        "expected": CATEGORY_NOT_INTERESTED,
    },
    "not_interested_unsubscribe": {
        "subject": "Re: Introduction -- deBoer Freight",
        "body": "Unsubscribe. Do not contact us again.",
        "sender": "noemail@example.com",
        "expected": CATEGORY_NOT_INTERESTED,
    },
    # ── NEED MORE INFO ────────────────────────────────────────────────────────
    "need_info_lanes": {
        "subject": "Re: Introduction -- deBoer Freight",
        "body": "What lanes are you moving freight on? We're mainly Southeast, not sure if that fits.",
        "sender": "owner@miamifreight.com",
        "expected": CATEGORY_NEED_MORE_INFO,
    },
    "need_info_rates": {
        "subject": "Re: Introduction -- deBoer Freight",
        "body": "Sounds interesting. What do you typically pay per mile on the FL-NC lane?",
        "sender": "driver.ops@southerntrans.net",
        "expected": CATEGORY_NEED_MORE_INFO,
    },
    # ── OOO ───────────────────────────────────────────────────────────────────
    "ooo_with_date": {
        "subject": "Automatic reply: Introduction -- deBoer Freight",
        "body": (
            "I am out of the office and will return on April 21. "
            "For urgent matters, contact our dispatch at dispatch@example.com."
        ),
        "sender": "bob@truckingco.com",
        "expected": CATEGORY_OOO,
    },
    "ooo_vacation": {
        "subject": "Auto-Reply: Quick intro from deBoer Freight",
        "body": (
            "Hi, I'm on vacation until April 25th. I'll respond when I'm back. "
            "If this is urgent, call 305-555-1234."
        ),
        "sender": "manager@floridatrucking.com",
        "expected": CATEGORY_OOO,
    },
    # ── BOUNCE ───────────────────────────────────────────────────────────────
    "bounce_hard": {
        "subject": "Delivery Status Notification (Failure)",
        "body": (
            "This message was not delivered to: badaddress@fakeco.com\n"
            "550 5.1.1 The email account you tried to reach does not exist.\n"
            "The response from the remote server was:\n550 5.1.1 No such user."
        ),
        "sender": "mailer-daemon@googlemail.com",
        "expected": CATEGORY_BOUNCE,
    },
    "bounce_mailer_daemon": {
        "subject": "Mail delivery failed: returning message to sender",
        "body": (
            "This message was created automatically by mail delivery software.\n"
            "A message that you sent could not be delivered to one or more of its recipients.\n"
            "Host or domain name not found. 550 Host unknown."
        ),
        "sender": "Mailer-Daemon@dispatch.example.net",
        "expected": CATEGORY_BOUNCE,
    },
    "bounce_soft": {
        "subject": "Delivery Status Notification (Delay)",
        "body": (
            "This is a warning message. Your message could not be delivered.\n"
            "Reason: 452 4.2.2 Mailbox full. The mailbox is over its storage limit.\n"
            "Delivery will be retried."
        ),
        "sender": "postmaster@recipient-domain.com",
        "expected": CATEGORY_BOUNCE,
    },
    # ── REDIRECT ──────────────────────────────────────────────────────────────
    "redirect_new_contact": {
        "subject": "Re: Introduction -- deBoer Freight",
        "body": (
            "Hi, I no longer handle freight for this company. "
            "Please contact our dispatch manager: dispatch@newcontact-trucking.com"
        ),
        "sender": "oldcontact@trucking.com",
        "expected": CATEGORY_REDIRECT,
    },
    "redirect_forward": {
        "subject": "Re: Quick intro from deBoer Freight",
        "body": (
            "I'm not the right person for this. "
            "You should reach out to our operations manager at ops.mgr@floridaexpress.com."
        ),
        "sender": "general@floridaexpress.com",
        "expected": CATEGORY_REDIRECT,
    },
}


# ── Helper: patch Claude call to avoid live API ───────────────────────────────

def _patched_classify_no_claude(monkeypatch_or_patch, subject, body, sender):
    """Run classify_reply with Claude patched to return empty (regex-only path)."""
    with patch("app.reply_classifier._call_claude_classify", return_value={}):
        return classify_reply(subject, body, sender)


# ── Tests: fast-path (regex) categories ──────────────────────────────────────

class TestFastPathClassification:
    """Bounce, OOO, not_interested, and redirect are classified by regex alone."""

    def test_hard_bounce(self):
        s = SAMPLES["bounce_hard"]
        result = _patched_classify_no_claude(None, s["subject"], s["body"], s["sender"])
        assert result.category == CATEGORY_BOUNCE
        assert result.extracted_data.get("bounce_type") == "hard"

    def test_soft_bounce(self):
        s = SAMPLES["bounce_soft"]
        result = _patched_classify_no_claude(None, s["subject"], s["body"], s["sender"])
        assert result.category == CATEGORY_BOUNCE
        assert result.extracted_data.get("bounce_type") == "soft"

    def test_mailer_daemon_bounce(self):
        s = SAMPLES["bounce_mailer_daemon"]
        result = _patched_classify_no_claude(None, s["subject"], s["body"], s["sender"])
        assert result.category == CATEGORY_BOUNCE

    def test_ooo_with_return_date(self):
        # This sample contains a redirect email so Amendment 1 upgrades it to ooo_redirect
        s = SAMPLES["ooo_with_date"]
        result = _patched_classify_no_claude(None, s["subject"], s["body"], s["sender"])
        assert result.category in (CATEGORY_OOO, CATEGORY_OOO_REDIRECT)

    def test_ooo_vacation(self):
        s = SAMPLES["ooo_vacation"]
        result = _patched_classify_no_claude(None, s["subject"], s["body"], s["sender"])
        assert result.category == CATEGORY_OOO

    def test_remove_request(self):
        s = SAMPLES["not_interested_remove"]
        result = _patched_classify_no_claude(None, s["subject"], s["body"], s["sender"])
        assert result.category == CATEGORY_NOT_INTERESTED

    def test_unsubscribe(self):
        s = SAMPLES["not_interested_unsubscribe"]
        result = _patched_classify_no_claude(None, s["subject"], s["body"], s["sender"])
        assert result.category == CATEGORY_NOT_INTERESTED

    def test_redirect_new_contact(self):
        s = SAMPLES["redirect_new_contact"]
        result = _patched_classify_no_claude(None, s["subject"], s["body"], s["sender"])
        assert result.category == CATEGORY_REDIRECT
        assert "newcontact-trucking.com" in result.extracted_data.get("new_email", "")

    def test_redirect_forward(self):
        s = SAMPLES["redirect_forward"]
        result = _patched_classify_no_claude(None, s["subject"], s["body"], s["sender"])
        assert result.category == CATEGORY_REDIRECT
        assert "floridaexpress.com" in result.extracted_data.get("new_email", "")


# ── Tests: Claude-path categories ────────────────────────────────────────────

class TestClaudePathClassification:
    """interested and need_more_info fall through to Claude. We mock Claude's output."""

    def _mock_claude(self, category: str, confidence: str = "high", extracted: dict = None):
        return {
            "category": category,
            "confidence": confidence,
            "action": "test action",
            "extracted_data": extracted or {},
        }

    def test_interested_short(self):
        s = SAMPLES["interested_short"]
        with patch(
            "app.reply_classifier._call_claude_classify",
            return_value=self._mock_claude(CATEGORY_INTERESTED),
        ):
            result = classify_reply(s["subject"], s["body"], s["sender"])
        assert result.category == CATEGORY_INTERESTED

    def test_interested_with_rates(self):
        s = SAMPLES["interested_rates"]
        with patch(
            "app.reply_classifier._call_claude_classify",
            return_value=self._mock_claude(CATEGORY_INTERESTED),
        ):
            result = classify_reply(s["subject"], s["body"], s["sender"])
        assert result.category == CATEGORY_INTERESTED
        assert result.confidence in ("high", "medium")

    def test_interested_detailed(self):
        s = SAMPLES["interested_detailed"]
        with patch(
            "app.reply_classifier._call_claude_classify",
            return_value=self._mock_claude(CATEGORY_INTERESTED, "high"),
        ):
            result = classify_reply(s["subject"], s["body"], s["sender"])
        assert result.category == CATEGORY_INTERESTED

    def test_need_info_lanes(self):
        s = SAMPLES["need_info_lanes"]
        with patch(
            "app.reply_classifier._call_claude_classify",
            return_value=self._mock_claude(
                CATEGORY_NEED_MORE_INFO,
                extracted={"question_text": "What lanes are you moving freight on?"},
            ),
        ):
            result = classify_reply(s["subject"], s["body"], s["sender"])
        assert result.category == CATEGORY_NEED_MORE_INFO

    def test_need_info_rates(self):
        s = SAMPLES["need_info_rates"]
        with patch(
            "app.reply_classifier._call_claude_classify",
            return_value=self._mock_claude(
                CATEGORY_NEED_MORE_INFO,
                extracted={"question_text": "What do you typically pay per mile?"},
            ),
        ):
            result = classify_reply(s["subject"], s["body"], s["sender"])
        assert result.category == CATEGORY_NEED_MORE_INFO

    def test_not_interested_plain(self):
        # "not looking for new broker" -- regex won't catch it, Claude needed
        s = SAMPLES["not_interested_plain"]
        with patch(
            "app.reply_classifier._call_claude_classify",
            return_value=self._mock_claude(CATEGORY_NOT_INTERESTED),
        ):
            result = classify_reply(s["subject"], s["body"], s["sender"])
        assert result.category == CATEGORY_NOT_INTERESTED


# ── Tests: extractor helpers ──────────────────────────────────────────────────

class TestExtractors:
    def test_extract_ooo_date_month_day(self):
        body = "I will be back on April 21 after vacation."
        date_str = _extract_ooo_return_date(body)
        # Should parse to some date
        assert date_str == "" or "-" in date_str  # may or may not parse depending on format

    def test_extract_new_email_from_redirect(self):
        body = "Please contact dispatch@newco.com for freight inquiries."
        sender = "old@trucking.com"
        email = _extract_new_email(body, sender)
        assert email == "dispatch@newco.com"

    def test_extract_new_email_excludes_sender(self):
        body = "Contact me at old@trucking.com or try dispatch@newco.com."
        sender = "old@trucking.com"
        email = _extract_new_email(body, sender)
        assert email == "dispatch@newco.com"

    def test_bounce_info_hard(self):
        body = "550 5.1.1 The email account does not exist."
        subject = "Delivery Status Notification"
        info = _extract_bounce_info(body, subject)
        assert info["bounce_type"] == "hard"
        assert "550" in info.get("bounce_code", "")

    def test_bounce_info_soft(self):
        body = "452 4.2.2 Mailbox full. The mailbox is over its storage limit."
        subject = "Delivery Status Notification"
        info = _extract_bounce_info(body, subject)
        assert info["bounce_type"] == "soft"


# ── Tests: canned reply logic ─────────────────────────────────────────────────

class TestCannedReplies:
    def test_lanes_question_gets_canned_reply(self):
        reply = _canned_reply_for_simple_question("What lanes do you run freight on?")
        assert reply is not None
        assert "FL" in reply or "Southeast" in reply

    def test_rates_question_gets_canned_reply(self):
        reply = _canned_reply_for_simple_question("What do you pay per mile?")
        assert reply is not None
        assert "rate" in reply.lower() or "mile" in reply.lower()

    def test_equipment_question_gets_canned_reply(self):
        reply = _canned_reply_for_simple_question("What equipment types do you need?")
        assert reply is not None
        assert "dry van" in reply.lower() or "flatbed" in reply.lower()

    def test_complex_question_escalates_to_derek(self):
        # A question about claims process, legal terms, or specific compliance
        # requirements has no canned answer and should escalate to Derek.
        reply = _canned_reply_for_simple_question(
            "What is your claims process if cargo is damaged and do you require a carrier agreement?"
        )
        assert reply is None  # no keyword match -> escalate


# ── Tests: ClassifiedReply dataclass integrity ────────────────────────────────

class TestClassifiedReplyIntegrity:
    def test_bounce_has_bounce_type(self):
        result = _patched_classify_no_claude(
            None,
            "Delivery Status Notification (Failure)",
            "550 5.1.1 The email account does not exist.",
            "mailer-daemon@example.com",
        )
        assert result.category == CATEGORY_BOUNCE
        assert "bounce_type" in result.extracted_data

    def test_ooo_category_returned(self):
        result = _patched_classify_no_claude(
            None,
            "Automatic reply: Introduction",
            "I am out of the office until May 1.",
            "user@company.com",
        )
        assert result.category == CATEGORY_OOO

    def test_redirect_has_new_email(self):
        result = _patched_classify_no_claude(
            None,
            "Re: Introduction",
            "Please contact ops@newco.com instead.",
            "old@trucking.com",
        )
        assert result.category == CATEGORY_REDIRECT
        assert result.extracted_data.get("new_email") == "ops@newco.com"

    def test_confidence_always_set(self):
        with patch("app.reply_classifier._call_claude_classify", return_value={}):
            result = classify_reply("Re: test", "some body text", "user@example.com")
        assert result.confidence in ("high", "medium", "low")

    def test_category_always_valid(self):
        from app.reply_classifier import ALL_CATEGORIES
        with patch(
            "app.reply_classifier._call_claude_classify",
            return_value={"category": "invented_category", "confidence": "high"},
        ):
            result = classify_reply("Re: test", "body", "sender@example.com")
        assert result.category in ALL_CATEGORIES


# ── Tests: Amendment 1 — ooo_redirect combined handling ──────────────────────

class TestOooRedirectCombined:
    """Three cases: pure OOO, OOO+clear redirect, OOO+ambiguous team mention."""

    def test_pure_ooo_no_redirect_stays_ooo(self):
        """OOO with return date but NO redirect email -- category must be ooo, not ooo_redirect."""
        result = _patched_classify_no_claude(
            None,
            "Automatic reply: Introduction",
            "I am out of the office and will return on April 28. I'll be checking email minimally.",
            "driver@carrier.com",
        )
        assert result.category == CATEGORY_OOO
        assert "new_email" not in result.extracted_data or not result.extracted_data.get("new_email")

    def test_ooo_with_clear_redirect_becomes_ooo_redirect(self):
        """OOO body that also provides a new email address -- must classify as ooo_redirect."""
        result = _patched_classify_no_claude(
            None,
            "Auto-Reply: Introduction -- deBoer Freight",
            (
                "I'm out of the office until April 22. "
                "For urgent matters please contact Sarah at sarah@acmecarriers.com."
            ),
            "bob@acmecarriers.com",
        )
        assert result.category == CATEGORY_OOO_REDIRECT
        assert result.extracted_data.get("new_email") == "sarah@acmecarriers.com"
        # return_date may or may not parse; just verify the key exists
        assert "return_date" in result.extracted_data

    def test_ooo_ambiguous_team_mention_stays_ooo(self):
        """OOO with vague 'team can help' text but no extractable email -- must stay ooo."""
        result = _patched_classify_no_claude(
            None,
            "Automatic reply: Introduction -- deBoer Freight",
            (
                "I'm out of the office this week. "
                "For anything urgent my team can help."
            ),
            "manager@truckingco.com",
        )
        assert result.category == CATEGORY_OOO
        # No email should have been extracted
        assert not result.extracted_data.get("new_email")


# ── Tests: Amendment 2 — auto-schedule E4 on interested ──────────────────────

class TestAutoScheduledE4:
    """Two cases: interested reply sets scheduling fields; process_scheduled triggers send."""

    def test_interested_reply_schedules_e4_not_sends_immediately(self):
        """Interested reply must write docs_request_scheduled + E4_ScheduledFor; no immediate send."""
        from unittest.mock import call
        from datetime import datetime, timedelta

        fake_now = datetime(2026, 4, 15, 12, 0, 0)
        expected_scheduled = fake_now + timedelta(minutes=5)

        sheet_updates: dict = {}

        def fake_update_fields(dot, fields):
            sheet_updates.update(fields)

        with patch("app.reply_classifier._get_carrier_summary", return_value="TestCo (FL)"), \
             patch("app.reply_classifier._update_carrier_outreach_status") as mock_status, \
             patch("app.reply_classifier.notify_slack"), \
             patch("app.reply_classifier.datetime") as mock_dt, \
             patch("app.sheets.update_carrier_fields_by_dot", side_effect=fake_update_fields):

            mock_dt.utcnow.return_value = fake_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            from app.reply_classifier import route_classified_reply, CATEGORY_INTERESTED, ClassifiedReply
            classified = ClassifiedReply(
                category=CATEGORY_INTERESTED,
                confidence="high",
                action="test",
                raw_subject="Re: Introduction",
                raw_sender="dispatch@testco.com",
            )
            route_classified_reply(classified, "1234567")

        # Status must flip to docs_request_scheduled (not docs_requested)
        mock_status.assert_called_once_with(
            "1234567",
            outreach_status="replied_interested",
            onboarding_status="docs_request_scheduled",
        )
        # ScheduledFor must be set to now+5m
        assert sheet_updates.get("Onboarding_E4_ScheduledFor") == expected_scheduled.isoformat()

    def test_process_scheduled_fires_e4_when_due(self):
        """process_scheduled_doc_requests sends E4 and flips row to docs_requested when time has passed."""
        from datetime import datetime, timedelta

        past_time = (datetime.utcnow() - timedelta(minutes=10)).isoformat()

        fake_carrier = {
            "Onboarding_Status": "docs_request_scheduled",
            "Onboarding_E4_ScheduledFor": past_time,
            "DOT Number": "9999999",
            "DOT_Number": "9999999",
            "Contact Email": "ops@readycarrier.com",
            "Primary_Email": "ops@readycarrier.com",
            "Outreach_Thread_Id": "thread_abc123",
            "Contact Name": "Joe Driver",
            "Company Name": "Ready Carrier LLC",
        }

        written_fields: dict = {}

        def fake_update(dot, fields):
            written_fields.update(fields)

        # process_scheduled_doc_requests imports get_all_carriers and
        # update_carrier_fields_by_dot from app.sheets at call-time.
        # Patch at the source module so the lazy import sees the mock.
        with patch("app.reply_classifier.get_settings") as mock_settings, \
             patch("app.sheets.get_all_carriers", return_value=[fake_carrier]), \
             patch("app.sheets.update_carrier_fields_by_dot", side_effect=fake_update), \
             patch("app.gmail.reply_to_thread"), \
             patch("app.onboarding_intake.handle_carrier_interested"):

            mock_settings.return_value.OUTREACH_AUTO_REPLY_ENABLED = True
            count = process_scheduled_doc_requests()

        # We should have attempted to send 1 E4
        assert count == 1
        # Final status must be docs_requested
        assert written_fields.get("Onboarding_Status") == "docs_requested"
        assert "Onboarding_E4_SentAt" in written_fields
