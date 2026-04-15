"""MDL Vendor Outreach dispatcher — privacy and behavior tests.

Load-bearing assertion: the dispatcher must NEVER request column F
(Derek's Notes) from the Sheets API. Column F is a private scratchpad
walled off from agent context by design.

We verify this two ways:
  1. Source-level grep: the two read ranges must be A:E and G:K.
  2. Behavioral: run the dispatcher against a fake Sheets service that
     records every range it's asked for, then assert none of those
     ranges includes column F.
"""
from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.workflows import mdl_vendor_outreach_dispatcher as disp


# ── Source-level guard ────────────────────────────────────────────────────

def test_dispatcher_source_never_mentions_column_f_range():
    """The dispatcher source must never contain a Sheets range that reads F.

    Allowed: A2:E, G2:K, H{n}:I, H{n}:J, I{n} (writes to stamped columns)
    Forbidden: any actual sheet range that crosses or includes F (e.g.
    !A:K, !A:F, !F2:...). We look specifically for qualified sheet ranges
    prefixed with `!` so the test doesn't match descriptive docstring text.
    """
    src_path = Path(disp.__file__)
    src = src_path.read_text(encoding="utf-8")

    # Qualified-range patterns (must be prefixed with `!` — real A1 range
    # syntax in a Sheets API call).
    forbidden_range_patterns = [
        r"!A\d*:K",     # full row span through K as a real range
        r"!A\d*:F",     # span ending at F
        r"!F\d*:",      # any range starting at F
        r"!F\d+\b",     # single F cell reference
        r":F\d+\b",     # F as the end of a range
    ]
    for pat in forbidden_range_patterns:
        assert not re.search(pat, src), (
            f"Dispatcher source matches forbidden col-F range pattern: {pat!r}"
        )

    # Positive: the two expected disjoint ranges must be present (with !)
    assert "!A2:E" in src, "Expected !A2:E read range not found in dispatcher"
    assert "!G2:K" in src, "Expected !G2:K read range not found in dispatcher"


# ── Behavioral guard: record every range the dispatcher asks for ─────────

class _RangeRecordingSheets:
    """Fake Sheets service that records all ranges requested."""

    def __init__(self):
        self.ranges_read: list[str] = []
        self.ranges_written: list[str] = []

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def batchGet(self, spreadsheetId, ranges, majorDimension="ROWS"):
        self.ranges_read.extend(ranges)
        # Return one pending row that matches col K checked, H empty, valid email
        ae_values = [
            ["Test Vendor LLC", "Derek", "deBoer", "derekndeboer@gmail.com", "Sasha"],
        ]
        gk_values = [
            ["", "", "", "", "TRUE"],
        ]
        resp = {
            "valueRanges": [
                {"values": ae_values},
                {"values": gk_values},
            ]
        }
        return _FakeExec(resp)

    def batchUpdate(self, spreadsheetId, body):
        for entry in body.get("data", []):
            self.ranges_written.append(entry.get("range", ""))
        return _FakeExec({})

    def update(self, **kwargs):
        self.ranges_written.append(kwargs.get("range", ""))
        return _FakeExec({})

    def get(self, **kwargs):
        return _FakeExec({})


class _FakeExec:
    def __init__(self, payload):
        self._payload = payload

    def execute(self):
        return self._payload


def test_dispatcher_never_requests_column_f_at_runtime():
    """Run a full dry-run cycle against a recording fake and assert
    that no requested range touches column F."""
    fake = _RangeRecordingSheets()

    with patch.object(disp, "get_sheets_service", return_value=fake), \
         patch.object(disp, "get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(MDL_VENDOR_SHEET_ID="FAKE_SHEET_ID")
        stats = disp.run(dry_run=True)

    assert stats["rows_scanned"] == 1
    assert stats["sent"] == 1  # dry-run counts render success

    # Every requested range must be in A:E or G:K (plus downstream H/I/J writes)
    for r in fake.ranges_read:
        # Allowed reads
        assert "F" not in _column_letters_in_range(r), (
            f"Read range {r!r} includes column F — privacy violation"
        )
    for r in fake.ranges_written:
        assert "F" not in _column_letters_in_range(r), (
            f"Write range {r!r} includes column F — privacy violation"
        )


def _column_letters_in_range(range_str: str) -> set[str]:
    """Return the set of column letters spanned by a Sheets A1 range.

    e.g. 'Vendors!A2:E' -> {'A','B','C','D','E'}
         'Vendors!H5:J5' -> {'H','I','J'}
         'Vendors!I5'    -> {'I'}
    """
    # Strip tab prefix if present
    if "!" in range_str:
        range_str = range_str.split("!", 1)[1]

    # Extract column letters from endpoints
    m = re.match(r"^([A-Z]+)\d*(?::([A-Z]+)\d*)?$", range_str)
    if not m:
        return set()
    start_col = m.group(1)
    end_col = m.group(2) or start_col

    # Single-letter columns only (this sheet is A-K)
    if len(start_col) > 1 or len(end_col) > 1:
        # Multi-letter shouldn't happen in our schema
        return set()

    start_ord = ord(start_col)
    end_ord = ord(end_col)
    return {chr(c) for c in range(start_ord, end_ord + 1)}


# ── Template render tests ────────────────────────────────────────────────

def test_render_with_first_name_and_referring():
    subject, body = disp._render_first_touch(
        first_name="Jamie", referring_contact_name="Pat Kowalski"
    )
    # Subject uses ASCII hyphen, not em-dash (AI tell).
    assert subject == "Following up from Derek's call today - deBoer Freight"
    assert "Hi Jamie," in body
    assert "after he spoke with Pat Kowalski earlier today" in body
    # No em-dash in body (AI tell).
    assert "—" not in body
    assert "{" not in body  # no unresolved placeholders


def test_render_blank_first_name_uses_plain_hello():
    _, body = disp._render_first_touch(first_name="", referring_contact_name="Pat")
    # Hard rule: no "there", no "team" as greeting fallback — literal 'Hello,'
    assert body.splitlines()[0] == "Hello,"
    assert "Hi ," not in body


def test_render_blank_referring_drops_phrase_cleanly():
    """Per feedback_avoid_ai_tells.md: when referring contact is blank,
    DROP the 'after he spoke with {name}' phrase entirely. Never
    substitute 'your team', 'your office', etc. — those are AI tells."""
    _, body = disp._render_first_touch(first_name="Jamie", referring_contact_name="")
    # Phrase should be replaced with 'after his call' (no name reference)
    assert "after his call earlier today" in body
    # Template-fill substitutes MUST NOT appear
    assert "your team" not in body
    assert "your office" not in body
    # The templated phrase ("after he spoke with") must be fully gone
    assert "after he spoke with" not in body
    # No leftover placeholders
    assert "{referring_contact_name}" not in body
    assert "{referring_contact_clause}" not in body
    assert "{" not in body


def test_render_fails_loud_on_unresolved_placeholder(monkeypatch, tmp_path):
    """If somebody adds a new {variable} to the template without updating
    the renderer, we must fail loudly rather than send a broken email."""
    bad_template = tmp_path / "bad.txt"
    bad_template.write_text(
        "SUBJECT: test\n---\n{greeting}\n\nHi {mystery_field}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(disp, "_TEMPLATE_PATH", bad_template)
    with pytest.raises((ValueError, KeyError)):
        disp._render_first_touch(first_name="Jamie", referring_contact_name="Pat")


# ── Email validation ─────────────────────────────────────────────────────

@pytest.mark.parametrize("email,expected", [
    ("derek@example.com", True),
    ("first.last+tag@sub.example.co.uk", True),
    ("bad email", False),
    ("no-at-sign.com", False),
    ("", False),
    ("@no-local.com", False),
])
def test_email_validation(email, expected):
    assert disp._is_valid_email(email) is expected
