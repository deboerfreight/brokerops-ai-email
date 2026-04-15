"""Lint-style coverage test for the carrier write path.

Greps `app/sheets.py` for any direct sheet-write call (`values.update`,
`values.append`, `values.batchUpdate`) that targets the Carrier Database
range. Every such call must be inside `insert_carrier`, which gates through
`vet_complete()` before writing.

If you add a new write path, wrap it with `app.vetting.writer.write_validated`
or route it through `insert_carrier` — do NOT call the sheets API directly
against `Carrier Database` from elsewhere in `app/sheets.py`.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


SHEETS_PY = Path(__file__).resolve().parent.parent / "app" / "sheets.py"


def test_sheets_py_exists():
    assert SHEETS_PY.exists(), f"sheets.py missing at {SHEETS_PY}"


def test_no_direct_carrier_db_writes_outside_insert_carrier():
    """Every write to 'Carrier Database' from app/sheets.py must be inside
    insert_carrier (the only function that runs the vetting gate first)."""
    src = SHEETS_PY.read_text(encoding="utf-8")

    # Locate insert_carrier function span (def insert_carrier ... next def at col 0)
    func_pat = re.compile(r"^def insert_carrier\b", re.MULTILINE)
    m = func_pat.search(src)
    assert m, "insert_carrier function not found in app/sheets.py"

    # Find the next top-level def after insert_carrier
    after = src[m.end():]
    next_def = re.search(r"^def \w+", after, re.MULTILINE)
    insert_carrier_end = m.end() + (next_def.start() if next_def else len(after))
    insert_body = src[m.start():insert_carrier_end]

    outside = src[:m.start()] + src[insert_carrier_end:]

    # Patterns that would indicate a direct carrier-db write
    danger_patterns = [
        r"CARRIER_DB_RANGE",
        r"'Carrier Database'!",
        r'"Carrier Database"!',
        r"CARRIER_DB_TAB",
    ]

    write_call_patterns = [
        r"values\(\)\.update",
        r"values\(\)\.append",
        r"values\(\)\.batchUpdate",
    ]

    # Allowed appearances outside insert_carrier:
    #   - constant definitions (CARRIER_DB_TAB = ..., CARRIER_DB_RANGE = ...)
    #   - read-only get() calls
    # We flag a violation only if we see a write call AND a Carrier Database
    # token within ~15 lines of the same function.
    violations: list[str] = []

    # Walk top-level functions (def at col 0)
    func_starts = [
        (m2.start(), m2.group())
        for m2 in re.finditer(r"^def (\w+)", outside, re.MULTILINE)
    ]
    func_starts.append((len(outside), "<eof>"))

    for i in range(len(func_starts) - 1):
        start, header = func_starts[i]
        end = func_starts[i + 1][0]
        body = outside[start:end]
        func_name = header.split()[1]

        # Skip helpers that are obviously read-only
        if func_name in {
            "get_all_carriers",
            "get_carrier",
            "get_carrier_by_dot",
            "find_carrier",
            "search_carriers_in_sheet",
            "is_carrier_dispatch_eligible",
        }:
            continue

        has_write = any(re.search(p, body) for p in write_call_patterns)
        mentions_carrier_db = any(re.search(p, body) for p in danger_patterns)
        if has_write and mentions_carrier_db:
            violations.append(
                f"{func_name}: contains both a write call and a Carrier Database "
                f"reference outside insert_carrier — wrap with write_validated() "
                f"or route through insert_carrier()."
            )

    if violations:
        pytest.fail(
            "Direct Carrier Database writes detected outside the vetting gate:\n  - "
            + "\n  - ".join(violations)
        )


def test_insert_carrier_gates_via_vet_complete():
    """insert_carrier must import vet_complete and route fails to quarantine."""
    src = SHEETS_PY.read_text(encoding="utf-8")
    func_pat = re.compile(r"^def insert_carrier\b", re.MULTILINE)
    m = func_pat.search(src)
    assert m, "insert_carrier function not found"
    after = src[m.end():]
    next_def = re.search(r"^def \w+", after, re.MULTILINE)
    body = after[: next_def.start() if next_def else len(after)]

    assert "vet_complete" in body, (
        "insert_carrier must call vet_complete() before writing"
    )
    assert "append_to_quarantine" in body, (
        "insert_carrier must route failures to append_to_quarantine()"
    )
    assert "PASS_BASIC" in body, (
        "insert_carrier must compare against PASS_BASIC"
    )
