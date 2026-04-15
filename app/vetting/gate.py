"""BrokerOps AI – Canonical carrier vetting gate.

`vet_complete()` is the only function that decides pass/fail for a carrier.
Every callsite that writes carriers to the database, dispatches loads, or
qualifies an outreach target should go through `is_carrier_vetted()`.

Tolerant of:
  - Sheet-augmented dicts (Power_Units, Insurance_Liability, Vetting Status)
  - Sheet-header dicts (Fleet Size, Insurance Liability, Vetting Status)
  - FMCSA-normalized dicts (Power_Units, Insurance_Liability, Safety_Rating)

Blank or missing critical fields → `needs_review` (NOT pass).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any

from app.vetting.rules import RULES


# ── Status constants ───────────────────────────────────────────────────────

PASS_BASIC = "pass_basic"
NEEDS_REVIEW = "needs_review"
FAIL_FLEET = "fail_fleet_size"
FAIL_LIABILITY = "fail_insurance_liability"
FAIL_CARGO = "fail_insurance_cargo"
FAIL_SAFETY = "fail_safety_rating"
FAIL_VEHICLE_OOS = "fail_vehicle_oos"
FAIL_DRIVER_OOS = "fail_driver_oos"
FAIL_CRASH = "fail_crash_rate"
FAIL_REEFER = "fail_reefer_maintenance"
FAIL_SHELL = "fail_shell_carrier"


@dataclass
class VettingResult:
    passed: bool
    status: str
    reason: str
    checked_at: str

    def to_dict(self) -> dict:
        return asdict(self)


# ── Tolerant field readers ────────────────────────────────────────────────


_INT_RX = re.compile(r"[-\d]+")


def _money(val: Any) -> int:
    """Parse a money/integer value tolerating $, commas, blanks."""
    if val is None:
        return 0
    s = str(val).strip()
    if not s:
        return 0
    s = s.replace("$", "").replace(",", "")
    try:
        return int(float(s))
    except (ValueError, TypeError):
        # try to extract first int run
        m = _INT_RX.search(s)
        if m:
            try:
                return int(m.group())
            except ValueError:
                return 0
        return 0


def _intish(val: Any) -> int:
    return _money(val)


def _floatish(val: Any) -> float:
    if val is None:
        return 0.0
    s = str(val).strip().replace("%", "").replace(",", "")
    if not s:
        return 0.0
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _get_first(carrier: dict, *keys: str) -> Any:
    for k in keys:
        if k in carrier and carrier[k] not in (None, ""):
            return carrier[k]
    return None


def _fleet(carrier: dict) -> int:
    return _intish(_get_first(carrier, "Power_Units", "Fleet Size", "Fleet_Size"))


def _liability(carrier: dict) -> int:
    return _money(
        _get_first(
            carrier,
            "Insurance_Liability",
            "Insurance Liability",
            "Auto_Liability_Coverage",
        )
    )


def _cargo(carrier: dict) -> int:
    return _money(
        _get_first(
            carrier,
            "Insurance_Cargo",
            "Insurance Cargo",
            "Cargo_Coverage",
        )
    )


def _safety(carrier: dict) -> str:
    return str(_get_first(carrier, "Safety_Rating", "Safety Rating") or "").strip().upper()


def _drivers(carrier: dict) -> int:
    return _intish(_get_first(carrier, "Driver_Count", "Drivers", "Total_Drivers"))


def _vehicle_oos(carrier: dict) -> float:
    return _floatish(_get_first(carrier, "Vehicle_OOS_Rate", "Vehicle OOS Rate"))


def _driver_oos(carrier: dict) -> float:
    return _floatish(_get_first(carrier, "Driver_OOS_Rate", "Driver OOS Rate"))


def _crash_rate(carrier: dict) -> float:
    return _floatish(_get_first(carrier, "Crash_Rate_Per100", "Crash Rate"))


def _equipment(carrier: dict) -> list[str]:
    eq = _get_first(carrier, "Equipment_Types", "Equipment Types") or ""
    return [e.strip().upper() for e in str(eq).split(",") if e.strip()]


# ── Main gate ──────────────────────────────────────────────────────────────


def vet_complete(carrier: dict) -> VettingResult:
    """Evaluate a carrier against every hard-reject rule.

    Order of evaluation:
      1. Fleet size  (0 = needs_review; <3 = fail)
      2. Insurance liability  (0 = needs_review; <$1M = fail)
      3. Insurance cargo  (0 = needs_review; <$100K = fail)
      4. Safety rating  (Unsatisfactory = fail)
      5. Vehicle OOS / Driver OOS / Crash rate (only if numerically present)
      6. Reefer maintenance (zero tolerance if any vehicle OOS inspection)
      7. Shell carrier (units > 0 with 0 drivers AND drivers field present)

    Returns a VettingResult; passed=True only if status == pass_basic.
    """
    now = datetime.now(timezone.utc).isoformat()

    fleet = _fleet(carrier)
    if fleet == 0:
        return VettingResult(
            passed=False,
            status=NEEDS_REVIEW,
            reason="fleet size missing/blank — cannot verify >=3 minimum",
            checked_at=now,
        )
    if fleet < RULES.fleet_min:
        return VettingResult(
            passed=False,
            status=FAIL_FLEET,
            reason=f"{fleet} power units below {RULES.fleet_min} minimum",
            checked_at=now,
        )

    liab = _liability(carrier)
    if liab == 0:
        return VettingResult(
            passed=False,
            status=NEEDS_REVIEW,
            reason="liability insurance missing/blank — cannot verify $1M minimum",
            checked_at=now,
        )
    if liab < RULES.liability_min:
        return VettingResult(
            passed=False,
            status=FAIL_LIABILITY,
            reason=f"liability ${liab:,} below ${RULES.liability_min:,} minimum",
            checked_at=now,
        )

    cargo = _cargo(carrier)
    # Cargo check is gated on RULES.cargo_min. When cargo_min == 0 (current
    # state — see app/vetting/rules.py for the why), blank/zero cargo is
    # accepted because FMCSA does not publish cargo filings for general-freight
    # carriers. Verification of cargo coverage moves to onboarding time, when a
    # real COI is collected. If Derek raises cargo_min above 0 later, the
    # blank-cargo → needs_review behavior returns automatically.
    if RULES.cargo_min > 0:
        if cargo == 0:
            return VettingResult(
                passed=False,
                status=NEEDS_REVIEW,
                reason=f"cargo insurance missing/blank — cannot verify ${RULES.cargo_min:,} minimum",
                checked_at=now,
            )
        if cargo < RULES.cargo_min:
            return VettingResult(
                passed=False,
                status=FAIL_CARGO,
                reason=f"cargo ${cargo:,} below ${RULES.cargo_min:,} minimum",
                checked_at=now,
            )

    safety = _safety(carrier)
    if safety in ("UNSATISFACTORY", "U"):
        return VettingResult(
            passed=False,
            status=FAIL_SAFETY,
            reason="unsatisfactory safety rating",
            checked_at=now,
        )

    veh_oos = _vehicle_oos(carrier)
    if veh_oos > RULES.vehicle_oos_max_pct:
        return VettingResult(
            passed=False,
            status=FAIL_VEHICLE_OOS,
            reason=f"vehicle OOS rate {veh_oos:.1f}% exceeds {RULES.vehicle_oos_max_pct}%",
            checked_at=now,
        )

    drv_oos = _driver_oos(carrier)
    if drv_oos > RULES.driver_oos_max_pct:
        return VettingResult(
            passed=False,
            status=FAIL_DRIVER_OOS,
            reason=f"driver OOS rate {drv_oos:.1f}% exceeds {RULES.driver_oos_max_pct}%",
            checked_at=now,
        )

    crash = _crash_rate(carrier)
    if crash > RULES.crash_rate_max_per_100:
        return VettingResult(
            passed=False,
            status=FAIL_CRASH,
            reason=f"crash rate {crash:.1f}/100 units exceeds {RULES.crash_rate_max_per_100}",
            checked_at=now,
        )

    equipment = _equipment(carrier)
    is_reefer = any("REEFER" in e for e in equipment)
    if is_reefer:
        veh_oos_insp = _intish(carrier.get("Vehicle_OOS_Insp", 0))
        if veh_oos_insp > 0:
            return VettingResult(
                passed=False,
                status=FAIL_REEFER,
                reason=f"reefer carrier with {veh_oos_insp} vehicle OOS inspection(s) — zero tolerance",
                checked_at=now,
            )

    # Shell check only if Driver_Count is present and explicitly zero
    drivers_raw = _get_first(carrier, "Driver_Count", "Drivers", "Total_Drivers")
    if drivers_raw is not None:
        drivers = _intish(drivers_raw)
        if drivers == 0 and fleet > 0:
            return VettingResult(
                passed=False,
                status=FAIL_SHELL,
                reason=f"0 drivers with {fleet} power units (shell/stale carrier)",
                checked_at=now,
            )

    return VettingResult(
        passed=True,
        status=PASS_BASIC,
        reason=f"fleet={fleet}, liability=${liab:,}, cargo=${cargo:,}, safety={safety or 'NONE'}",
        checked_at=now,
    )


def is_carrier_vetted(carrier: dict) -> bool:
    """Fast yes/no gate.

    1. If a cached `Vetting Status` / `Vetting_Status` cell is present and
       non-blank, trust it (only `pass_basic` returns True).
    2. Otherwise compute fresh via vet_complete().
    """
    status = (
        carrier.get("Vetting_Status")
        or carrier.get("Vetting Status")
        or ""
    )
    status = str(status).strip().lower()
    if status:
        return status == PASS_BASIC
    # No cached result — compute inline (will return False for empty dicts
    # because fleet=0 → needs_review)
    return vet_complete(carrier).passed
