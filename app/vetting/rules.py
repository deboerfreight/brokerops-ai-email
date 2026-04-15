"""BrokerOps AI – Vetting rule thresholds (single source of truth).

Change values here to update gates, sweeps, and writers in lockstep. Any
hard-coded threshold elsewhere is a bug — update it to read from RULES.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class VettingRules:
    fleet_min: int = 3
    liability_min: int = 1_000_000
    # cargo_min dropped to 0 on 2026-04-14 after empirical verification that
    # FMCSA does not publish cargo insurance filings for general-freight carriers.
    # Federal cargo filings exist only for HHG (household goods) carriers. General-
    # freight cargo coverage is contractual, verified during ONBOARDING from a real
    # carrier COI — not at prospect-time from public data. The $100K rule still
    # applies; enforcement point moved to onboarding.py where a COI is collected.
    cargo_min: int = 0
    vehicle_oos_max_pct: float = 30.0
    driver_oos_max_pct: float = 15.0
    crash_rate_max_per_100: float = 30.0

    # Reefer-specific stricter rules (replaced 2026-04-15 — old rule was "any
    # vehicle OOS inspection = reject" which was mathematically impossible for
    # any mid-size or larger reefer carrier to clear. New rate-based rule is
    # roughly half the national average for vehicle OOS, still meaningfully
    # stricter than the 30% floor for general freight).
    reefer_vehicle_oos_max_pct: float = 10.0
    # Minimum vehicle inspection count before trusting a carrier's OOS rate.
    # Prevents the "0 inspections = safe" false positive for carriers with
    # insufficient data history. Carriers under this floor get needs_review.
    reefer_min_inspection_count: int = 10

    stale_data_days: int = 30  # re-fetch FMCSA if last_checked older than this


RULES = VettingRules()
