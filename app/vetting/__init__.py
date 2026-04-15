"""BrokerOps AI – Carrier vetting module.

Single source of truth for carrier hard-reject screening. Every write to the
Carrier Database tab MUST go through `validate_before_write` / `write_validated`.

Public API:
    is_carrier_vetted(carrier) -> bool
    vet_complete(carrier) -> VettingResult
    fetch_fresh_fmcsa(dot) -> dict
    validate_before_write(rows) -> (passes, quarantines)
    write_validated(rows, tab=...) -> summary dict
    RULES (frozen dataclass with thresholds)
"""
from app.vetting.gate import is_carrier_vetted, vet_complete, VettingResult
from app.vetting.rules import RULES
from app.vetting.writer import validate_before_write, write_validated

__all__ = [
    "is_carrier_vetted",
    "vet_complete",
    "VettingResult",
    "RULES",
    "validate_before_write",
    "write_validated",
]
