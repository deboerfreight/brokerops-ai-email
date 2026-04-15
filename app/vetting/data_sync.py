"""BrokerOps AI – Fresh FMCSA data fetcher for the vetting pipeline.

Wraps `app.fmcsa.get_carrier_details()` (which already pulls /carriers/{dot} +
/cargo-carried + /docket-number + /basics) and returns a normalized dict with
the key shape vetting expects.

Insurance overlay: QCMobile's REST API returns None/0 for BIPD and cargo
for virtually every carrier. We backfill from the FMCSA L&I bulk-file
SQLite lookup (see app/vetting/li_insurance_lookup.py) so vetting has real
insurance amounts. The overlay is non-destructive — it only fills missing
values, never overwrites a real QCMobile response.

Caller is responsible for rate limiting (1 req/sec to FMCSA).
"""
from __future__ import annotations

import logging
from typing import Optional

from app.fmcsa import get_carrier_details
from app.vetting.li_insurance_lookup import get_insurance

logger = logging.getLogger("brokerops.vetting.data_sync")


def _merge_li_insurance(normalized: dict, dot: str) -> dict:
    """Overlay FMCSA L&I bulk-file insurance onto a normalized FMCSA dict.

    Overwrite policy: L&I bulk is authoritative for BIPD (QCMobile's REST
    API returns None/0 for insurance fields across the board). For cargo,
    fill only when normalized is blank — federal cargo filings are sparse
    (HHG-only), so L&I will often be 0 and we don't want to clobber a
    value supplied upstream.

    Safe when the L&I DB hasn't been built yet (get_insurance returns None;
    the normalized dict passes through untouched).
    """
    li = get_insurance(str(dot))
    if not li:
        logger.info("L&I insurance: no record for DOT %s", dot)
        return normalized

    def _is_blank(v) -> bool:
        if v in (None, "", 0, "0"):
            return True
        try:
            return int(v) == 0
        except (TypeError, ValueError):
            return False

    # BIPD: L&I wins (QCMobile always returns None here)
    if li.bipd_liability > 0:
        normalized["Insurance_Liability"] = li.bipd_liability

    # Cargo: fill only blanks; L&I cargo is usually 0 for non-HHG
    if _is_blank(normalized.get("Insurance_Cargo")) and li.cargo > 0:
        normalized["Insurance_Cargo"] = li.cargo

    # Informational fields — always prefer L&I because QCMobile never has them
    if li.insurer_name:
        normalized["Insurance_Company"] = li.insurer_name
    if li.effective_date:
        normalized["Insurance_Effective_Date"] = li.effective_date
    normalized["Insurance_Source"] = f"fmcsa_li_bulk:{li.file_date}"

    logger.info(
        "L&I insurance merged for DOT %s: liability=%d cargo=%d insurer=%s eff=%s",
        dot, li.bipd_liability, li.cargo, li.insurer_name, li.effective_date,
    )
    return normalized


def fetch_fresh_fmcsa(dot: str) -> Optional[dict]:
    """Fetch current FMCSA data for the given DOT number.

    Returns the normalized dict from `get_carrier_details()` which includes:
        DOT_Number, MC_Number, Legal_Name, DBA_Name, City, State, Zip,
        Contact_Phone, Contact_Email, Authority_Status, Authority_Date,
        Insurance_Liability, Insurance_Cargo, Safety_Rating,
        Vehicle_OOS_Rate, Driver_OOS_Rate, Power_Units, Driver_Count,
        Equipment_Types, OOS_Active, Crash_Total, Crash_Rate_Per100, ...

    Also overlays L&I bulk-file insurance (BIPD, cargo, insurer name,
    effective date) when the local L&I lookup DB has a record for the DOT.

    Returns None on API/network failure (logged). Caller decides whether to
    retry, skip, or fall back to stale sheet data.
    """
    if not dot:
        return None
    try:
        result = get_carrier_details(str(dot).strip())
        if not result:
            logger.warning("FMCSA returned empty result for DOT %s", dot)
            return None
        return _merge_li_insurance(result, str(dot).strip())
    except Exception as exc:
        logger.error("FMCSA fetch failed for DOT %s: %s", dot, exc)
        return None
