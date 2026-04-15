"""
BrokerOps AI – Known carrier-directory URL patterns for DOT-number lookups.

Probed against live traffic on 2026-04-14 with headless Chromium. Each pattern
is keyed by a short site id so callers can log / rate-limit per-hostname.

Patterns support {dot} and {state_lower}. If a pattern requires {state_lower}
and the carrier has no HQ state, that pattern is skipped.

Probe results (2026-04-14):
    brokersnapshot : HIT on 5/5 probed DOTs — returns role-based and
                     gmail-based emails cleanly
    dotreport      : HIT on 4/5 probed DOTs — uppercase emails, useful
                     confirmatory cross-check
    carriernetwork : 404 on every DOT-profile URL tried. DISABLED.
    quicktransport : redirect-loop without carrier-name slug. DISABLED for now
                     (would need a search step).
    partnercarrier : connection timeout / unreachable. DISABLED.

Add more patterns here as they're verified. Do not enable an unverified
pattern — the whole point of the 2026-04-13 root-cause was that plain
requests-based scraping was silently hitting dead directories.
"""
from __future__ import annotations

from typing import Optional


# (site_id, pattern_template, requires_state)
DIRECTORY_URL_PATTERNS: list[tuple[str, str, bool]] = [
    ("brokersnapshot", "https://brokersnapshot.com/Company?dot={dot}", False),
    ("dotreport",      "https://dot.report/usdot/{dot}",               False),
]


def build_urls(dot: str, state: Optional[str] = None) -> list[tuple[str, str]]:
    """Return [(site_id, url), ...] for all directory patterns applicable.

    Patterns that require {state_lower} are skipped if `state` is blank.
    """
    if not dot:
        return []
    state_lower = (state or "").strip().lower()
    out: list[tuple[str, str]] = []
    for site_id, tpl, requires_state in DIRECTORY_URL_PATTERNS:
        if requires_state and not state_lower:
            continue
        try:
            url = tpl.format(dot=dot, state_lower=state_lower)
        except KeyError:
            continue
        out.append((site_id, url))
    return out


def hostname_for(site_id: str) -> str:
    """Return the primary hostname for a site id — used by the rate limiter."""
    mapping = {
        "brokersnapshot": "brokersnapshot.com",
        "dotreport": "dot.report",
        "carriernetwork": "carriernetwork.ai",
        "quicktransportsolutions": "quicktransportsolutions.com",
        "partnercarrier": "partnercarrier.com",
    }
    return mapping.get(site_id, site_id)
