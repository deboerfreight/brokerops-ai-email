"""
BrokerOps AI – Carrier email enrichment (3-step waterfall).

Steps:
  1. Apollo.io      – org enrichment by domain + people search (primary email source)
  2. Brave Search   – web search fallback via Brave Search API (BRAVE_SEARCH_API_KEY in .env)
  3. PHONE_ONLY     – last resort when no email found

Note: SAFER scraping (_scrape_safer) was removed 2026-04-15. Root cause: SAFER web portal
returns JS-gated content when hit with plain httpx.post() — bot-block identified 2026-04-13.
The Playwright enrichment path (scripts/enrich_carriers_playwright.py) handles website
discovery for carriers where Apollo/Brave miss. See project_brokerops_enrichment_gap.md.

Google CSE (_search_google_cse) was attempted 2026-04-15 and dropped the same day — Google
deprecated the "Search the entire web" toggle in the CSE console, making it unusable for
open-web carrier discovery. Replaced by Brave Search API. CSE keys remain in vault but
are dormant; prune after Brave is proven in production.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any

import httpx

from app.config import get_settings

logger = logging.getLogger("brokerops.email_enrichment")

_EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}")

_APOLLO_ORG_URL = "https://api.apollo.io/api/v1/organizations/enrich"
_APOLLO_PEOPLE_URL = "https://api.apollo.io/api/v1/people/search"
_BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"


# ── Public API ────────────────────────────────────────────────────────────────

def enrich_carrier_email(carrier: dict[str, Any]) -> dict[str, Any]:
    """Run the 3-step waterfall and return {email, source, website}.

    Waterfall:
      1. Apollo.io   — org enrichment + people search (parked on free tier; limited results)
      2. Brave Search — open-web discovery via Brave Search API (BRAVE_SEARCH_API_KEY)
      3. PHONE_ONLY  — last resort; carrier has a phone number, outreach via voice instead
    """
    dot = str(carrier.get("DOT_Number") or "").strip()
    mc = str(carrier.get("MC_Number") or "").strip()
    name = str(carrier.get("Legal_Name") or "").strip()
    city = str(carrier.get("City") or "").strip()
    state = str(carrier.get("State") or "").strip()

    logger.info("Enriching email for %s (DOT=%s, MC=%s)", name, dot, mc)

    website: str | None = None

    # Step 1 — Apollo (primary source for contact emails)
    apollo_result = _search_apollo(name, website)
    if apollo_result:
        logger.info("Apollo hit for %s: %s", name, apollo_result["email"])
        return {"email": apollo_result["email"], "source": "APOLLO", "website": apollo_result.get("website") or website}

    time.sleep(0.5)

    # Step 2 — Brave Search
    brave_result = _search_brave(name, state)
    if brave_result:
        discovered_email = brave_result["email"]
        discovered_website = brave_result.get("website") or ""
        confidence = _domain_match_confidence(discovered_email, discovered_website, name)
        if confidence == "LOW":
            logger.info(
                "Brave Search found email %s for %s but domain-match failed "
                "(website=%s) — downgrading to PHONE_ONLY; website still recorded",
                discovered_email, name, discovered_website,
            )
            # Website discovery is still useful even when email is rejected
            return {"email": None, "source": "PHONE_ONLY", "website": discovered_website or website}
        logger.info("Brave Search hit for %s: %s (confidence=%s)", name, discovered_email, confidence)
        return {"email": discovered_email, "source": "BRAVE_SEARCH", "website": discovered_website or website}

    time.sleep(0.5)

    # Step 3 — PHONE_ONLY
    logger.info("No email found for %s — falling back to PHONE_ONLY", name)
    return {"email": None, "source": "PHONE_ONLY", "website": website}


# ── Step 2: Brave Search ──────────────────────────────────────────────────────

def _search_brave(company_name: str, state: str) -> dict[str, Any] | None:
    """Search Brave Search API for carrier contact email.

    Auth:  X-Subscription-Token header (NOT a query param).
    Rate:  free tier = 1 req/sec, 2 000 req/month. A 1.1-second sleep is
           applied after every successful call to stay inside the rate limit.
    Retry: single 2-second retry on HTTP 429; give up after that.
    Key:   BRAVE_SEARCH_API_KEY in .env (via vault → hydrate_from_vault).
    """
    import os as _os

    api_key = _os.environ.get("BRAVE_SEARCH_API_KEY", "").strip()
    if not api_key:
        logger.warning("BRAVE_SEARCH_API_KEY not set — skipping Brave Search step")
        return None

    query = f"{company_name} {state} trucking contact email"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": api_key,
    }

    def _do_request() -> httpx.Response:
        return httpx.get(
            _BRAVE_SEARCH_URL,
            params={"q": query, "count": 10},
            headers=headers,
            timeout=15,
        )

    try:
        resp = _do_request()
        if resp.status_code == 429:
            logger.warning("Brave Search 429 — rate limited; retrying in 2s")
            time.sleep(2)
            resp = _do_request()
        if resp.status_code in (401, 403):
            logger.error(
                "Brave Search auth error %d — check BRAVE_SEARCH_API_KEY; skipping",
                resp.status_code,
            )
            return None
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as exc:
        logger.warning("Brave Search request failed: %s", exc)
        return None

    results = (data.get("web") or {}).get("results", [])
    logger.info("Brave Search: query=%r  results=%d", query, len(results))

    # Respect free-tier 1 req/sec limit
    time.sleep(1.1)

    # Scan description (snippet) + url for email addresses
    for item in results:
        text = (item.get("description") or "") + " " + (item.get("url") or "")
        emails = _EMAIL_RE.findall(text)
        if emails:
            return {"email": emails[0], "website": item.get("url")}

    return None


# ── DEPRECATED: Google CSE (dropped 2026-04-15) ───────────────────────────────

def _search_google_cse(*args, **kwargs):  # type: ignore[override]
    """DEPRECATED — Google CSE dropped 2026-04-15.

    Google deprecated the 'Search the entire web' toggle in the CSE console,
    making open-web carrier discovery impossible via CSE. Use _search_brave().
    """
    import warnings
    warnings.warn(
        "_search_google_cse() is deprecated and no longer functional. "
        "Use _search_brave() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    raise DeprecationWarning(
        "_search_google_cse() was removed 2026-04-15 — use _search_brave()"
    )


# ── Step 1: Apollo.io ─────────────────────────────────────────────────────────

def _search_apollo(company_name: str, website: str | None) -> dict[str, Any] | None:
    """Try Apollo org enrichment (if we have a website), then people search."""
    settings = get_settings()
    if not settings.APOLLO_API_KEY:
        logger.debug("Apollo API key not configured — skipping")
        return None

    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": settings.APOLLO_API_KEY,
    }

    # 3a — Org enrichment (needs a domain, requires paid tier)
    if website:
        domain = _extract_domain(website)
        if domain:
            result = _apollo_org_enrich(domain, headers)
            if result:
                return result
            time.sleep(0.3)

    # 3b — People search by company name
    return _apollo_people_search(company_name, headers)


def _apollo_org_enrich(
    domain: str, headers: dict[str, str]
) -> dict[str, Any] | None:
    """Apollo organization enrichment by domain."""
    try:
        resp = httpx.get(
            _APOLLO_ORG_URL,
            params={"domain": domain},
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as exc:
        logger.warning("Apollo org enrichment failed for %s: %s", domain, exc)
        return None

    org = data.get("organization") or {}

    # Check for a primary email on the org
    email = org.get("primary_email")
    if email:
        return {"email": email, "website": org.get("website_url")}

    return None


def _apollo_people_search(
    company_name: str, headers: dict[str, str]
) -> dict[str, Any] | None:
    """Apollo people search — find a contact at the company with an email."""
    if not company_name:
        return None

    payload = {
        "q_organization_name": company_name,
        "page": 1,
        "per_page": 5,
        "person_titles": ["owner", "president", "dispatch", "operations", "manager"],
    }

    try:
        resp = httpx.post(
            _APOLLO_PEOPLE_URL,
            json=payload,
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as exc:
        logger.warning("Apollo people search failed for %s: %s", company_name, exc)
        return None

    for person in data.get("people", []):
        email = person.get("email")
        if email:
            org = person.get("organization") or {}
            return {"email": email, "website": org.get("website_url")}

    return None


# ── Helpers ───────────────────────────────────────────────────────────────────

# Domains that are directory listings / aggregator sites, NOT the carrier's own
# website.  When the discovered website is one of these, we can't use the domain
# to validate the email — fall back to name-match logic.
_DIRECTORY_DOMAINS = {
    "brokersnapshot.com",
    "dotreport.io",
    "carrier411.com",
    "fmcsa.dot.gov",
    "safer.fmcsa.dot.gov",
    "bbb.org",
    "linkedin.com",
    "indeed.com",
    "yelp.com",
    "yellowpages.com",
    "manta.com",
    "bizapedia.com",
    "opencorporates.com",
}

# Personal/free email providers — never treated as a valid business domain match.
_PERSONAL_EMAIL_PROVIDERS = {
    "yahoo.com",
    "gmail.com",
    "hotmail.com",
    "outlook.com",
    "aol.com",
    "icloud.com",
    "protonmail.com",
    "proton.me",
    "me.com",
    "live.com",
    "msn.com",
}


def _bare_domain(url_or_domain: str) -> str:
    """Strip protocol, www., and path from a URL; return bare domain lowercase."""
    d = url_or_domain.strip().lower()
    for prefix in ("https://", "http://"):
        if d.startswith(prefix):
            d = d[len(prefix):]
    # Strip leading www. (and any single subdomain like mail./m. etc.)
    if d.startswith("www."):
        d = d[4:]
    d = d.split("/", 1)[0].strip()
    return d


def _domain_core(domain: str) -> str:
    """Return the registrable domain core: last two dot-segments (e.g. 'ctsls-usa.com')."""
    parts = domain.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return domain


def _token_overlap_ratio(a: str, b: str) -> float:
    """Simple token-overlap ratio (0.0–1.0) between two strings.

    Splits on non-alphanumeric characters, finds intersection size / union size.
    Used as a poor-man's fuzzy match when rapidfuzz is not installed.
    """
    import re as _re
    tok_a = set(_re.split(r"[^a-z0-9]+", a.lower())) - {""}
    tok_b = set(_re.split(r"[^a-z0-9]+", b.lower())) - {""}
    if not tok_a and not tok_b:
        return 1.0
    if not tok_a or not tok_b:
        return 0.0
    intersection = tok_a & tok_b
    union = tok_a | tok_b
    return len(intersection) / len(union)


def _length_ratio(a: str, b: str) -> float:
    """Length similarity ratio — 1.0 if equal length, approaches 0 as lengths diverge."""
    la, lb = len(a), len(b)
    if la == 0 and lb == 0:
        return 1.0
    return min(la, lb) / max(la, lb)


def _char_ngram_ratio(a: str, b: str, n: int = 3) -> float:
    """Character n-gram Jaccard similarity between two strings.

    Strips non-alphanumeric chars before computing.  Good at catching
    acronym/abbreviation variants like 'ctsls' vs 'ctslogistics'.
    """
    import re as _re
    a_clean = _re.sub(r"[^a-z0-9]", "", a.lower())
    b_clean = _re.sub(r"[^a-z0-9]", "", b.lower())
    if len(a_clean) < n or len(b_clean) < n:
        # Fall back to substring for very short strings
        return 1.0 if (a_clean in b_clean or b_clean in a_clean) else 0.0
    a_ngrams = {a_clean[i:i + n] for i in range(len(a_clean) - n + 1)}
    b_ngrams = {b_clean[i:i + n] for i in range(len(b_clean) - n + 1)}
    intersection = a_ngrams & b_ngrams
    union = a_ngrams | b_ngrams
    return len(intersection) / len(union) if union else 0.0


def _common_prefix_ratio(a: str, b: str) -> float:
    """Fraction of the shorter string that is a common prefix."""
    import re as _re
    a_clean = _re.sub(r"[^a-z0-9]", "", a.lower())
    b_clean = _re.sub(r"[^a-z0-9]", "", b.lower())
    shorter = min(len(a_clean), len(b_clean))
    if shorter == 0:
        return 0.0
    prefix_len = 0
    for ca, cb in zip(a_clean, b_clean):
        if ca != cb:
            break
        prefix_len += 1
    return prefix_len / shorter


def _fuzzy_domain_match(a: str, b: str) -> float:
    """Return a [0, 1] similarity score between two domain-core strings.

    Strategy (no external deps):
    1. Exact match → 1.0
    2. Substring either way → 0.85
    3. Common-prefix ratio >= 0.5 (catches acronym stems like cts/ctsls/ctslogistics) → 0.75
    4. Character trigram Jaccard overlap — picks up shared substrings
    5. Token overlap ratio weighted with trigram ratio and length ratio
    """
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    # Strip TLD for comparison (compare 'ctsls-usa' vs 'ctslogisticssolutions')
    a_stem = a.rsplit(".", 1)[0] if "." in a else a
    b_stem = b.rsplit(".", 1)[0] if "." in b else b
    # Exact stem match
    if a_stem == b_stem:
        return 1.0
    # Substring match (either direction) on cleaned stems
    import re as _re
    a_c = _re.sub(r"[^a-z0-9]", "", a_stem.lower())
    b_c = _re.sub(r"[^a-z0-9]", "", b_stem.lower())
    if a_c in b_c or b_c in a_c:
        return 0.85
    # Common-prefix ratio (catches ctsls vs ctslogisticssolutions)
    prefix_ratio = _common_prefix_ratio(a_stem, b_stem)
    if prefix_ratio >= 0.5:
        return 0.75
    # Character trigram overlap
    trigram_ratio = _char_ngram_ratio(a_stem, b_stem, n=3)
    # Token overlap
    tok_ratio = _token_overlap_ratio(a_stem, b_stem)
    len_ratio = _length_ratio(a_stem, b_stem)
    # Weighted combination — trigram is the strongest signal here
    combined = 0.5 * trigram_ratio + 0.3 * tok_ratio + 0.2 * len_ratio
    return combined


def _domain_match_confidence(email: str, website: str, carrier_name: str) -> str:
    """Gate an email candidate against a discovered website.

    Returns:
        "HIGH"   — domain match or strong fuzzy match
        "MEDIUM" — moderate fuzzy match (token overlap)
        "LOW"    — no meaningful match → caller should downgrade to PHONE_ONLY
    """
    if not email:
        return "LOW"

    email_lower = email.strip().lower()
    # Extract email domain
    if "@" not in email_lower:
        return "LOW"
    email_domain = email_lower.split("@", 1)[1]
    email_domain_core = _domain_core(email_domain)

    # Personal provider → always LOW when website is a real carrier domain
    if email_domain_core in _PERSONAL_EMAIL_PROVIDERS or email_domain in _PERSONAL_EMAIL_PROVIDERS:
        logger.debug(
            "domain_match: %s is a personal email provider → LOW confidence", email_domain
        )
        return "LOW"

    # Determine the reference domain to match against
    website_bare = _bare_domain(website) if website else ""
    website_core = _domain_core(website_bare) if website_bare else ""

    is_directory = website_core in _DIRECTORY_DOMAINS or website_bare in _DIRECTORY_DOMAINS

    if is_directory:
        # Fall back to fuzzy match between email domain and carrier name tokens
        logger.debug(
            "domain_match: website %s is a directory listing; matching email domain "
            "against carrier name '%s' instead",
            website_bare, carrier_name,
        )
        score = _token_overlap_ratio(email_domain_core.rsplit(".", 1)[0], carrier_name)
        logger.debug("domain_match: name-vs-email-domain token score=%.2f", score)
        if score >= 0.5:
            return "MEDIUM"
        return "LOW"

    if not website_core:
        # No website at all — can't validate; treat as LOW
        logger.debug("domain_match: no website discovered; cannot validate email → LOW")
        return "LOW"

    score = _fuzzy_domain_match(email_domain_core, website_core)
    logger.debug(
        "domain_match: email_domain=%s website=%s score=%.2f",
        email_domain_core, website_core, score,
    )
    if score >= 0.95:
        return "HIGH"
    if score >= 0.5:
        return "MEDIUM"
    return "LOW"


def _extract_domain(url: str) -> str | None:
    """Pull bare domain from a URL string."""
    url = url.strip().lower()
    if not url:
        return None
    # Strip protocol
    for prefix in ("https://", "http://", "www."):
        if url.startswith(prefix):
            url = url[len(prefix):]
    # Take up to first slash
    domain = url.split("/", 1)[0].strip()
    return domain if "." in domain else None
