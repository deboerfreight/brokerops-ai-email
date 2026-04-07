"""
BrokerOps AI – Carrier email enrichment (4-step waterfall).

Steps:
  1. SAFER scrape   – parse FMCSA SAFER snapshot for contact email
  2. Google CSE     – custom search (stubbed until keys configured)
  3. Apollo.io      – org enrichment + people search
  4. PHONE_ONLY     – fallback when no email found
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

_SAFER_SNAPSHOT_URL = "https://safer.fmcsa.dot.gov/query.asp"
_APOLLO_ORG_URL = "https://api.apollo.io/api/v1/organizations/enrich"
_APOLLO_PEOPLE_URL = "https://api.apollo.io/v1/mixed_people/search"
_GOOGLE_CSE_URL = "https://www.googleapis.com/customsearch/v1"

# Domains that show up in SAFER HTML but aren't real carrier emails
_IGNORED_EMAIL_DOMAINS = {"fmcsa.dot.gov", "dot.gov", "safer.fmcsa.dot.gov"}


# ── Public API ────────────────────────────────────────────────────────────────

def enrich_carrier_email(carrier: dict[str, Any]) -> dict[str, Any]:
    """Run the 4-step waterfall and return {email, source, website}."""
    dot = str(carrier.get("DOT_Number") or "").strip()
    mc = str(carrier.get("MC_Number") or "").strip()
    name = str(carrier.get("Legal_Name") or "").strip()
    city = str(carrier.get("City") or "").strip()
    state = str(carrier.get("State") or "").strip()

    logger.info("Enriching email for %s (DOT=%s, MC=%s)", name, dot, mc)

    website: str | None = None

    # Step 1 — SAFER scrape
    result = _scrape_safer(dot)
    if result:
        if result.get("email"):
            logger.info("SAFER hit for DOT %s: %s", dot, result["email"])
            return {"email": result["email"], "source": "SAFER", "website": result.get("website")}
        website = result.get("website")

    time.sleep(0.5)

    # Step 2 — Google CSE
    cse_result = _search_google_cse(name, state)
    if cse_result:
        logger.info("Google CSE hit for %s: %s", name, cse_result["email"])
        return {"email": cse_result["email"], "source": "GOOGLE_CSE", "website": cse_result.get("website") or website}

    time.sleep(0.5)

    # Step 3 — Apollo
    apollo_result = _search_apollo(name, website)
    if apollo_result:
        logger.info("Apollo hit for %s: %s", name, apollo_result["email"])
        return {"email": apollo_result["email"], "source": "APOLLO", "website": apollo_result.get("website") or website}

    # Step 4 — fallback
    logger.info("No email found for %s — falling back to PHONE_ONLY", name)
    return {"email": None, "source": "PHONE_ONLY", "website": website}


# ── Step 1: SAFER ─────────────────────────────────────────────────────────────

def _scrape_safer(dot: str) -> dict[str, Any] | None:
    """Fetch SAFER company snapshot and extract email + website via regex."""
    if not dot:
        return None

    try:
        resp = httpx.post(
            _SAFER_SNAPSHOT_URL,
            data={
                "searchtype": "ANY",
                "query_type": "queryCarrierSnap",
                "query_param": "USDOT",
                "query_string": dot,
            },
            headers={"User-Agent": "BrokerOps-AI/1.0"},
            timeout=15,
            follow_redirects=True,
        )
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("SAFER request failed for DOT %s: %s", dot, exc)
        return None

    html = resp.text

    # Extract website (look for http links that aren't gov sites)
    website: str | None = None
    url_matches = re.findall(r'href=["\']?(https?://[^"\'>\s]+)', html, re.IGNORECASE)
    for url in url_matches:
        lower = url.lower()
        if "fmcsa" not in lower and "dot.gov" not in lower and "safer" not in lower:
            website = url
            break

    # Extract emails
    emails = _EMAIL_RE.findall(html)
    valid_email: str | None = None
    for email in emails:
        domain = email.split("@", 1)[1].lower()
        if domain not in _IGNORED_EMAIL_DOMAINS:
            valid_email = email
            break

    return {"email": valid_email, "website": website}


# ── Step 2: Google CSE (stub) ─────────────────────────────────────────────────

def _search_google_cse(company_name: str, state: str) -> dict[str, Any] | None:
    """Search Google Custom Search for carrier contact email."""
    settings = get_settings()
    if not settings.GOOGLE_CSE_API_KEY or not settings.GOOGLE_CSE_CX:
        logger.debug("Google CSE not configured — skipping")
        return None

    query = f"{company_name} {state} trucking contact email"
    try:
        resp = httpx.get(
            _GOOGLE_CSE_URL,
            params={
                "key": settings.GOOGLE_CSE_API_KEY,
                "cx": settings.GOOGLE_CSE_CX,
                "q": query,
                "num": 5,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as exc:
        logger.warning("Google CSE request failed: %s", exc)
        return None

    # Scan snippets + links for email addresses
    for item in data.get("items", []):
        snippet = item.get("snippet", "") + " " + item.get("link", "")
        emails = _EMAIL_RE.findall(snippet)
        if emails:
            return {"email": emails[0], "website": item.get("link")}

    return None


# ── Step 3: Apollo.io ─────────────────────────────────────────────────────────

def _search_apollo(company_name: str, website: str | None) -> dict[str, Any] | None:
    """Try Apollo org enrichment (if we have a website), then people search."""
    settings = get_settings()
    if not settings.APOLLO_API_KEY:
        logger.debug("Apollo API key not configured — skipping")
        return None

    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
    }
    api_key = settings.APOLLO_API_KEY

    # 3a — Org enrichment (needs a domain)
    if website:
        domain = _extract_domain(website)
        if domain:
            result = _apollo_org_enrich(domain, api_key, headers)
            if result:
                return result
            time.sleep(0.3)

    # 3b — People search by company name
    return _apollo_people_search(company_name, api_key, headers)


def _apollo_org_enrich(
    domain: str, api_key: str, headers: dict[str, str]
) -> dict[str, Any] | None:
    """Apollo organization enrichment by domain."""
    try:
        resp = httpx.get(
            _APOLLO_ORG_URL,
            params={"api_key": api_key, "domain": domain},
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
    company_name: str, api_key: str, headers: dict[str, str]
) -> dict[str, Any] | None:
    """Apollo people search — find a contact at the company with an email."""
    if not company_name:
        return None

    payload = {
        "api_key": api_key,
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
