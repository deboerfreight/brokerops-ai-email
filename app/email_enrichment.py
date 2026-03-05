"""
BrokerOps AI – Email Enrichment Pipeline.

Waterfall strategy to find carrier email addresses:
  1. SAFER website scrape  (FMCSA company snapshot → carrier website → emails)
  2. Google Custom Search   (search for carrier + city + state + email)
  3. Apollo.io API          (organization → people search for dispatch contacts)
  4. Flag as PHONE_ONLY     (carrier goes to phone-outreach queue)

Stops at the first step that yields a usable email.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Optional

import httpx

from app.config import get_settings

logger = logging.getLogger("brokerops.enrichment")

# ── Email ranking priority ──────────────────────────────────────────────────

_EMAIL_PRIORITY = [
    "dispatch",
    "freight",
    "loads",
    "operations",
    "logistics",
    "info",
    "contact",
    "admin",
    "office",
]

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

_NOREPLY_PATTERNS = re.compile(
    r"(noreply|no-reply|do-not-reply|donotreply|mailer-daemon|postmaster|bounce)",
    re.IGNORECASE,
)

# ── Helpers ─────────────────────────────────────────────────────────────────


def _pick_best_email(emails: list[str]) -> Optional[str]:
    """Rank found emails by relevance.  dispatch@ > freight@ > info@ > generic."""
    if not emails:
        return None

    # Deduplicate and lowercase
    seen: set[str] = set()
    unique: list[str] = []
    for e in emails:
        low = e.lower().strip()
        if low not in seen and not _NOREPLY_PATTERNS.search(low):
            seen.add(low)
            unique.append(low)

    if not unique:
        return None
    if len(unique) == 1:
        return unique[0]

    # Score by prefix priority
    def _score(addr: str) -> int:
        local = addr.split("@")[0].lower()
        for i, prefix in enumerate(_EMAIL_PRIORITY):
            if prefix in local:
                return i
        return len(_EMAIL_PRIORITY)  # generic / unknown

    unique.sort(key=_score)
    return unique[0]


def _extract_emails_from_text(text: str) -> list[str]:
    """Pull all email addresses from raw text."""
    return _EMAIL_RE.findall(text)


# ── Step 1: SAFER Website Scrape ───────────────────────────────────────────


def _scrape_safer_website(dot_number: str) -> Optional[str]:
    """Fetch the FMCSA SAFER Company Snapshot page and extract website URL."""
    url = (
        "https://safer.fmcsa.dot.gov/query.asp"
        f"?searchtype=ANY&query_type=queryCarrierSnapshot"
        f"&query_param=USDOT&query_string={dot_number}"
    )
    try:
        resp = httpx.get(url, timeout=15, follow_redirects=True)
        resp.raise_for_status()
        html = resp.text

        # Look for a URL pattern near common labels like "Website" or "URL"
        # SAFER pages embed carrier website as plain text or links
        website_patterns = [
            # <a href="http://...">
            re.compile(r'href=["\']?(https?://[^"\'>\s]+)', re.IGNORECASE),
            # plain-text URLs
            re.compile(r'(https?://(?:www\.)?[a-zA-Z0-9\-]+\.[a-zA-Z]{2,}[^\s<"\']*)', re.IGNORECASE),
        ]

        found_urls: list[str] = []
        for pat in website_patterns:
            found_urls.extend(pat.findall(html))

        # Filter out FMCSA's own URLs and government sites
        carrier_urls = [
            u for u in found_urls
            if not any(skip in u.lower() for skip in [
                "fmcsa.dot.gov", "safer.fmcsa", "dot.gov",
                "googleapis.com", "google.com", "javascript:",
            ])
        ]

        if carrier_urls:
            return carrier_urls[0]
        return None

    except Exception as exc:
        logger.warning("SAFER scrape failed for DOT %s: %s", dot_number, exc)
        return None


def _scrape_website_for_email(url: str) -> list[str]:
    """Fetch a carrier website homepage and extract email addresses."""
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (compatible; BrokerOps-Bot/1.0; "
                "+https://deboerfreight.com)"
            ),
        }
        resp = httpx.get(url, timeout=15, follow_redirects=True, headers=headers)
        resp.raise_for_status()
        text = resp.text

        emails = _extract_emails_from_text(text)

        # Also try to find a contact page link and scrape it
        contact_links = re.findall(
            r'href=["\']?(https?://[^"\'>\s]*contact[^"\'>\s]*)',
            text,
            re.IGNORECASE,
        )
        for link in contact_links[:2]:
            time.sleep(1)  # Respectful delay
            try:
                r2 = httpx.get(link, timeout=10, follow_redirects=True, headers=headers)
                r2.raise_for_status()
                emails.extend(_extract_emails_from_text(r2.text))
            except Exception:
                pass

        return emails

    except Exception as exc:
        logger.warning("Website scrape failed for %s: %s", url, exc)
        return []


# ── Step 2: Google Custom Search ───────────────────────────────────────────


def _google_search_email(
    legal_name: str, city: str, state: str
) -> list[str]:
    """Use Google Custom Search API to find carrier email addresses.

    Requires GOOGLE_CSE_API_KEY and GOOGLE_CSE_CX env vars.
    Free tier: 100 queries/day.
    """
    settings = get_settings()
    api_key = settings.GOOGLE_CSE_API_KEY
    cx = settings.GOOGLE_CSE_CX
    if not api_key or not cx:
        logger.debug("Google CSE not configured — skipping")
        return []

    query = f'"{legal_name}" "{city}" "{state}" email trucking contact'
    url = "https://www.googleapis.com/customsearch/v1"

    try:
        resp = httpx.get(
            url,
            params={"key": api_key, "cx": cx, "q": query, "num": 5},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        emails: list[str] = []
        for item in data.get("items", []):
            snippet = item.get("snippet", "")
            title = item.get("title", "")
            link = item.get("link", "")
            emails.extend(_extract_emails_from_text(snippet))
            emails.extend(_extract_emails_from_text(title))

            # Prioritize freight directory results
            freight_dirs = ["dat.com", "truckstop.com", "123loadboard", "carrierlists"]
            if any(d in link.lower() for d in freight_dirs):
                try:
                    page = httpx.get(link, timeout=10, follow_redirects=True)
                    page.raise_for_status()
                    emails.extend(_extract_emails_from_text(page.text))
                except Exception:
                    pass

        # Cross-reference domains with carrier name to reduce false positives
        name_parts = set(legal_name.lower().split())
        scored: list[str] = []
        unscored: list[str] = []
        for e in emails:
            domain = e.split("@")[1].lower()
            domain_parts = set(domain.replace(".", " ").replace("-", " ").split())
            if name_parts & domain_parts:
                scored.append(e)
            else:
                unscored.append(e)
        return scored + unscored

    except Exception as exc:
        logger.warning("Google CSE search failed for '%s': %s", legal_name, exc)
        return []


# ── Step 3: Apollo.io API ──────────────────────────────────────────────────

# Target titles in priority order
_APOLLO_TITLES = [
    "dispatcher",
    "dispatch",
    "operations manager",
    "fleet manager",
    "owner",
    "operator",
    "logistics",
    "trucking",
    "freight",
    "transportation",
]


def _apollo_lookup(
    legal_name: str, city: str, state: str
) -> tuple[Optional[str], Optional[str]]:
    """Search Apollo.io for a carrier contact email.

    Returns (email, apollo_person_id) or (None, None).
    Free tier: 250 email credits/month — track carefully.
    """
    settings = get_settings()
    api_key = settings.APOLLO_API_KEY
    if not api_key:
        logger.debug("Apollo API key not configured — skipping")
        return None, None

    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
    }
    base = "https://api.apollo.io/v1"

    try:
        # Step A: Organization search
        org_resp = httpx.post(
            f"{base}/mixed_companies/search",
            headers=headers,
            json={
                "api_key": api_key,
                "q_organization_name": legal_name,
                "organization_locations": [f"{city}, {state}"],
                "page": 1,
                "per_page": 3,
            },
            timeout=15,
        )
        org_resp.raise_for_status()
        orgs = org_resp.json().get("organizations", [])
        if not orgs:
            logger.debug("Apollo: no org found for '%s'", legal_name)
            return None, None

        org_id = orgs[0].get("id")

        # Step B: People search within that org, targeting dispatch titles
        people_resp = httpx.post(
            f"{base}/mixed_people/search",
            headers=headers,
            json={
                "api_key": api_key,
                "organization_ids": [org_id],
                "page": 1,
                "per_page": 10,
                "person_titles": _APOLLO_TITLES,
            },
            timeout=15,
        )
        people_resp.raise_for_status()
        people = people_resp.json().get("people", [])

        if not people:
            logger.debug("Apollo: no people found at org %s", org_id)
            return None, None

        # Pick the best match by title priority
        def _title_score(person: dict) -> int:
            title = (person.get("title") or "").lower()
            for i, t in enumerate(_APOLLO_TITLES):
                if t in title:
                    return i
            return len(_APOLLO_TITLES)

        people.sort(key=_title_score)
        best = people[0]
        email = best.get("email")
        person_id = best.get("id")

        if email:
            logger.info(
                "Apollo: found %s (%s) at '%s'",
                email,
                best.get("title", "?"),
                legal_name,
            )
            return email, person_id

        return None, None

    except Exception as exc:
        logger.warning("Apollo lookup failed for '%s': %s", legal_name, exc)
        return None, None


# ── Main waterfall pipeline ────────────────────────────────────────────────


def enrich_carrier_email(carrier: dict) -> dict:
    """Run the waterfall enrichment pipeline for a single carrier.

    Args:
        carrier: dict with at least DOT_Number, Legal_Name, City, State.

    Returns:
        dict with keys:
            email:        str | None — best email found (or None)
            source:       str — SAFER_WEBSITE | GOOGLE | APOLLO | PHONE_ONLY
            website:      str | None — carrier website URL if found
            apollo_id:    str | None — Apollo person ID if used
    """
    dot = carrier.get("DOT_Number", "")
    name = carrier.get("Legal_Name", "")
    city = carrier.get("City", "")
    state = carrier.get("State", "")
    mc = carrier.get("MC_Number", "?")

    result = {
        "email": None,
        "source": "PHONE_ONLY",
        "website": None,
        "apollo_id": None,
    }

    # ── Step 1: SAFER → carrier website → scrape emails ─────────────
    if dot:
        logger.info("Enrichment [%s]: Step 1 — SAFER scrape for DOT %s", mc, dot)
        website_url = _scrape_safer_website(dot)
        if website_url:
            result["website"] = website_url
            logger.info("Enrichment [%s]: found website %s", mc, website_url)
            time.sleep(1)  # Respectful delay
            emails = _scrape_website_for_email(website_url)
            best = _pick_best_email(emails)
            if best:
                result["email"] = best
                result["source"] = "SAFER_WEBSITE"
                logger.info("Enrichment [%s]: email via SAFER_WEBSITE: %s", mc, best)
                return result

    # ── Step 2: Google Custom Search ────────────────────────────────
    if name:
        logger.info("Enrichment [%s]: Step 2 — Google search", mc)
        emails = _google_search_email(name, city, state)
        best = _pick_best_email(emails)
        if best:
            result["email"] = best
            result["source"] = "GOOGLE"
            logger.info("Enrichment [%s]: email via GOOGLE: %s", mc, best)
            return result

    # ── Step 3: Apollo.io ──────────────────────────────────────────
    if name:
        logger.info("Enrichment [%s]: Step 3 — Apollo.io lookup", mc)
        email, person_id = _apollo_lookup(name, city, state)
        if email:
            result["email"] = email
            result["source"] = "APOLLO"
            result["apollo_id"] = person_id
            logger.info("Enrichment [%s]: email via APOLLO: %s", mc, email)
            return result

    # ── Step 4: Flag for phone outreach ────────────────────────────
    logger.info(
        "Enrichment [%s]: no email found — flagging as PHONE_ONLY", mc
    )
    return result
