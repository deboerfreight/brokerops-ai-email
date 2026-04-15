"""
BrokerOps AI – Playwright-backed page fetcher for carrier enrichment.

Replaces the `requests`/`httpx` fetcher used in the existing enrichment
waterfall (app/email_enrichment.py). The diagnosis on 2026-04-13 showed that
SAFER, DuckDuckGo Lite, and 5 of 7 carrier directories were returning either
JS-disabled placeholders or bot-block walls when hit with plain requests.
This module uses a real headless Chromium so pages render exactly as a
browser sees them.

Single-browser/per-run lifecycle:
  with PlaywrightFetcher() as fetcher:
      result = fetcher.fetch_page("https://brokersnapshot.com/Company?dot=...")

Result dict shape:
  {
    "url":        requested URL
    "final_url":  URL after redirects
    "status":     HTTP status (int) or None
    "title":      page <title> text
    "html":       full rendered HTML
    "text":       body.innerText
    "emails":     list[str]  — de-duped, regex + mailto-link union
    "mailto_links": list[str]
    "blocked":    bool — True if sanity gate tripped (captcha/JS-gate/etc.)
    "block_reason": Optional[str]
    "error":      Optional[str] — navigation/timeout exception
  }
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from playwright.sync_api import (
    Error as PlaywrightError,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

logger = logging.getLogger("brokerops.enrichment.playwright_fetcher")

# Standard email regex — liberal enough to catch obfuscated formats, we filter
# the noise below.
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

# Image/asset suffixes that sneak into the regex via filenames like
# "logo@2x.png" or CDN hashes.
_IMAGE_SUFFIXES = (
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp",
    ".ico", ".woff", ".woff2", ".ttf", ".eot", ".css", ".js",
)

# Junk / placeholder emails that show up on directory pages but are not the
# carrier's email.
_JUNK_PREFIXES = (
    "example@", "test@", "testing@", "noreply@", "no-reply@", "donotreply@",
    "sentry@", "wordpress@", "admin@example", "info@example",
    "you@", "your@", "your-email@", "youremail@", "name@", "yourname@",
    "email@", "user@", "username@", "firstname@", "lastname@",
    "first.last@", "someone@", "sample@", "demo@", "placeholder@",
)

# Specific placeholder addresses that sneak past prefix matching
_JUNK_EXACT = {
    "user@domain.com", "email@domain.com", "name@domain.com",
    "you@example.com", "your-email@example.com", "name@example.com",
    "user@example.com", "email@example.com",
}

_JUNK_DOMAINS = {
    "example.com", "example.org", "sentry.io", "sentry-next.wixpress.com",
    "wixpress.com", "gstatic.com", "googleapis.com", "google.com",
    "schema.org", "w3.org", "fmcsa.dot.gov", "dot.gov",
    "rocketreach.co", "carriernetwork.ai", "brokersnapshot.com",
    "dot.report", "quicktransportsolutions.com", "partnercarrier.com",
    "carrierlookup.com", "sentry.wixpress.com",
}

# Sanity-gate phrases. If the rendered body text contains any of these we
# treat the page as un-fetched (Bolt's v2 pattern).
_BLOCK_PHRASES = (
    "requires scripting",
    "enable javascript",
    "please enable js",
    "access denied",
    "captcha",
    "verify you are human",
    "checking your browser",
    "cloudflare",
    "attention required",
    "ddos protection",
    "just a moment",
)


def _is_junk_email(email: str) -> bool:
    """True if the string matches the email regex but is not a real address."""
    el = email.lower()
    if el in _JUNK_EXACT:
        return True
    if any(el.endswith(suf) for suf in _IMAGE_SUFFIXES):
        return True
    if any(el.startswith(pref) for pref in _JUNK_PREFIXES):
        return True
    # obfuscated formats like dispxxxx@xxxxxxx.xxx
    if "xxxx" in el:
        return True
    try:
        local, domain = el.split("@", 1)
    except ValueError:
        return True
    if not local or not domain:
        return True
    if domain in _JUNK_DOMAINS:
        return True
    if domain in ("domain.com", "yourdomain.com", "yoursite.com", "website.com"):
        return True
    # reject if domain is clearly a hash/asset path
    if any(domain.endswith(suf) for suf in _IMAGE_SUFFIXES):
        return True
    return False


def extract_emails_from_html(html: str) -> list[str]:
    """Regex-extract and de-junk emails from a blob of HTML/text."""
    found: set[str] = set()
    for raw in EMAIL_RE.findall(html or ""):
        if _is_junk_email(raw):
            continue
        found.add(raw.strip())
    # Normalize case — emails are case-insensitive on the local part by convention
    # but we preserve original-case; dedupe case-insensitively.
    seen_ci: dict[str, str] = {}
    for e in found:
        key = e.lower()
        if key not in seen_ci:
            seen_ci[key] = e
    return sorted(seen_ci.values(), key=lambda s: s.lower())


def _sanity_gate(title: str, text: str) -> tuple[bool, Optional[str]]:
    """Return (blocked, reason). True means don't trust this page's content."""
    blob = f"{title}\n{text}".lower()
    if not text.strip():
        return True, "empty_body"
    for phrase in _BLOCK_PHRASES:
        if phrase in blob:
            return True, f"block_phrase:{phrase}"
    return False, None


class PlaywrightFetcher:
    """Reusable headless-Chromium page fetcher.

    Usage:
        with PlaywrightFetcher() as fetcher:
            result = fetcher.fetch_page(url)
    """

    def __init__(
        self,
        user_agent: str = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        default_timeout_ms: int = 30_000,
        fresh_context_per_request: bool = True,
    ):
        """
        fresh_context_per_request: if True, create a new browser context for
            every fetch_page call. Defeats session-based fingerprinting (which
            Cloudflare uses to 403 long-running scraper sessions). Slightly
            slower but ~100% reliability vs. ~20% on shared contexts.
        """
        self._user_agent = user_agent
        self._default_timeout = default_timeout_ms
        self._fresh_context = fresh_context_per_request
        self._pw = None
        self._browser = None
        self._context = None  # only used when fresh_context_per_request=False

    def __enter__(self) -> "PlaywrightFetcher":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _new_context(self):
        ctx = self._browser.new_context(
            user_agent=self._user_agent,
            viewport={"width": 1366, "height": 900},
            java_script_enabled=True,
            ignore_https_errors=True,
        )
        def _route(route):
            rt = route.request.resource_type
            if rt in ("image", "media", "font"):
                return route.abort()
            return route.continue_()
        try:
            ctx.route("**/*", _route)
        except Exception:
            pass
        return ctx

    def start(self) -> None:
        if self._pw is not None:
            return
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=True)
        if not self._fresh_context:
            self._context = self._new_context()
        logger.info(
            "PlaywrightFetcher started (headless Chromium, fresh_context=%s)",
            self._fresh_context,
        )

    def close(self) -> None:
        try:
            if self._context is not None:
                self._context.close()
        except Exception:
            pass
        try:
            if self._browser is not None:
                self._browser.close()
        except Exception:
            pass
        try:
            if self._pw is not None:
                self._pw.stop()
        except Exception:
            pass
        self._pw = None
        self._browser = None
        self._context = None
        logger.info("PlaywrightFetcher stopped")

    def _get_context_for_request(self):
        """Return a context to use for a single request.

        When fresh_context_per_request=True, callers MUST close the returned
        context after they're done with the page. When False, a shared context
        is returned and callers must NOT close it.
        """
        if self._fresh_context:
            return self._new_context(), True
        return self._context, False

    def fetch_page(self, url: str, timeout: int | None = None) -> dict[str, Any]:
        """Fetch a single URL. Never raises — errors go into the result dict."""
        if self._browser is None:
            self.start()

        timeout_ms = timeout if timeout is not None else self._default_timeout
        result: dict[str, Any] = {
            "url": url,
            "final_url": None,
            "status": None,
            "title": "",
            "html": "",
            "text": "",
            "emails": [],
            "mailto_links": [],
            "blocked": False,
            "block_reason": None,
            "error": None,
        }

        page = None
        ctx = None
        ctx_owned = False
        try:
            ctx, ctx_owned = self._get_context_for_request()
            page = ctx.new_page()
            page.set_default_timeout(timeout_ms)
            resp = page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
            # Give JS a second to settle; most directory pages do a tiny ajax
            # hydration after domcontentloaded.
            try:
                page.wait_for_load_state("networkidle", timeout=5_000)
            except PlaywrightTimeoutError:
                pass
            page.wait_for_timeout(500)

            result["final_url"] = page.url
            result["status"] = resp.status if resp else None
            result["title"] = (page.title() or "").strip()
            result["html"] = page.content() or ""
            try:
                result["text"] = page.inner_text("body") or ""
            except Exception:
                result["text"] = ""

            # DOM-side mailto extraction
            try:
                mailtos = page.eval_on_selector_all(
                    'a[href^="mailto:"]',
                    "els => els.map(e => e.getAttribute('href'))",
                ) or []
            except Exception:
                mailtos = []
            clean_mailtos: list[str] = []
            for m in mailtos:
                if not m:
                    continue
                addr = m.split(":", 1)[1].split("?", 1)[0].strip()
                if addr and not _is_junk_email(addr):
                    clean_mailtos.append(addr)
            result["mailto_links"] = clean_mailtos

            # Sanity gate
            blocked, reason = _sanity_gate(result["title"], result["text"])
            result["blocked"] = blocked
            result["block_reason"] = reason

            # Regex emails from rendered HTML + text (union with mailtos)
            all_emails = set(extract_emails_from_html(result["html"]))
            all_emails |= set(extract_emails_from_html(result["text"]))
            all_emails |= set(clean_mailtos)
            result["emails"] = sorted(all_emails, key=lambda s: s.lower())

        except PlaywrightTimeoutError as exc:
            result["error"] = f"timeout: {exc}"
            logger.warning("fetch_page TIMEOUT %s: %s", url, exc)
        except PlaywrightError as exc:
            result["error"] = f"playwright: {exc}"
            logger.warning("fetch_page PLAYWRIGHT_ERR %s: %s", url, str(exc)[:200])
        except Exception as exc:
            result["error"] = f"{type(exc).__name__}: {exc}"
            logger.warning("fetch_page UNEXPECTED %s: %s", url, exc)
        finally:
            if page is not None:
                try:
                    page.close()
                except Exception:
                    pass
            if ctx_owned and ctx is not None:
                try:
                    ctx.close()
                except Exception:
                    pass

        return result
