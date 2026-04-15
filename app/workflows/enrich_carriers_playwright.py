"""
BrokerOps AI – Playwright-driven carrier email enrichment workflow.

Reads the Carrier Database, filters to for_hire rows with a blank
Contact Email (col G), visits each carrier's DOT-indexed directory pages via
`PlaywrightFetcher`, extracts the best candidate email, and writes results
back to the sheet in a single batched update at the end.

Design notes:
  - SYNC Playwright — simpler lifecycle; our per-hostname rate limit (6/min)
    is the real ceiling, so async concurrency gains are small for 26 rows.
  - Single browser / single context per run. Pages are opened and closed per
    request.
  - Per-hostname rate limit enforced via a timestamp map.
  - Per-carrier budget = 5 pages max (4 directories + 1 website).
  - Checkpointing: after every carrier we update an on-disk JSON file so a
    mid-run crash can resume without hammering the same sites twice.

Idempotency:
  - Skip rows where col G already has an email.
  - Skip rows whose Notes (col AE) already contain the marker
    `[PLAYWRIGHT ENRICH 2026-04-14]` (unless caller forces rerun).
  - Never overwrite col G or col J if they have content.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from app.config import get_settings
from app.enrichment.directory_targets import (
    DIRECTORY_URL_PATTERNS,
    build_urls,
    hostname_for,
)
from app.enrichment.playwright_fetcher import (
    PlaywrightFetcher,
    _is_junk_email,
    extract_emails_from_html,
)
from app.fmcsa import _BASE_URL, _cached_get
from app.google_auth import get_sheets_service
from app.sheets import read_range

logger = logging.getLogger("brokerops.enrich_carriers_playwright")

CARRIER_DB_TAB = "'Carrier Database'"
CARRIER_DB_RANGE_AF = f"{CARRIER_DB_TAB}!A:AG"
MARKER = "[PLAYWRIGHT ENRICH 2026-04-14]"
ENRICH_DATE = "2026-04-14"

# Column letters (1-indexed for readability — we convert at use-site)
COL_G_CONTACT_EMAIL = 6   # 0-indexed in row lists
COL_J_DISPATCH_EMAIL = 9
COL_N_STATE = 13
COL_AE_NOTES = 30
COL_AF_CLASS = 31
COL_AG_VETTING = 32

# Public role prefixes, ranked best-to-worst
_ROLE_RANKS = [
    "dispatch", "dispatcher", "dispatching",
    "ops", "operations",
    "logistics",
    "booking", "bookings",
    "broker", "brokerage",
    "office",
    "info", "contact",
    "sales",
    "hr", "hr2", "recruiter", "recruiting",
    "admin",
    "safety",
    "billing", "accounts", "accounting", "ar", "ap",
    "support", "help",
]

# Shared / generic public domains where role@ emails are almost always junk
_SHARED_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
    "live.com", "icloud.com", "msn.com", "comcast.net", "att.net",
    "verizon.net", "sbcglobal.net", "bellsouth.net",
}


@dataclass
class CarrierRecord:
    row_idx: int            # 1-indexed sheet row number
    dot: str
    mc: str
    name: str
    state: str
    city: str
    current_email: str
    current_dispatch: str
    notes: str
    classification: str
    website_hint: Optional[str] = None  # extracted from Notes


@dataclass
class EnrichmentResult:
    carrier: CarrierRecord
    pages_fetched: int = 0
    pages_blocked: int = 0
    pages_errored: int = 0
    emails_found: list[str] = field(default_factory=list)
    emails_rejected: list[str] = field(default_factory=list)
    picked_email: Optional[str] = None
    picked_source: Optional[str] = None  # site id
    picked_url: Optional[str] = None
    picked_is_name_match: bool = False
    picked_is_shared_domain: bool = False
    per_page_log: list[dict[str, Any]] = field(default_factory=list)
    outcome: str = "no_email_found"  # written | skipped_existing | error | no_email_found


# ─── Helpers ────────────────────────────────────────────────────────────────

_WEBSITE_RE = re.compile(r"https?://[^\s,;|<>\"')]+", re.IGNORECASE)


def _extract_website_from_notes(notes: str) -> Optional[str]:
    """Pull the first plausible carrier website out of the Notes field."""
    if not notes:
        return None
    for m in _WEBSITE_RE.finditer(notes):
        url = m.group(0)
        low = url.lower()
        if any(
            bad in low
            for bad in (
                "fmcsa", "dot.gov", "brokersnapshot", "quicktransportsolutions",
                "carriernetwork", "dot.report", "partnercarrier", "google.com",
                "safer",
            )
        ):
            continue
        return url.rstrip(".,;)")
    return None


def _role_rank(local: str) -> int:
    """Lower is better. 999 = not a role address."""
    low = local.lower()
    for i, role in enumerate(_ROLE_RANKS):
        if low == role or low.startswith(role):
            return i
    return 999


_STOPWORD_TOKENS = {
    "llc", "inc", "corp", "co", "ltd", "limited", "the", "and", "of",
    "trucking", "transport", "transportation", "freight", "logistics",
    "carriers", "carrier", "hauling", "haul", "express", "lines",
    "services", "service", "group", "enterprises", "enterprise",
    "heavy", "flatbed", "reefer", "vans", "van", "trailers", "dispatch",
    "company", "companies", "solutions", "shop",
}


def _tokenize_name(name: str) -> set[str]:
    """Normalize a carrier name into distinctive tokens (drop common suffixes)."""
    if not name:
        return set()
    low = re.sub(r"[^a-z0-9]+", " ", name.lower())
    toks = {t for t in low.split() if len(t) >= 3 and t not in _STOPWORD_TOKENS}
    return toks


def _name_overlap(email: str, name_tokens: set[str]) -> bool:
    """Does the email's local part or domain contain any distinctive name token?"""
    if not name_tokens:
        return False
    local, _, domain = email.lower().partition("@")
    hay = local + " " + domain
    return any(tok in hay for tok in name_tokens)


def _pick_best_email(
    emails: list[str],
    website_hint: Optional[str],
    carrier_name: str = "",
) -> Optional[str]:
    """Pick the best email from candidates given the ranking rules.

    Ranking (lower is better):
      1. domain == known website domain                      → 0 else 1
      2. email has overlap with carrier name tokens          → 0 else 1
      3. shared-domain (gmail/yahoo) penalty                  → 0 else 1
      4. role-rank (dispatch > ops > info > ...)             → 0..999
      5. alphabetical tiebreaker
    """
    if not emails:
        return None

    # Deduplicate case-insensitively, keep first-seen casing
    seen: dict[str, str] = {}
    for e in emails:
        key = e.lower()
        if key not in seen:
            seen[key] = e
    cands = list(seen.values())

    hint_domain: Optional[str] = None
    if website_hint:
        d = website_hint.lower()
        for p in ("https://", "http://", "www."):
            if d.startswith(p):
                d = d[len(p):]
        hint_domain = d.split("/", 1)[0].strip() or None

    name_tokens = _tokenize_name(carrier_name)

    def score(email: str) -> tuple[int, int, int, int, str]:
        local, _, domain = email.partition("@")
        domain = domain.lower()
        local_low = local.lower()
        domain_match = 0 if (hint_domain and domain == hint_domain) else 1
        name_match = 0 if _name_overlap(email, name_tokens) else 1
        shared = 1 if domain in _SHARED_DOMAINS else 0
        rr = _role_rank(local_low)
        return (domain_match, name_match, shared, rr, email.lower())

    cands.sort(key=score)
    # If the best candidate has both name_match=1 AND shared=1 AND no domain
    # match, and there's an alternative with name_match=0, we still pick the
    # alternative — the sort key already handles this.
    return cands[0]


def _notes_append(existing: str, fragment: str) -> str:
    """Append a fragment to the Notes column without destroying existing text."""
    if not existing:
        return fragment
    sep = " | " if not existing.rstrip().endswith("|") else " "
    return f"{existing.rstrip()}{sep}{fragment}"


def _col_letter_af(idx0: int) -> str:
    """0-indexed column → sheet letter. Supports A..AF only."""
    if idx0 < 26:
        return chr(ord("A") + idx0)
    return "A" + chr(ord("A") + (idx0 - 26))


# ─── Rate limiter ───────────────────────────────────────────────────────────


class HostnameRateLimiter:
    """Min-interval-per-hostname throttle. 6 req/min → 10s interval."""

    def __init__(self, min_interval_s: float = 10.0):
        self.min_interval = min_interval_s
        self._last: dict[str, float] = {}

    def wait(self, hostname: str) -> float:
        """Block until the next request to `hostname` is allowed. Returns waited seconds."""
        now = time.monotonic()
        last = self._last.get(hostname)
        if last is None:
            self._last[hostname] = now
            return 0.0
        delta = now - last
        if delta < self.min_interval:
            to_wait = self.min_interval - delta
            time.sleep(to_wait)
            self._last[hostname] = time.monotonic()
            return to_wait
        self._last[hostname] = now
        return 0.0


# ─── Checkpointing ──────────────────────────────────────────────────────────


def _load_checkpoint(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {"processed_dots": {}, "started_at": None}
    try:
        with open(path, "r", encoding="utf-8") as fp:
            return json.load(fp)
    except Exception as exc:
        logger.warning("Could not load checkpoint %s: %s", path, exc)
        return {"processed_dots": {}, "started_at": None}


def _save_checkpoint(path: str, state: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fp:
        json.dump(state, fp, indent=2, default=str)
    os.replace(tmp, path)


# ─── Sheet loading ──────────────────────────────────────────────────────────


def load_eligible_carriers(
    dot_filter: Optional[set[str]] = None,
) -> list[CarrierRecord]:
    """Read the sheet and return for_hire rows with blank col G."""
    sid = get_settings().CARRIER_MASTER_SHEET_ID
    rows = read_range(sid, CARRIER_DB_RANGE_AF)
    if not rows:
        return []
    headers = rows[0]
    width = len(headers)
    out: list[CarrierRecord] = []
    for i, r in enumerate(rows[1:], start=2):  # start=2 because sheet row 1 is header
        r = r + [""] * (width - len(r))
        cls = (r[COL_AF_CLASS] or "").strip()
        email = (r[COL_G_CONTACT_EMAIL] or "").strip()
        if email:
            continue
        # Vetting gate: only enrich rows that passed the sheet-level vetting
        # sweep (col AG = 'pass_basic'). Carriers that failed the 3 hard
        # rules (fleet>=3, liability>=$1M, cargo>=$100K) must not be enriched.
        # 2026-04-14 PM: Classification is blank on the fresh L&I batch (the
        # db_cleanup script hasn't re-run since they landed). pass_basic is
        # the canonical gate — for_hire is a useful taxonomy but not required
        # for enrichment. Accept pass_basic OR explicit for_hire classification.
        vetting = (r[COL_AG_VETTING] if len(r) > COL_AG_VETTING else "").strip().lower()
        if vetting != "pass_basic" and cls != "for_hire":
            continue
        dot = (r[4] or "").strip()
        if dot_filter is not None and dot not in dot_filter:
            continue
        notes = r[COL_AE_NOTES] or ""
        rec = CarrierRecord(
            row_idx=i,
            dot=dot,
            mc=(r[3] or "").strip(),
            name=(r[2] or "").strip(),
            state=(r[COL_N_STATE] or "").strip(),
            city=(r[12] or "").strip(),
            current_email=email,
            current_dispatch=(r[COL_J_DISPATCH_EMAIL] or "").strip(),
            notes=notes,
            classification=cls,
            website_hint=_extract_website_from_notes(notes),
        )
        out.append(rec)
    return out


# ─── The workflow ───────────────────────────────────────────────────────────


def enrich_one(
    carrier: CarrierRecord,
    fetcher: PlaywrightFetcher,
    limiter: HostnameRateLimiter,
    *,
    per_carrier_budget: int = 5,
) -> EnrichmentResult:
    """Run all directory lookups for one carrier. Returns the populated result."""
    res = EnrichmentResult(carrier=carrier)

    # Idempotency: skip if already processed today
    if MARKER in (carrier.notes or ""):
        res.outcome = "skipped_existing_marker"
        return res

    if carrier.current_email:
        res.outcome = "skipped_existing_email"
        return res

    # Build URL list: directories first, then optionally the carrier's own website
    url_queue: list[tuple[str, str]] = build_urls(carrier.dot, carrier.state)
    if carrier.website_hint:
        url_queue.append(("website", carrier.website_hint))

    # Respect per-carrier budget
    url_queue = url_queue[:per_carrier_budget]

    all_candidates: list[str] = []
    candidate_source: dict[str, tuple[str, str]] = {}  # email -> (site_id, url)

    for site_id, url in url_queue:
        host = hostname_for(site_id) if site_id != "website" else _hostname_of(url)
        waited = limiter.wait(host)
        page_log: dict[str, Any] = {
            "site_id": site_id,
            "url": url,
            "host": host,
            "waited_s": round(waited, 2),
        }
        result = fetcher.fetch_page(url, timeout=60_000)
        page_log["status"] = result["status"]
        page_log["final_url"] = result["final_url"]
        page_log["blocked"] = result["blocked"]
        page_log["block_reason"] = result["block_reason"]
        page_log["error"] = result["error"]

        if result["error"]:
            res.pages_errored += 1
            page_log["outcome"] = "error"
            res.per_page_log.append(page_log)
            continue

        res.pages_fetched += 1

        if result["blocked"]:
            res.pages_blocked += 1
            page_log["outcome"] = "blocked"
            res.per_page_log.append(page_log)
            continue

        # Collect emails — union regex + mailto
        raw_emails = result["emails"] or []
        kept: list[str] = []
        for e in raw_emails:
            if _is_junk_email(e):
                res.emails_rejected.append(e)
                continue
            kept.append(e)
            if e not in candidate_source:
                candidate_source[e] = (site_id, url)
        page_log["emails_raw"] = raw_emails
        page_log["emails_kept"] = kept
        page_log["outcome"] = "ok"
        res.per_page_log.append(page_log)
        all_candidates.extend(kept)

        # If we already have a domain-matched or dispatch@ address we could
        # break early — but keeping going through budgeted pages gives us
        # better confirmation. We have a small budget anyway.

    # Pick best
    res.emails_found = sorted(set(all_candidates), key=lambda s: s.lower())
    best = _pick_best_email(
        res.emails_found, carrier.website_hint, carrier.name
    )
    if best:
        src = candidate_source.get(best)
        name_tokens = _tokenize_name(carrier.name)
        best_local = best.split("@", 1)[0].lower()
        best_domain = best.split("@", 1)[1].lower()
        is_name = _name_overlap(best, name_tokens)
        is_role = _role_rank(best_local) < 999
        is_shared = best_domain in _SHARED_DOMAINS
        is_domain_match = False
        if carrier.website_hint:
            d = carrier.website_hint.lower()
            for p in ("https://", "http://", "www."):
                if d.startswith(p):
                    d = d[len(p):]
            hint_dom = d.split("/", 1)[0].strip()
            is_domain_match = best_domain == hint_dom

        # Low-confidence guard: cross-contamination is only a real problem on
        # dot.report (which mixes unrelated cars/vehicles/military links on
        # the same DOT page). brokersnapshot profiles are structured per-carrier
        # so any email surfaced there IS the carrier's own email — trust those
        # even without a name-token match.
        source_is_trusted = (src is not None and src[0] == "brokersnapshot")
        if (not source_is_trusted) and not (is_name or is_role or is_domain_match):
            res.picked_email = None
            res.picked_source = src[0] if src else None
            res.picked_url = src[1] if src else None
            res.picked_is_name_match = False
            res.picked_is_shared_domain = is_shared
            res.outcome = f"low_confidence:{best}"
        else:
            res.picked_email = best
            if src:
                res.picked_source, res.picked_url = src
            res.picked_is_name_match = is_name
            res.picked_is_shared_domain = is_shared
            res.outcome = "email_picked"
    else:
        res.outcome = "no_email_found"

    return res


def _hostname_of(url: str) -> str:
    low = url.lower()
    for p in ("https://", "http://"):
        if low.startswith(p):
            low = low[len(p):]
    return low.split("/", 1)[0]


def run_enrichment(
    *,
    limit: Optional[int] = None,
    dots: Optional[set[str]] = None,
    dry_run: bool = False,
    checkpoint_path: Optional[str] = None,
    per_host_interval_s: float = 10.0,
) -> dict[str, Any]:
    """Main entry point. Returns a summary dict."""
    started = datetime.now(timezone.utc)
    checkpoint_path = checkpoint_path or (
        "scripts/.checkpoints/enrich_playwright_20260414.json"
    )
    state = _load_checkpoint(checkpoint_path)
    if state.get("started_at") is None:
        state["started_at"] = started.isoformat()

    carriers = load_eligible_carriers(dot_filter=dots)
    if limit is not None:
        carriers = carriers[:limit]
    logger.info(
        "Loaded %d eligible for_hire carriers (dots=%s, limit=%s)",
        len(carriers), dots, limit,
    )

    results: list[EnrichmentResult] = []
    pending_updates: list[tuple[int, str, str]] = []  # (row_idx, email, new_notes)

    limiter = HostnameRateLimiter(min_interval_s=per_host_interval_s)

    with PlaywrightFetcher() as fetcher:
        for i, carrier in enumerate(carriers, start=1):
            logger.info(
                "[%d/%d] DOT=%s name=%s state=%s",
                i, len(carriers), carrier.dot, carrier.name[:40], carrier.state,
            )
            # Skip if checkpoint says we handled it already in this run id
            if carrier.dot in state.get("processed_dots", {}):
                logger.info("  skip (already in checkpoint)")
                continue

            try:
                r = enrich_one(carrier, fetcher, limiter)
            except Exception as exc:
                logger.exception("enrich_one crashed for DOT %s", carrier.dot)
                r = EnrichmentResult(carrier=carrier, outcome=f"crash:{type(exc).__name__}")

            results.append(r)
            logger.info(
                "  outcome=%s pages=%d blocked=%d errored=%d candidates=%d picked=%s src=%s",
                r.outcome, r.pages_fetched, r.pages_blocked, r.pages_errored,
                len(r.emails_found), r.picked_email, r.picked_source,
            )

            # Queue sheet update if we got an email
            if r.picked_email and not carrier.current_email:
                note_frag = (
                    f"{MARKER} email={r.picked_email} "
                    f"source={r.picked_source or '?'} "
                    f"url={r.picked_url or '?'}"
                )
                new_notes = _notes_append(carrier.notes, note_frag)
                pending_updates.append((carrier.row_idx, r.picked_email, new_notes))

            # Checkpoint
            state.setdefault("processed_dots", {})[carrier.dot] = {
                "outcome": r.outcome,
                "picked_email": r.picked_email,
                "picked_source": r.picked_source,
                "pages_fetched": r.pages_fetched,
                "pages_blocked": r.pages_blocked,
                "pages_errored": r.pages_errored,
                "emails_found": r.emails_found,
            }
            _save_checkpoint(checkpoint_path, state)

    # Batched sheet write
    written = 0
    if not dry_run and pending_updates:
        written = _batch_write_updates(pending_updates)

    ended = datetime.now(timezone.utc)
    summary = {
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "elapsed_s": (ended - started).total_seconds(),
        "eligible_loaded": len(carriers),
        "results_count": len(results),
        "emails_found_any": sum(1 for r in results if r.emails_found),
        "emails_picked": sum(1 for r in results if r.picked_email),
        "writes_queued": len(pending_updates),
        "writes_committed": written,
        "dry_run": dry_run,
        "per_carrier": [
            {
                "dot": r.carrier.dot,
                "name": r.carrier.name,
                "state": r.carrier.state,
                "outcome": r.outcome,
                "pages_fetched": r.pages_fetched,
                "pages_blocked": r.pages_blocked,
                "pages_errored": r.pages_errored,
                "emails_found": r.emails_found,
                "emails_rejected": r.emails_rejected,
                "picked_email": r.picked_email,
                "picked_source": r.picked_source,
                "picked_url": r.picked_url,
                "picked_is_name_match": r.picked_is_name_match,
                "picked_is_shared_domain": r.picked_is_shared_domain,
            }
            for r in results
        ],
    }
    return summary


# ─── State Backfill ─────────────────────────────────────────────────────────
# Why this exists: the original Playwright enrichment loop wrote Contact Email
# and Notes but never backfilled City/State/ZIP for carriers that already had
# those fields blank at load time. 42 pre-L&I carriers landed with blank State
# after the 2026-04-14 enrichment pass. This function is the standalone fix.
# See memory/feedback_carrier_category_rules.md for carrier classification rules
# that govern which rows are safe to write.


def _fetch_hq_raw(dot: str) -> Optional[dict]:
    """Fetch raw /carriers/{dot} dict from FMCSA QCMobile. Returns carrier sub-dict or None."""
    try:
        data = _cached_get(f"{_BASE_URL}/{dot}")
    except Exception as exc:
        logger.warning("DOT %s: FMCSA fetch error: %s", dot, exc)
        return None
    content = data.get("content", data)
    if isinstance(content, list):
        content = content[0] if content else None
    if not isinstance(content, dict):
        return None
    carrier = content.get("carrier", content)
    if not isinstance(carrier, dict) or not carrier:
        return None
    return carrier


def _load_quarantined_dots() -> set[str]:
    """Return the set of DOT numbers currently in the Carrier Quarantine tab."""
    from app.vetting.quarantine import QUARANTINE_TAB
    sid = get_settings().CARRIER_MASTER_SHEET_ID
    svc = get_sheets_service()
    try:
        resp = svc.spreadsheets().values().get(
            spreadsheetId=sid,
            range=f"'{QUARANTINE_TAB}'!A:E",
        ).execute()
    except Exception as exc:
        logger.warning("Could not read Quarantine tab: %s — assuming empty", exc)
        return set()
    rows = resp.get("values", [])
    if not rows:
        return set()
    header = rows[0]
    try:
        dot_idx = header.index("DOT Number")
    except ValueError:
        logger.warning("Quarantine tab has no 'DOT Number' header column")
        return set()
    quarantined: set[str] = set()
    for r in rows[1:]:
        if len(r) > dot_idx:
            val = str(r[dot_idx]).strip()
            if val:
                quarantined.add(val)
    logger.info("Quarantine tab: %d DOTs loaded (write-gate)", len(quarantined))
    return quarantined


def _load_blank_state_carriers() -> list[CarrierRecord]:
    """Read main tab and return all rows (any classification) with blank State."""
    sid = get_settings().CARRIER_MASTER_SHEET_ID
    rows = read_range(sid, CARRIER_DB_RANGE_AF)
    if not rows:
        return []
    headers = rows[0]
    width = len(headers)
    out: list[CarrierRecord] = []
    for i, r in enumerate(rows[1:], start=2):
        r = r + [""] * (width - len(r))
        state = (r[COL_N_STATE] or "").strip()
        if state:
            continue  # already has a state — skip
        dot = (r[4] or "").strip()
        if not dot:
            continue
        out.append(CarrierRecord(
            row_idx=i,
            dot=dot,
            mc=(r[3] or "").strip(),
            name=(r[2] or "").strip(),
            state=state,
            city=(r[12] or "").strip(),
            current_email=(r[COL_G_CONTACT_EMAIL] or "").strip(),
            current_dispatch=(r[COL_J_DISPATCH_EMAIL] or "").strip(),
            notes=(r[COL_AE_NOTES] or "").strip(),
            classification=(r[COL_AF_CLASS] or "").strip(),
        ))
    return out


def backfill_blank_states(
    *,
    dry_run: bool = False,
    log_path: Optional[str] = None,
) -> dict[str, Any]:
    """
    One-shot backfill of City/State/ZIP for main-tab carriers with blank State.

    Algorithm:
      1. Load all main-tab rows with blank State (any classification).
      2. Load all quarantined DOTs — skip those to avoid resurrecting quarantined rows.
      3. For each remaining DOT, call FMCSA /carriers/{dot} (1 req/sec).
      4. Extract phyCity / phyState / phyZipcode.
      5. Batch-write City, State, ZIP back in one batchUpdate call.

    Returns a summary dict with per-DOT results.
    """
    started = datetime.now(timezone.utc)

    # Set up file logging if requested
    file_handler: Optional[logging.FileHandler] = None
    if log_path:
        os.makedirs(os.path.dirname(log_path) if os.path.dirname(log_path) else ".", exist_ok=True)
        file_handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s | %(message)s"))
        logging.getLogger("brokerops").addHandler(file_handler)
        logger.info("State backfill log: %s", log_path)

    try:
        return _backfill_blank_states_inner(started, dry_run=dry_run)
    finally:
        if file_handler:
            logging.getLogger("brokerops").removeHandler(file_handler)
            file_handler.close()


def _backfill_blank_states_inner(started: datetime, *, dry_run: bool) -> dict[str, Any]:
    logger.info("=== STATE BACKFILL START (dry_run=%s) ===", dry_run)

    blank_carriers = _load_blank_state_carriers()
    logger.info("Main tab: %d rows with blank State", len(blank_carriers))

    quarantined_dots = _load_quarantined_dots()

    # Separate into actionable vs skipped-quarantined
    to_process: list[CarrierRecord] = []
    skipped_quarantined: list[str] = []
    for c in blank_carriers:
        if c.dot in quarantined_dots:
            logger.info("DOT %s (%s): SKIP — in Quarantine tab", c.dot, c.name[:40])
            skipped_quarantined.append(c.dot)
        else:
            to_process.append(c)

    logger.info(
        "%d to process, %d skipped (quarantined)",
        len(to_process), len(skipped_quarantined),
    )

    # FMCSA calls — 1 req/sec
    per_dot: list[dict[str, Any]] = []
    # (row_idx, city, state, zip) tuples for the batch write
    pending_writes: list[tuple[int, str, str, str]] = []

    for idx, c in enumerate(to_process, start=1):
        logger.info(
            "[%d/%d] DOT %s  %s",
            idx, len(to_process), c.dot, c.name[:50],
        )
        carrier_raw = _fetch_hq_raw(c.dot)

        if carrier_raw is None:
            logger.info("  → still blank (FMCSA returned no data)")
            per_dot.append({
                "dot": c.dot, "name": c.name,
                "result": "still_blank", "reason": "fmcsa_no_data",
                "city": "", "state": "", "zip": "",
            })
        else:
            fm_city = (carrier_raw.get("phyCity") or "").strip()
            fm_state = (carrier_raw.get("phyState") or "").strip()
            fm_zip = str(carrier_raw.get("phyZipcode") or "").strip()

            if fm_state:
                logger.info(
                    "  → filled: city=%s state=%s zip=%s",
                    fm_city, fm_state, fm_zip,
                )
                per_dot.append({
                    "dot": c.dot, "name": c.name,
                    "result": "filled",
                    "city": fm_city, "state": fm_state, "zip": fm_zip,
                })
                pending_writes.append((c.row_idx, fm_city, fm_state, fm_zip))
            else:
                logger.info("  → still blank (FMCSA phyState is empty)")
                per_dot.append({
                    "dot": c.dot, "name": c.name,
                    "result": "still_blank", "reason": "fmcsa_phy_state_empty",
                    "city": fm_city, "state": "", "zip": fm_zip,
                })

        # Rate limit: 1 req/sec
        time.sleep(1.0)

    # Batch write
    writes_committed = 0
    if not dry_run and pending_writes:
        writes_committed = _batch_write_geo(pending_writes)
    elif dry_run:
        logger.info("DRY RUN — %d writes suppressed", len(pending_writes))

    ended = datetime.now(timezone.utc)
    n_filled = sum(1 for d in per_dot if d["result"] == "filled")
    n_still_blank = sum(1 for d in per_dot if d["result"] == "still_blank")

    logger.info(
        "=== STATE BACKFILL DONE: attempted=%d filled=%d still_blank=%d skipped_quarantined=%d "
        "writes_committed=%d elapsed=%.1fs ===",
        len(to_process), n_filled, n_still_blank,
        len(skipped_quarantined), writes_committed,
        (ended - started).total_seconds(),
    )

    return {
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "elapsed_s": (ended - started).total_seconds(),
        "blank_state_rows_found": len(blank_carriers),
        "dots_attempted": len(to_process),
        "dots_filled": n_filled,
        "dots_still_blank": n_still_blank,
        "dots_skipped_quarantined": len(skipped_quarantined),
        "writes_committed": writes_committed,
        "dry_run": dry_run,
        "per_dot": per_dot,
        "skipped_quarantined_dots": skipped_quarantined,
    }


def _batch_write_geo(writes: list[tuple[int, str, str, str]]) -> int:
    """Write City (col M), State (col N), ZIP (col O) in one batchUpdate.

    Only writes cells where the FMCSA value is non-empty.
    Never touches any other column.
    """
    if not writes:
        return 0
    sid = get_settings().CARRIER_MASTER_SHEET_ID
    svc = get_sheets_service().spreadsheets()
    data = []
    for row_idx, city, state, zip_code in writes:
        if city:
            data.append({"range": f"{CARRIER_DB_TAB}!M{row_idx}", "values": [[city]]})
        if state:
            data.append({"range": f"{CARRIER_DB_TAB}!N{row_idx}", "values": [[state]]})
        if zip_code:
            data.append({"range": f"{CARRIER_DB_TAB}!O{row_idx}", "values": [[zip_code]]})
    if not data:
        return 0
    body = {"valueInputOption": "USER_ENTERED", "data": data}
    svc.values().batchUpdate(spreadsheetId=sid, body=body).execute()
    logger.info(
        "Geo batch write: %d carrier rows → %d cells updated",
        len(writes), len(data),
    )
    return len(writes)


def _batch_write_updates(updates: list[tuple[int, str, str]]) -> int:
    """Write all queued (row_idx, email, new_notes) in one batchUpdate call.

    Writes col G (email) and col AE (notes) only. Never touches B/AC/AF.
    Only writes col G if that cell is currently blank (re-verified at write time
    would require a second read — we already filtered to blank on load, and we
    run a single pass, so we trust the in-memory snapshot).
    """
    if not updates:
        return 0
    sid = get_settings().CARRIER_MASTER_SHEET_ID
    svc = get_sheets_service().spreadsheets()
    data = []
    for row_idx, email, new_notes in updates:
        data.append({
            "range": f"{CARRIER_DB_TAB}!G{row_idx}",
            "values": [[email]],
        })
        data.append({
            "range": f"{CARRIER_DB_TAB}!AE{row_idx}",
            "values": [[new_notes]],
        })
    body = {"valueInputOption": "USER_ENTERED", "data": data}
    svc.values().batchUpdate(spreadsheetId=sid, body=body).execute()
    logger.info("Batch-wrote %d carrier updates (%d cells)", len(updates), len(data))
    return len(updates)
