"""
BrokerOps AI -- Carrier reply classifier.

Uses Claude (via app/ai_parser.py Gemini-compatible interface, or direct
Anthropic API if available) to classify inbound carrier replies into one of
six categories and route each to the appropriate action.

Categories:
  interested        -- positive engagement, wants to proceed
  not_interested    -- explicit decline, remove/unsubscribe request
  need_more_info    -- question asked, wants clarification
  ooo               -- out-of-office auto-reply with optional return date
  bounce            -- DSN / mailer-daemon / SMTP failure notice
  redirect          -- "contact X instead" / "I no longer work here"

Entry points:
  classify_reply(subject, body, sender) -> ClassifiedReply
  route_classified_reply(classified, carrier_dot) -> None
"""
from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Optional

from app.config import get_settings
from app.notifications import notify_slack

logger = logging.getLogger("brokerops.reply_classifier")

# ── Reply categories ──────────────────────────────────────────────────────────

CATEGORY_INTERESTED = "interested"
CATEGORY_NOT_INTERESTED = "not_interested"
CATEGORY_NEED_MORE_INFO = "need_more_info"
CATEGORY_OOO = "ooo"
CATEGORY_OOO_REDIRECT = "ooo_redirect"
CATEGORY_BOUNCE = "bounce"
CATEGORY_REDIRECT = "redirect"

ALL_CATEGORIES = [
    CATEGORY_INTERESTED,
    CATEGORY_NOT_INTERESTED,
    CATEGORY_NEED_MORE_INFO,
    CATEGORY_OOO,
    CATEGORY_OOO_REDIRECT,
    CATEGORY_BOUNCE,
    CATEGORY_REDIRECT,
]

# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class ClassifiedReply:
    category: str               # one of ALL_CATEGORIES
    confidence: str             # high / medium / low
    extracted_data: dict = field(default_factory=dict)
    # keys used by router:
    #   return_date   (str, ISO)  -- for ooo
    #   new_email     (str)       -- for redirect
    #   bounce_code   (str)       -- for bounce (e.g. "550 5.1.1")
    #   bounce_type   (str)       -- "hard" or "soft"
    #   question_text (str)       -- for need_more_info
    action: str = ""            # short description of next step
    raw_subject: str = ""
    raw_sender: str = ""

# ── Regex pre-classifiers (fast path before Claude) ──────────────────────────

_OOO_RE = re.compile(
    r"\b(out of office|automatic reply|auto[- ]?reply|on vacation|"
    r"on leave|away from (my )?(desk|office)|will be (back|returning)|"
    r"currently (away|out|unavailable))\b",
    re.I,
)
_UNSUB_RE = re.compile(
    r"\b(unsubscribe|remove (me|my email)|stop (emailing|contacting)|"
    r"do not (email|contact)|opt[ -]?out|not interested|no thank[s]?|"
    r"please remove|take me off)\b"
    r"|^\s*stop\s*$",  # bare "STOP" reply per CAN-SPAM footer instruction
    re.I | re.MULTILINE,
)
_BOUNCE_SENDER_RE = re.compile(
    r"(mailer-daemon|postmaster|delivery.*failure|mail.*delivery.*subsystem|"
    r"auto.*submit|noreply@.*google\.com|bounce|dsn@)",
    re.I,
)
_BOUNCE_SUBJECT_RE = re.compile(
    r"(delivery status notification|undeliverable|mail delivery failed|"
    r"returned mail|delivery failure|bounced message|message not delivered|"
    r"failure notice)",
    re.I,
)
_BOUNCE_CODE_RE = re.compile(r"\b(5\d\d|4\d\d)\s+\d+\.\d+\.\d+\b")
_SOFT_BOUNCE_RE = re.compile(
    r"\b(mailbox (full|over quota)|temporarily unavailable|"
    r"try again|quota exceeded|4\d\d )\b",
    re.I,
)
_REDIRECT_RE = re.compile(
    r"\b(please (contact|email|reach out to)|"
    r"(i |i'm |i am )?(no longer|not) (at|with|working for)|"
    r"forward(ed)? to|you should (contact|email|reach)|"
    r"(try|use) (this|my) (email|address))\b",
    re.I,
)
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
_RETURN_DATE_RE = re.compile(
    r"(return(ing)?|back|available)\s+(?:on\s+)?(\w+ \d+|\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)",
    re.I,
)

# ── Claude prompt ─────────────────────────────────────────────────────────────

_CLASSIFY_PROMPT = """You are a freight brokerage assistant for deBoer Freight.

A carrier has replied to an outreach email. Classify the reply and extract relevant data.

Reply details:
From: {sender}
Subject: {subject}
Body:
---
{body}
---

Return ONLY a JSON object with these fields:
- "category": one of "interested" | "not_interested" | "need_more_info" | "ooo" | "bounce" | "redirect"
- "confidence": "high" | "medium" | "low"
- "action": short string describing what to do next (e.g. "send docs request", "mark excluded", "schedule E2 after return date")
- "extracted_data": object with any of these keys that apply:
    - "return_date": ISO date string if OOO (e.g. "2026-04-20")
    - "new_email": email address if redirect
    - "bounce_code": SMTP code if bounce (e.g. "550 5.1.1")
    - "bounce_type": "hard" or "soft" if bounce
    - "question_text": the question asked if need_more_info

Category definitions:
- interested: any positive signal, wants to work together, asks about rates/lanes in an engaged way
- not_interested: explicit decline, remove request, unsubscribe
- need_more_info: asks a genuine question about your lanes, rates, equipment needs, or process
- ooo: automated out-of-office reply; look for a return date
- bounce: DSN / mailer-daemon / delivery failure notification
- redirect: "contact X instead" or "I no longer handle this" with a new contact

No markdown. No backticks. JSON only.
"""

# ── Classification ────────────────────────────────────────────────────────────

def _fast_classify(subject: str, body: str, sender: str) -> Optional[str]:
    """Fast regex pre-classifier. Returns category or None for Claude fallback."""
    text = f"{subject}\n{body}"
    sender_l = sender.lower()

    # Bounce -- highest priority (sender or subject pattern)
    if _BOUNCE_SENDER_RE.search(sender_l) or _BOUNCE_SUBJECT_RE.search(text):
        return CATEGORY_BOUNCE

    # OOO
    if _OOO_RE.search(text):
        return CATEGORY_OOO

    # Explicit unsubscribe / not interested
    if _UNSUB_RE.search(text):
        return CATEGORY_NOT_INTERESTED

    # Redirect
    if _REDIRECT_RE.search(text) and _EMAIL_RE.search(text):
        return CATEGORY_REDIRECT

    return None  # need Claude


def _extract_ooo_return_date(body: str) -> str:
    m = _RETURN_DATE_RE.search(body)
    if m:
        raw = m.group(3).strip()
        # Try to parse
        for fmt in ("%B %d", "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y"):
            try:
                dt = datetime.strptime(raw, fmt)
                # If no year, assume current year
                if dt.year == 1900:
                    dt = dt.replace(year=date.today().year)
                return dt.date().isoformat()
            except ValueError:
                continue
    return ""


def _extract_new_email(body: str, sender: str) -> str:
    emails = _EMAIL_RE.findall(body)
    sender_email = _EMAIL_RE.search(sender)
    sender_addr = sender_email.group(0).lower() if sender_email else ""
    for e in emails:
        if e.lower() != sender_addr and not e.lower().endswith("deboerfreight.com"):
            return e
    return ""


def _extract_bounce_info(body: str, subject: str) -> dict:
    text = f"{subject}\n{body}"
    code_m = _BOUNCE_CODE_RE.search(text)
    code = code_m.group(0) if code_m else ""
    is_soft = bool(_SOFT_BOUNCE_RE.search(text)) or (code.startswith("4") if code else False)
    return {
        "bounce_code": code,
        "bounce_type": "soft" if is_soft else "hard",
    }


def _call_claude_classify(subject: str, body: str, sender: str) -> dict:
    """Call AI for classification. Uses Gemini (existing pattern in ai_parser.py).
    Falls back gracefully if unavailable.
    """
    try:
        from app.ai_parser import _call_gemini, _extract_json
        prompt = _CLASSIFY_PROMPT.format(
            sender=sender[:200],
            subject=subject[:200],
            body=body[:2000],
        )
        raw = _call_gemini(prompt, max_tokens=512)
        return _extract_json(raw)
    except Exception as e:
        logger.warning("Claude/Gemini classify call failed: %s -- falling back to regex", e)
        return {}


def classify_reply(subject: str, body: str, sender: str) -> ClassifiedReply:
    """Classify a carrier email reply into one of six categories.

    Uses fast regex pre-classifier first; falls back to Claude/Gemini for
    ambiguous cases. Returns ClassifiedReply dataclass.
    """
    fast_cat = _fast_classify(subject, body, sender)
    extracted: dict = {}
    confidence = "high" if fast_cat else "medium"
    action = ""

    if fast_cat == CATEGORY_BOUNCE:
        extracted = _extract_bounce_info(body, subject)
        action = "mark email invalid" if extracted.get("bounce_type") == "hard" else "retry after 24h"

    elif fast_cat == CATEGORY_OOO:
        rd = _extract_ooo_return_date(body)
        # Amendment 1: if body also contains a redirect email, upgrade to ooo_redirect
        new_email = _extract_new_email(body, sender)
        if new_email:
            fast_cat = CATEGORY_OOO_REDIRECT
            extracted = {"return_date": rd, "new_email": new_email}
            action = f"send E1 to {new_email}; mark ooo_redirected with return date {rd}"
        else:
            extracted = {"return_date": rd}
            action = f"pause follow-up until {rd}" if rd else "delay follow-up sequence"

    elif fast_cat == CATEGORY_NOT_INTERESTED:
        action = "mark excluded, no reply"

    elif fast_cat == CATEGORY_REDIRECT:
        new_email = _extract_new_email(body, sender)
        extracted = {"new_email": new_email}
        action = f"update email to {new_email}, queue fresh E1" if new_email else "extract new email manually"

    else:
        # Claude path for interested / need_more_info / ambiguous
        result = _call_claude_classify(subject, body, sender)
        if result:
            fast_cat = result.get("category", "")
            confidence = result.get("confidence", "medium")
            extracted = result.get("extracted_data", {})
            action = result.get("action", "")
        else:
            # Last resort: any short positive reply = interested
            fast_cat = CATEGORY_INTERESTED
            confidence = "low"
            action = "notify Derek, manual review"

    # Validate category
    if fast_cat not in ALL_CATEGORIES:
        logger.warning("Unknown category %r from classifier -- defaulting to interested", fast_cat)
        fast_cat = CATEGORY_INTERESTED
        confidence = "low"

    return ClassifiedReply(
        category=fast_cat,
        confidence=confidence,
        extracted_data=extracted,
        action=action,
        raw_subject=subject,
        raw_sender=sender,
    )


# ── Reply draft generation (Fix 6, 2026-04-15) ───────────────────────────────

_REPLY_DRAFT_PROMPT = """\
You are Derek deBoer, owner/operator of deBoer Freight, a small licensed freight brokerage in Key West, Florida. You specialize in moving commodities and building materials in and out of Florida.

A carrier just replied to your cold outreach email. Draft a SHORT, natural, professional response.

Carrier: {legal_name} (DOT {dot}), based in {city}, {state}
Their equipment: {equipment_types}

The original reply they sent you:
---
{reply_body}
---

Draft your response as plain text (no subject line -- it's in-thread).

Rules:
- 3-6 sentences max. Short.
- Match their tone. Formal if they're formal, casual if they're casual.
- If they asked a specific question, answer it directly
- If they expressed interest, briefly outline next steps: you'll need W-9, COI ($1M auto / $100K cargo), operating authority, and ACH info to get them set up
- If they mentioned specific lanes or equipment, acknowledge what they said
- Do NOT use "I hope this email finds you well", "Best regards", "Sincerely", "Please don't hesitate", or any other AI-sounding phrase
- Sign off with "Thanks, Derek" on its own line. No title, no footer, no signature block -- just "Thanks, Derek"
- No em-dashes. Use periods and commas.
- Sound like a human wrote it in 30 seconds\
"""


def _generate_reply_draft(carrier: dict, reply_body: str) -> str:
    """Call Claude (via existing ai_parser Gemini interface) to draft a reply.

    Returns the draft as a plain-text string. On failure, returns a minimal
    fallback so the approval flow always has something to offer Derek.
    """
    legal_name = (
        carrier.get("DBA_Name") or carrier.get("Legal_Name") or carrier.get("Company Name") or ""
    ).strip()
    dot = (carrier.get("DOT_Number") or carrier.get("DOT Number") or "").strip()
    city = (carrier.get("City") or "").strip()
    state = (carrier.get("State") or "").strip()
    eq = (carrier.get("Equipment_Type") or carrier.get("Equipment Types") or "").strip()

    prompt = _REPLY_DRAFT_PROMPT.format(
        legal_name=legal_name or "this carrier",
        dot=dot or "unknown",
        city=city or "unknown",
        state=state or "unknown",
        equipment_types=eq or "not specified",
        reply_body=reply_body[:1500],
    )

    try:
        from app.ai_parser import _call_gemini
        draft = _call_gemini(prompt, max_tokens=512)
        draft = draft.strip()
        if draft:
            return draft
    except Exception as e:
        logger.warning("_generate_reply_draft: LLM call failed: %s -- using fallback", e)

    # Minimal fallback if LLM unavailable
    contact = (carrier.get("Contact Name") or "").strip()
    greeting = f"Hi {contact}," if contact else "Hi,"
    return (
        f"{greeting}\n\n"
        f"Thanks for getting back to me. To get you set up in our system, we'll need a W-9, "
        f"COI ($1M auto / $100K cargo), copy of your operating authority, and ACH info. "
        f"Reply with those as PDFs and we'll get you active.\n\n"
        f"Thanks, Derek"
    )


def _post_draft_approval_slack(
    carrier: dict,
    reply_body: str,
    classified: "ClassifiedReply",
    draft: str,
    draft_id: str,
    approve_url: str,
) -> None:
    """Post a Slack DM to Derek with the draft and approval tap-link."""
    legal_name = (
        carrier.get("DBA_Name") or carrier.get("Legal_Name") or carrier.get("Company Name") or "unknown"
    ).strip()
    dot = (carrier.get("DOT_Number") or carrier.get("DOT Number") or "").strip()
    state = (carrier.get("State") or "").strip()

    reply_excerpt = reply_body[:300].replace("\n", " ").strip()
    draft_preview = draft[:400].strip()

    msg = (
        f"*Carrier reply — {legal_name} (DOT {dot}, {state})*\n"
        f"Category: `{classified.category}` | Confidence: `{classified.confidence}`\n\n"
        f"*Their reply:*\n> {reply_excerpt}\n\n"
        f"*Draft response:*\n```{draft_preview}```\n\n"
        f"<{approve_url}|Tap to review, edit, and send>\n"
        f"_(24h to action)_"
    )
    notify_slack(msg)


# ── Action router ─────────────────────────────────────────────────────────────

def _get_carrier_summary(dot: str) -> str:
    """Return a short carrier summary string for Slack messages."""
    try:
        from app.sheets import get_carrier_by_dot
        c = get_carrier_by_dot(dot)
        if c:
            name = (c.get("DBA_Name") or c.get("Legal_Name") or c.get("Company Name") or dot)
            state = (c.get("State") or "")
            email = (c.get("Contact Email") or c.get("Primary_Email") or "")
            return f"{name} ({state}) <{email}>"
    except Exception:
        pass
    return f"DOT={dot}"


def _update_carrier_outreach_status(dot: str, outreach_status: str = "", onboarding_status: str = "") -> None:
    """Write Outreach_Status and/or Onboarding_Status to the carrier's sheet row."""
    updates = {}
    if outreach_status:
        updates["Outreach_Status"] = outreach_status
    if onboarding_status:
        updates["Onboarding_Status"] = onboarding_status
    if updates:
        try:
            from app.sheets import update_carrier_fields_by_dot
            update_carrier_fields_by_dot(dot, updates)
        except Exception as e:
            logger.error("Failed to update carrier DOT=%s: %s", dot, e)


def _send_docs_request(dot: str) -> None:
    """Send E4 docs-request email in-thread.

    Gated by OUTREACH_AUTO_REPLY_ENABLED. When disabled, logs and no-ops.
    """
    if not get_settings().OUTREACH_AUTO_REPLY_ENABLED:
        logger.info("AUTO-REPLY DISABLED: would send E4 docs request to DOT=%s", dot)
        return

    try:
        from app.sheets import get_carrier_by_dot
        from app.gmail import reply_to_thread
        c = get_carrier_by_dot(dot)
        if not c:
            logger.warning("Cannot send docs request -- carrier DOT=%s not found", dot)
            return

        email = (c.get("Contact Email") or c.get("Primary_Email") or "").strip()
        thread_id = (c.get("Outreach_Thread_Id") or c.get("Outreach Thread Id") or "").strip()
        contact_name = ""
        cn = (c.get("Contact Name") or "").strip()
        if cn:
            contact_name = cn  # preserve original casing
        legal_name = (c.get("DBA_Name") or c.get("Legal_Name") or c.get("Company Name") or "").strip()

        body = _render_e4_body(contact_name, legal_name)
        subject = "Re: Introduction -- deBoer Freight"

        if thread_id:
            reply_to_thread(thread_id=thread_id, to=email, subject=subject, body_text=body)
        else:
            from app.gmail import send_email
            send_email(to=email, subject=subject, body_text=body)

        _update_carrier_outreach_status(dot, onboarding_status="docs_requested")
        logger.info("Sent E4 docs request to DOT=%s (%s)", dot, email)
    except Exception as e:
        logger.error("Failed to send E4 for DOT=%s: %s", dot, e)


def _render_e4_body(contact_name: str, legal_name: str) -> str:
    greeting = f"Hi {contact_name}," if contact_name else "Hi,"
    return f"""{greeting}

Good to hear from you. To get you set up in our system, we need four things:

1. W-9 (signed)
2. Certificate of Insurance showing auto liability >= $1,000,000 and cargo >= $100,000, with deBoer Freight listed as certificate holder
3. Copy of your operating authority
4. ACH / direct deposit info for payment

Reply with those as PDFs and we'll get you active. Once you're in, loads start coming your way immediately.

Thanks,
Derek deBoer
www.deboerfreight.com
(305) 767-3480
MC 1712065"""


def _canned_reply_for_simple_question(question_text: str) -> Optional[str]:
    """Return a canned answer for simple FAQ questions, or None for escalation."""
    q = question_text.lower()
    if any(kw in q for kw in ("lane", "route", "where", "origin", "destination")):
        return (
            "We primarily move FL-origin freight to Southeast and Midwest destinations. "
            "Most loads are dry van, flatbed, or box truck. "
            "Reply with your preferred lanes and we'll match you when something fits."
        )
    if any(kw in q for kw in ("rate", "pay", "price", "how much", "what do you pay")):
        return (
            "Rates depend on the lane and load. We pay market rates, with quick-pay options available. "
            "Share your target rate per mile on your regular lanes and we'll see if we can match it."
        )
    if any(kw in q for kw in ("equipment", "what do you need", "what type")):
        return (
            "We primarily need dry van, flatbed, and box truck. "
            "No reefer on this account. Let us know what you're running."
        )
    return None  # escalate to Derek


def _generate_and_post_draft(
    carrier_dot: str,
    reply_body: str,
    classified: "ClassifiedReply",
) -> None:
    """Generate a Claude draft, store in GCS, post Slack DM with approval link.

    Called for: interested, need_more_info, redirect categories.
    Safe to call in test mode — GCS and Slack calls can be patched.
    """
    import os

    try:
        from app.sheets import get_carrier_by_dot
        carrier = get_carrier_by_dot(carrier_dot) or {}
    except Exception as e:
        logger.error("_generate_and_post_draft: could not load carrier DOT=%s: %s", carrier_dot, e)
        carrier = {}

    # Generate draft
    draft_text = _generate_reply_draft(carrier, reply_body)

    # Store in GCS
    draft_id = str(uuid.uuid4())
    draft_data = {
        "draft_id": draft_id,
        "carrier_dot": carrier_dot,
        "carrier_name": (
            carrier.get("DBA_Name") or carrier.get("Legal_Name") or carrier.get("Company Name") or ""
        ),
        "original_reply": reply_body,
        "draft": draft_text,
        "thread_id": (carrier.get("Outreach_Thread_Id") or "").strip(),
        "contact_email": (carrier.get("Contact Email") or carrier.get("Primary_Email") or "").strip(),
        "category": classified.category,
        "confidence": classified.confidence,
        "used": False,
        "sent": False,
    }

    gcs_uri = ""
    try:
        from app.reply_draft_store import store_reply_draft
        gcs_uri = store_reply_draft(draft_id, draft_data)
        logger.info("Reply draft stored: draft_id=%s uri=%s", draft_id, gcs_uri)
    except Exception as e:
        logger.error("_generate_and_post_draft: GCS write failed for DOT=%s: %s", carrier_dot, e)

    # Build approval URL (24h TTL)
    approve_url = ""
    try:
        secret = os.environ.get("APPROVAL_SIGNING_SECRET", "")
        service_url = get_settings().SERVICE_URL
        if secret:
            from app.signed_urls import sign_token
            signed = sign_token({"batch_id": draft_id}, secret, ttl_seconds=86400)  # 24h
            approve_url = (
                f"{service_url}/reply-approve"
                f"?token={signed['token']}"
                f"&sig={signed['sig']}"
                f"&exp={signed['exp']}"
            )
    except Exception as e:
        logger.error("_generate_and_post_draft: URL signing failed for DOT=%s: %s", carrier_dot, e)
        approve_url = f"(draft_id={draft_id} — check GCS)"

    # Post Slack DM
    try:
        _post_draft_approval_slack(carrier, reply_body, classified, draft_text, draft_id, approve_url)
    except Exception as e:
        logger.error("_generate_and_post_draft: Slack post failed for DOT=%s: %s", carrier_dot, e)


def route_classified_reply(
    classified: ClassifiedReply,
    carrier_dot: str,
    reply_body: str = "",
) -> None:
    """Route a classified reply to the appropriate action.

    Fix 6 (2026-04-15): interested / need_more_info / redirect categories now
    generate a Claude draft and post a Slack DM with an approval tap-link.
    The old auto-E4 path (Amendment 2) is DISABLED -- see process_scheduled_doc_requests.

    Actions never fire sends/writes directly -- they call helpers that are
    themselves gated by OUTREACH_AUTO_REPLY_ENABLED or remain log-only.
    Sheet writes happen regardless of auto-reply flag.
    """
    cat = classified.category
    summary = _get_carrier_summary(carrier_dot)

    logger.info(
        "Routing reply DOT=%s category=%s confidence=%s action=%s",
        carrier_dot, cat, classified.confidence, classified.action,
    )

    if cat == CATEGORY_INTERESTED:
        # Fix 6: generate a Claude draft + post for Derek's approval instead of auto-E4.
        _update_carrier_outreach_status(
            carrier_dot,
            outreach_status="replied_interested",
            onboarding_status="replied_interested",
        )
        _generate_and_post_draft(carrier_dot, reply_body, classified)
        logger.info("DOT=%s interested reply -- draft generated, pending Derek approval", carrier_dot)

    elif cat == CATEGORY_NOT_INTERESTED:
        _update_carrier_outreach_status(
            carrier_dot,
            outreach_status="replied_not_interested",
            onboarding_status="rejected",
        )
        # Fix 6: FYI-only Slack DM; no draft, no auto-reply.
        notify_slack(f"FYI: {summary} marked not interested. No action needed.")
        logger.info("DOT=%s marked not_interested -- permanent exclusion", carrier_dot)

    elif cat == CATEGORY_NEED_MORE_INFO:
        # Fix 6: draft instead of canned auto-reply
        _update_carrier_outreach_status(carrier_dot, outreach_status="replied_interested")
        _generate_and_post_draft(carrier_dot, reply_body, classified)
        logger.info("DOT=%s need_more_info -- draft generated, pending Derek approval", carrier_dot)

    elif cat == CATEGORY_OOO:
        return_date = classified.extracted_data.get("return_date", "")
        _update_carrier_outreach_status(carrier_dot, outreach_status="ooo_paused")
        if return_date:
            logger.info(
                "DOT=%s OOO until %s -- E2/E3 follow-up sequence paused until %s",
                carrier_dot, return_date, return_date,
            )
            # Scheduler wiring: the follow-up runner checks Outreach_Status=ooo_paused
            # AND compares Outreach_E1_SentAt + resume date before sending E2/E3.
            # That logic lives in the follow-up scheduler (future wiring).
        else:
            logger.info("DOT=%s OOO -- no return date found, pausing follow-up sequence", carrier_dot)

    elif cat == CATEGORY_OOO_REDIRECT:
        # Amendment 1: OOO + redirect combined. The redirect path takes over;
        # we do NOT pause the pipeline — the new contact is now primary.
        new_email = classified.extracted_data.get("new_email", "")
        return_date = classified.extracted_data.get("return_date", "")
        if new_email:
            try:
                from app.sheets import get_carrier_by_dot, update_carrier_fields_by_dot
                c = get_carrier_by_dot(carrier_dot)
                if c:
                    old_email = (c.get("Contact Email") or c.get("Primary_Email") or "").strip()
                    updates: dict = {
                        "Primary_Email": new_email,
                        "Outreach_Status": "ooo_redirected",
                        "Internal_Notes": (
                            f"{c.get('Notes') or c.get('Internal_Notes') or ''}; "
                            f"OOO redirect from {old_email} to {new_email} on "
                            f"{date.today().isoformat()}"
                            + (f"; OOO return {return_date}" if return_date else "")
                        ).lstrip("; "),
                    }
                    if return_date:
                        updates["Outreach_OOO_Return_Date"] = return_date
                    update_carrier_fields_by_dot(carrier_dot, updates)
                    logger.info(
                        "DOT=%s ooo_redirect: old=%s new=%s return_date=%s",
                        carrier_dot, old_email, new_email, return_date,
                    )
                    # Reset so batch picks up fresh E1 to the new address
                    _update_carrier_outreach_status(carrier_dot, outreach_status="none")
                    notify_slack(
                        f"Carrier OOO+redirect DOT={carrier_dot}: "
                        f"old={old_email} new={new_email} "
                        + (f"returns {return_date} " if return_date else "")
                        + "-- queued for fresh E1 to new contact"
                    )
            except Exception as e:
                logger.error("Failed to process ooo_redirect for DOT=%s: %s", carrier_dot, e)
        else:
            # No email extractable: treat as pure OOO
            logger.warning(
                "DOT=%s ooo_redirect category but new_email is blank -- falling back to ooo_paused",
                carrier_dot,
            )
            _update_carrier_outreach_status(carrier_dot, outreach_status="ooo_paused")

    elif cat == CATEGORY_BOUNCE:
        bounce_type = classified.extracted_data.get("bounce_type", "hard")
        bounce_code = classified.extracted_data.get("bounce_code", "")

        if bounce_type == "hard":
            # Mark email invalid, permanent email exclusion
            try:
                from app.sheets import get_carrier_by_dot
                c = get_carrier_by_dot(carrier_dot)
                if c:
                    email = (c.get("Contact Email") or c.get("Primary_Email") or "").strip()
                    from app.sheets import update_carrier_fields_by_dot
                    updates: dict = {"Primary_Email": f"{email}_INVALID"}
                    phone = (c.get("Contact Phone") or c.get("Primary_Phone") or "").strip()
                    if phone:
                        updates["Internal_Notes"] = (
                            f"{c.get('Notes') or c.get('Internal_Notes') or ''}; "
                            f"Email bounced hard ({bounce_code}) {date.today().isoformat()} -- phone-first"
                        ).lstrip("; ")
                    update_carrier_fields_by_dot(carrier_dot, updates)
                    logger.info("DOT=%s hard bounce (%s) -- marked %s_INVALID", carrier_dot, bounce_code, email)
                    if phone:
                        notify_slack(
                            f"Hard bounce DOT={carrier_dot} ({email}) -- "
                            f"phone available: {phone}. Consider phone outreach."
                        )
            except Exception as e:
                logger.error("Failed to process hard bounce for DOT=%s: %s", carrier_dot, e)
            _update_carrier_outreach_status(carrier_dot, outreach_status="bounced")

        else:  # soft bounce
            logger.info("DOT=%s soft bounce (%s) -- will retry once after 24h", carrier_dot, bounce_code)
            # Soft retry: the outreach scheduler checks for soft-bounce state
            # and queues a retry 24h later. Second soft = hard treatment.
            # Status stays at E1_SENT to allow the retry path.

    elif cat == CATEGORY_REDIRECT:
        new_email = classified.extracted_data.get("new_email", "")
        if new_email:
            try:
                from app.sheets import get_carrier_by_dot
                c = get_carrier_by_dot(carrier_dot)
                if c:
                    old_email = (c.get("Contact Email") or c.get("Primary_Email") or "").strip()
                    from app.sheets import update_carrier_fields_by_dot
                    update_carrier_fields_by_dot(carrier_dot, {
                        "Primary_Email": new_email,
                        "Internal_Notes": (
                            f"{c.get('Notes') or c.get('Internal_Notes') or ''}; "
                            f"Redirected from {old_email} to {new_email} on {date.today().isoformat()}"
                        ).lstrip("; "),
                    })
                    logger.info("DOT=%s redirect: %s -> %s", carrier_dot, old_email, new_email)
                    # Reset outreach status so the batch picks them up fresh
                    _update_carrier_outreach_status(carrier_dot, outreach_status="none")
                    notify_slack(
                        f"Carrier redirect DOT={carrier_dot}: "
                        f"old={old_email} new={new_email} -- queued for fresh E1"
                    )
            except Exception as e:
                logger.error("Failed to process redirect for DOT=%s: %s", carrier_dot, e)
        else:
            notify_slack(
                f"Carrier redirect DOT={carrier_dot} but could not extract new email. "
                f"Derek -- manual update needed."
            )


# ── Scheduled doc-request processor (DISABLED — Fix 6, 2026-04-15) ───────────

def process_scheduled_doc_requests() -> int:
    """DISABLED: was Amendment 2 auto-E4 scheduler.

    Fix 6 (2026-04-15): the auto-E4 path is superseded by the draft-and-approve
    flow. Interested carrier replies now generate a Claude draft and route to
    Derek for approval via /reply-approve instead of auto-firing E4.

    This function is a stub that logs and returns 0. Any still-scheduled E4s
    (rows with Onboarding_Status='docs_request_scheduled') are NOT fired.
    Derek can approve a draft that contains the docs ask, which is what E4 did.
    """
    logger.info(
        "process_scheduled_doc_requests: DISABLED (Fix 6) -- "
        "auto-E4 superseded by draft-and-approve flow"
    )
    return 0
