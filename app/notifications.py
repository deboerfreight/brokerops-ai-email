"""
BrokerOps AI – Slack notifications.

Replaces the _notify_slack() stubs that used to live inline in
outreach_reply.py and mdl_vendor_outreach_dispatcher.py. Posts to the
webhook URL configured in Settings.SLACK_WEBHOOK_URL. If the URL is
blank, falls back to logger-only (the original stub behavior).
"""
from __future__ import annotations

import logging

import httpx

from app.config import get_settings

logger = logging.getLogger("brokerops.notifications")

# Short cache of the webhook URL to avoid reading Settings on every call.
# Settings is already lru_cached; this is just for symmetry.
_DEFAULT_TIMEOUT_SEC = 5.0


def notify_slack(msg: str) -> bool:
    """Post a plain-text message to the configured Slack webhook.

    Returns True on successful send, False on any error (including missing
    webhook URL). Errors are logged but never raised — Slack notifications
    are advisory and must not break the calling workflow.

    When SLACK_WEBHOOK_URL is blank, emits a `[SLACK STUB]` log line and
    returns False. This preserves the original stub behavior so workflows
    can run without Slack wiring.
    """
    webhook = get_settings().SLACK_WEBHOOK_URL
    if not webhook:
        logger.info("[SLACK STUB] %s", msg)
        return False
    try:
        resp = httpx.post(
            webhook,
            json={"text": msg},
            timeout=_DEFAULT_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        logger.debug("Slack notification sent: %s", msg[:120])
        return True
    except Exception as exc:
        logger.warning("Slack notification failed, falling back to log: %s", exc)
        logger.info("[SLACK STUB] %s", msg)
        return False
