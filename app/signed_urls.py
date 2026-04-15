"""
BrokerOps AI — Signed URL helper module.

Used by the mobile approval flow to generate and verify tamper-evident,
time-limited URLs for carrier outreach batch approve/cancel actions.

Security notes:
  - HMAC-SHA256 with a 32-byte random secret (APPROVAL_SIGNING_SECRET env var).
  - hmac.compare_digest used for constant-time comparison (timing attack prevention).
  - Canonical sign string: "{batch_id}:{exp}" — no other inputs to prevent injection.
  - Expiration is checked server-side on every verify call.
  - One-shot flag (batch.used) is enforced by pending_batch_store, not here.
"""
from __future__ import annotations

import hmac
import hashlib
import time
from typing import Tuple


def sign_token(payload: dict, secret: str, ttl_seconds: int = 21600) -> dict:
    """
    Generate a signed URL token for a batch approval/cancel action.

    Args:
        payload:     Must contain at minimum a 'batch_id' str.
        secret:      HMAC signing secret (APPROVAL_SIGNING_SECRET env var).
        ttl_seconds: Token lifetime in seconds. Default 6 hours (21600).

    Returns:
        dict with keys: token (str), sig (str), exp (int)
    """
    exp = int(time.time()) + ttl_seconds
    # Canonical string to sign: "{batch_id}:{exp}"
    msg = f"{payload['batch_id']}:{exp}".encode()
    sig = hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()
    return {"token": payload["batch_id"], "sig": sig, "exp": exp}


def verify_token(token: str, sig: str, exp: int, secret: str) -> Tuple[bool, str]:
    """
    Verify a signed URL token.

    Args:
        token:  The batch_id (token query param).
        sig:    HMAC hex digest (sig query param).
        exp:    Unix expiration timestamp (exp query param).
        secret: HMAC signing secret (APPROVAL_SIGNING_SECRET env var).

    Returns:
        (valid: bool, reason: str)
        reason is empty string if valid; otherwise explains why verification failed.
    """
    if int(time.time()) > int(exp):
        return False, "expired"
    expected_sig = hmac.new(
        secret.encode(),
        f"{token}:{exp}".encode(),
        hashlib.sha256,
    ).hexdigest()
    # Constant-time comparison — prevents timing attacks
    if not hmac.compare_digest(sig, expected_sig):
        return False, "signature mismatch"
    return True, ""
