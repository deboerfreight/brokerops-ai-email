"""
Ensure every OPS/* Gmail label the codebase references actually exists.

OPTION A (chosen): create missing labels, leave code untouched.
Rationale: git log shows "OPS/OUTREACH_REPLY" is the name the code was
written around (one commit); "OPS/CARRIER_RESPONSES" has zero git history
and is almost certainly a legacy hand-created label. The code (e.g.
inbox_scanner.py:85 via _ensure_label) already assumes OUTREACH_REPLY is
the canonical name. Fixing the label side rather than repointing code keeps
the naming consistent with everything else under OPS/*.

Usage:

    # Dry-run (default) — prints which labels exist, which are missing.
    python scripts/create_ops_labels.py

    # Create any missing labels.
    python scripts/create_ops_labels.py --apply

Dry-run writes nothing. Derek reviews before --apply.
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.google_auth import get_gmail_service  # noqa: E402

logger = logging.getLogger("brokerops.create_ops_labels")

# Directories to scan for label references.
SCAN_DIRS = [
    _REPO_ROOT / "app",
    _REPO_ROOT / "scripts",
]
# Files to skip (this script itself, cache dirs).
SKIP_NAMES = {"create_ops_labels.py", "__pycache__"}

OPS_LABEL_RE = re.compile(r'["\'](OPS/[A-Z0-9_]+)["\']')


def _discover_referenced_labels() -> dict[str, list[str]]:
    """Grep the codebase for 'OPS/...' label string literals.

    Returns a dict: {label_name: [relative_path_where_found, ...]}.
    """
    found: dict[str, set[str]] = {}
    for base in SCAN_DIRS:
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            if path.name in SKIP_NAMES:
                continue
            if any(part in SKIP_NAMES for part in path.parts):
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except Exception as e:
                logger.warning("Could not read %s: %s", path, e)
                continue
            for match in OPS_LABEL_RE.findall(text):
                rel = str(path.relative_to(_REPO_ROOT))
                found.setdefault(match, set()).add(rel)
    return {k: sorted(v) for k, v in sorted(found.items())}


def _list_existing_labels(svc) -> dict[str, str]:
    """Return {label_name: label_id} for all Gmail labels on the account."""
    resp = svc.users().labels().list(userId="me").execute()
    return {lbl["name"]: lbl["id"] for lbl in resp.get("labels", [])}


def _create_label(svc, name: str) -> str:
    """Create a Gmail label, return its ID."""
    result = svc.users().labels().create(
        userId="me",
        body={
            "name": name,
            "labelListVisibility": "labelShow",
            "messageListVisibility": "show",
        },
    ).execute()
    return result["id"]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ensure OPS/* Gmail labels referenced in the codebase exist.")
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually create missing labels. Default is dry-run (read-only).",
    )
    return p.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    args = _parse_args()
    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info("Starting OPS/* label audit in %s mode.", mode)

    referenced = _discover_referenced_labels()
    logger.info("Discovered %d distinct OPS/* label reference(s) in code:", len(referenced))
    for name, sources in referenced.items():
        logger.info("  %-30s  (in: %s)", name, ", ".join(sources))

    svc = get_gmail_service()
    existing = _list_existing_labels(svc)
    logger.info("Gmail account has %d label(s) total.", len(existing))

    missing: list[str] = []
    present: list[str] = []
    for name in referenced:
        if name in existing:
            present.append(name)
        else:
            missing.append(name)

    logger.info("── Present (%d) ──", len(present))
    for name in present:
        logger.info("  OK  %-30s id=%s", name, existing[name])

    logger.info("── Missing (%d) ──", len(missing))
    for name in missing:
        logger.info("  --  %s", name)

    # Also surface non-referenced OPS/* labels that exist in Gmail (informational).
    stray = sorted(
        name for name in existing
        if name.startswith("OPS/") and name not in referenced
    )
    if stray:
        logger.info("── Existing OPS/* labels NOT referenced by code (informational) ──")
        for name in stray:
            logger.info("  ??  %-30s id=%s", name, existing[name])

    if not missing:
        logger.info("All referenced OPS/* labels exist. Nothing to do.")
        return 0

    if not args.apply:
        logger.info(
            "DRY-RUN complete. %d label(s) would be created. "
            "Re-run with --apply to create.",
            len(missing),
        )
        return 0

    # APPLY
    created = 0
    errors = 0
    for name in missing:
        try:
            lbl_id = _create_label(svc, name)
            logger.info("Created label '%s' (id=%s)", name, lbl_id)
            created += 1
        except Exception as e:
            logger.exception("Failed to create label '%s': %s", name, e)
            errors += 1

    logger.info(
        "APPLY complete. created=%d errors=%d total_missing=%d",
        created, errors, len(missing),
    )
    return 0 if errors == 0 else 5


if __name__ == "__main__":
    sys.exit(main())
