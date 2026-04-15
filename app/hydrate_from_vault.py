"""
hydrate_from_vault.py — Vault-to-.env hydration for BrokerOps AI
Owner: Bolt (Backend Developer & Systems Architect)
Version: 1.0.0
Created: 2026-04-15

Reads Fernet-encrypted secrets from the org vault (org.db) and writes them
into .env, preserving all existing non-vault lines, comments, and blank lines.

Vault source of truth:
    C:/Users/Owner/Desktop/Claude Work/team/org/org.db
    C:/Users/Owner/Desktop/Claude Work/team/org/.vault_key

Usage (dry-run):
    python -m app.hydrate_from_vault --dry-run
    python -m app.hydrate_from_vault --dry-run --tier operations

Usage (live — Rex/Sasha only after all keys are in vault):
    python -m app.hydrate_from_vault --tier operations
"""

from __future__ import annotations

import os
import re
import sys
import sqlite3
import logging
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_ORG_DIR = Path("C:/Users/Owner/Desktop/Claude Work/team/org")
_VAULT_DB = _ORG_DIR / "org.db"
_VAULT_KEY = _ORG_DIR / ".vault_key"
_DEFAULT_ENV = Path("C:/Users/Owner/brokerops-ai/.env")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
_LOG_FILE = Path("C:/Users/Owner/brokerops-ai/scripts/logs/hydrate_vault_build_20260415.log")
_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.FileHandler(_LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("hydrate_from_vault")


# ---------------------------------------------------------------------------
# Vault helpers (mirrors db_manager.py — read-only, no import coupling)
# ---------------------------------------------------------------------------

def _get_fernet():
    """Load the Fernet key from the org vault key file. Raises if missing."""
    from cryptography.fernet import Fernet  # type: ignore

    if not _VAULT_KEY.exists():
        raise FileNotFoundError(
            f"Vault key not found at {_VAULT_KEY}. "
            "Cannot decrypt secrets without it."
        )
    key = _VAULT_KEY.read_bytes().strip()
    return Fernet(key)


def _decrypt(fernet, token: bytes) -> str:
    return fernet.decrypt(token).decode()


def _redact(key: str, value: str) -> str:
    """Return a safe display string: KEY=<N chars>."""
    return f"{key}=<{len(value)} chars>"


def _vault_list_by_tier(tiers: list[str]) -> list[dict]:
    """
    Return list of {key_name, encrypted_value, access_tier} from the vault
    filtered to the requested tiers. Read-only, no audit writes.
    """
    if not _VAULT_DB.exists():
        raise FileNotFoundError(f"Vault DB not found: {_VAULT_DB}")

    conn = sqlite3.connect(str(_VAULT_DB))
    conn.row_factory = sqlite3.Row
    placeholders = ",".join("?" for _ in tiers)
    rows = conn.execute(
        f"SELECT key_name, encrypted_value, access_tier, description "
        f"FROM vault WHERE access_tier IN ({placeholders}) ORDER BY id",
        tiers,
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# .env parser helpers
# ---------------------------------------------------------------------------
_KEY_LINE_RE = re.compile(r"^([A-Z][A-Z0-9_]*)=(.*)$")
_TODO_RE = re.compile(r".*TODO.*migrate to vault.*hydrate_from_vault.*", re.IGNORECASE)


def _parse_env(text: str) -> dict[str, int]:
    """
    Return a dict mapping KEY -> line_index (0-based) for every KEY=VALUE
    line in the .env text.
    """
    result: dict[str, int] = {}
    for i, line in enumerate(text.splitlines()):
        m = _KEY_LINE_RE.match(line.strip())
        if m:
            result[m.group(1)] = i
    return result


def _replace_line(lines: list[str], idx: int, new_line: str) -> None:
    lines[idx] = new_line


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------

def hydrate_from_vault(
    env_path: str | None = None,
    tiers: list[str] | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Read secrets from the Fernet vault and write them to .env.

    Args:
        env_path: path to .env (default: C:/Users/Owner/brokerops-ai/.env)
        tiers:    list of vault tiers to hydrate (default: ['operations'])
        dry_run:  if True, return what would be written without touching .env

    Returns:
        dict with keys: {'written': [...], 'skipped': [...], 'errors': [...]}
    """
    env_path_obj = Path(env_path) if env_path else _DEFAULT_ENV
    tiers = tiers or ["operations"]

    result: dict[str, list] = {"written": [], "skipped": [], "errors": []}

    mode_label = "[DRY-RUN]" if dry_run else "[LIVE]"
    log.info("%s hydrate_from_vault started — env=%s  tiers=%s", mode_label, env_path_obj, tiers)

    # ── 1. Load Fernet ───────────────────────────────────────────────────────
    try:
        fernet = _get_fernet()
        log.info("%s Fernet key loaded from %s", mode_label, _VAULT_KEY)
    except Exception as exc:
        msg = f"Failed to load Fernet key: {exc}"
        log.error("%s %s", mode_label, msg)
        result["errors"].append(msg)
        return result

    # ── 2. Fetch vault secrets for requested tiers ───────────────────────────
    try:
        vault_rows = _vault_list_by_tier(tiers)
        log.info("%s Vault query returned %d secret(s) in tiers %s", mode_label, len(vault_rows), tiers)
    except Exception as exc:
        msg = f"Failed to query vault: {exc}"
        log.error("%s %s", mode_label, msg)
        result["errors"].append(msg)
        return result

    if not vault_rows:
        log.warning("%s No secrets found in tiers %s — nothing to hydrate.", mode_label, tiers)
        return result

    # ── 3. Decrypt all secrets ───────────────────────────────────────────────
    decrypted: dict[str, str] = {}
    for row in vault_rows:
        key = row["key_name"]
        try:
            value = _decrypt(fernet, row["encrypted_value"])
            decrypted[key] = value
            log.info("%s Decrypted: %s", mode_label, _redact(key, value))
        except Exception as exc:
            msg = f"Failed to decrypt '{key}': {exc}"
            log.error("%s %s", mode_label, msg)
            result["errors"].append(msg)

    if not decrypted:
        log.warning("%s All decrypt attempts failed — aborting.", mode_label)
        return result

    # ── 4. Read current .env ─────────────────────────────────────────────────
    if not env_path_obj.exists():
        msg = f".env not found at {env_path_obj}"
        log.error("%s %s", mode_label, msg)
        result["errors"].append(msg)
        return result

    env_text = env_path_obj.read_text(encoding="utf-8")
    lines = env_text.splitlines()

    # ── 5. Locate existing keys in .env and TODO comment ────────────────────
    key_positions = _parse_env(env_text)
    todo_line_idx: int | None = None
    for i, line in enumerate(lines):
        if _TODO_RE.match(line):
            todo_line_idx = i
            break

    # ── 6. Upsert each secret into lines list ────────────────────────────────
    new_keys: list[str] = []   # keys that will be appended (not already in .env)

    for key, value in decrypted.items():
        new_line = f"{key}={value}"
        if key in key_positions:
            existing_value = lines[key_positions[key]].split("=", 1)[1]
            if existing_value == value:
                log.info("%s SKIP (unchanged): %s", mode_label, _redact(key, value))
                result["skipped"].append(key)
            else:
                log.info("%s REPLACE line %d: %s", mode_label, key_positions[key] + 1, _redact(key, value))
                if not dry_run:
                    _replace_line(lines, key_positions[key], new_line)
                result["written"].append(key)
        else:
            log.info("%s APPEND (new key): %s", mode_label, _redact(key, value))
            new_keys.append(key)
            result["written"].append(key)

    # ── 7. Replace TODO comment line ─────────────────────────────────────────
    vault_header = f"# vault hydration active — hydrate_from_vault() is live (last run: {datetime.now().strftime('%Y-%m-%d')})"
    if todo_line_idx is not None:
        log.info("%s Replacing TODO comment at line %d with vault header", mode_label, todo_line_idx + 1)
        if not dry_run:
            lines[todo_line_idx] = vault_header
    else:
        log.info("%s TODO comment not found — vault header will be added with appended keys", mode_label)

    # ── 8. Append new keys under a hydration header ──────────────────────────
    if new_keys:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        append_header = f"\n# hydrated from vault {ts}"
        append_lines = [append_header]
        for key in new_keys:
            append_lines.append(f"{key}={decrypted[key]}")
        # Also append vault_header if TODO wasn't found
        if todo_line_idx is None:
            append_lines.insert(0, vault_header)
        log.info("%s Appending %d new key(s) to .env", mode_label, len(new_keys))
        if not dry_run:
            lines.extend(append_lines)

    # ── 9. Write .env (live only) ────────────────────────────────────────────
    if not dry_run:
        new_text = "\n".join(lines) + "\n"
        env_path_obj.write_text(new_text, encoding="utf-8")

        # POSIX only: set owner-only permissions (skip on Windows)
        if os.name != "nt":
            try:
                env_path_obj.chmod(0o600)
                log.info("[LIVE] Set .env permissions to 0600")
            except Exception as exc:
                log.warning("[LIVE] Could not set .env permissions: %s", exc)

        log.info("[LIVE] .env written — written=%s  skipped=%s  errors=%s",
                 result["written"], result["skipped"], result["errors"])
    else:
        log.info("[DRY-RUN] No files written — written=%s  skipped=%s  errors=%s",
                 result["written"], result["skipped"], result["errors"])

    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args(argv=None):
    import argparse
    parser = argparse.ArgumentParser(
        description="Hydrate .env from org vault (Fernet-encrypted secrets).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m app.hydrate_from_vault --dry-run
  python -m app.hydrate_from_vault --dry-run --tier operations
  python -m app.hydrate_from_vault --tier operations          # live write
  python scripts/hydrate_vault.py --dry-run
        """,
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be written without modifying .env",
    )
    parser.add_argument(
        "--tier", action="append", dest="tiers", metavar="TIER",
        help="Vault tier(s) to hydrate (default: operations). Repeat for multiple.",
    )
    parser.add_argument(
        "--env", dest="env_path", default=None,
        help="Path to .env file (default: C:/Users/Owner/brokerops-ai/.env)",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)
    tiers = args.tiers or ["operations"]
    result = hydrate_from_vault(
        env_path=args.env_path,
        tiers=tiers,
        dry_run=args.dry_run,
    )
    if result["errors"]:
        log.error("Hydration completed with errors: %s", result["errors"])
        sys.exit(1)
    written = len(result["written"])
    skipped = len(result["skipped"])
    mode = "DRY-RUN" if args.dry_run else "LIVE"
    print(f"\n[{mode}] Done — {written} key(s) would be written, {skipped} unchanged.")
    if args.dry_run:
        print("Run without --dry-run to apply changes.")


if __name__ == "__main__":
    main()
