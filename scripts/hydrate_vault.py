"""
scripts/hydrate_vault.py — CLI shim for vault hydration.

Delegates to app.hydrate_from_vault.main().

Usage:
    python scripts/hydrate_vault.py --dry-run
    python scripts/hydrate_vault.py --dry-run --tier operations
    python scripts/hydrate_vault.py --tier operations   # live write
"""
import sys
import os

# Ensure brokerops-ai root is on sys.path regardless of cwd
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.hydrate_from_vault import main  # noqa: E402

if __name__ == "__main__":
    main()
