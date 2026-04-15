"""
CLI script: unblock a pending Sofia E1 outreach batch.

Usage:
    python scripts/approve_outreach_batch.py <token>
    python scripts/approve_outreach_batch.py <token> --reject

The script finds the approval JSON file with the matching token and sets
approved=True (or rejected=True). The run_daily_outreach_batch() polling
loop picks this up within ~30 seconds.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

LOGS_DIR = Path("scripts/logs")


def main() -> None:
    parser = argparse.ArgumentParser(description="Approve or reject a Sofia outreach batch.")
    parser.add_argument("token", help="Approval token from the Slack message")
    parser.add_argument("--reject", action="store_true", help="Reject instead of approve")
    args = parser.parse_args()

    token = args.token.strip()
    found = False

    for path in LOGS_DIR.glob("outreach_approval_*.json"):
        try:
            with open(path) as f:
                state = json.load(f)
        except Exception as e:
            print(f"Could not read {path}: {e}", file=sys.stderr)
            continue

        if state.get("token") != token:
            continue

        found = True
        if state.get("approved") or state.get("rejected"):
            print(f"Token {token} already resolved (approved={state.get('approved')} rejected={state.get('rejected')}).")
            sys.exit(0)

        if args.reject:
            state["rejected"] = True
            print(f"Batch {token} REJECTED.")
        else:
            state["approved"] = True
            print(f"Batch {token} APPROVED. Sends will begin within 30 seconds.")

        with open(path, "w") as f:
            json.dump(state, f, indent=2)

        # Print the carrier list for confirmation
        carriers = state.get("carriers", [])
        print(f"\n{len(carriers)} carriers in this batch:")
        for c in carriers:
            print(f"  DOT={c.get('dot','?'):12} {c.get('legal_name','?')[:32]:34} {c.get('email','?')}")
        break

    if not found:
        print(f"No pending batch found with token {token!r}.", file=sys.stderr)
        print("Check scripts/logs/ for outreach_approval_*.json files.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
