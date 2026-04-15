# scripts/_deprecated/

This directory is a one-way archive for scripts that have been superseded by the canonical pipeline.

## Contract

- Files here are **read-only history**. Do not run them. Do not import them.
- Do not delete files from this directory — git history is not sufficient because some of these scripts contain per-state tuning comments that are useful for audit trail purposes.
- If you need to understand what a deprecated script did, read it here and then run the canonical equivalent instead.

## Why scripts end up here

A script gets archived when it:
1. Calls `insert_carrier()` directly without running `EXCLUDED_SERVICE_TYPE_PATTERNS` (the denylist)
2. Was a one-time migration or backfill that has already run
3. Has been fully absorbed into a canonical entry point

## Archived files (as of 2026-04-15)

| File | Reason | Canonical replacement |
|---|---|---|
| `mn_carrier_search_20260415.py` | Bypassed denylist; towing rows entered DB | `python -m scripts.prospect_carriers --state MN --buckets flatbed,dry_van,box_truck --limit 5` |
| `oh_carrier_search_20260415.py` | Bypassed denylist; towing rows entered DB | `python -m scripts.prospect_carriers --state OH --buckets flatbed,dry_van,reefer,box_truck --limit 10` |
| `tx_carrier_search_20260415.py` | Bypassed denylist; towing rows entered DB | `python -m scripts.prospect_carriers --state TX --buckets flatbed,dry_van,reefer,box_truck --limit 10` |

## The one-way archival contract

Once a file is moved here, it stays here. "Archive to `_deprecated/`" beats `rm`. If you need to resurrect logic, copy it into the canonical script — do not move files back out of this directory.

See `feedback_carrier_category_rules.md` for the denylist policy that triggered this consolidation.
