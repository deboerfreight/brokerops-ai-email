# Carrier Vetting Pipeline

End-to-end architecture for the BrokerOps carrier hard-reject screening
pipeline. Built 2026-04-14 to consolidate scattered ad-hoc checks into one
authoritative gate.

## Why

Three things were broken before the rebuild:

1. The `is_carrier_vetted` helper read column AG of the sheet but nothing
   guaranteed that column was current. Workflows could miss a fresh insertion
   that hadn't been swept yet.
2. The hard-reject thresholds (fleet ≥ 3, $1M liability, $100K cargo) were
   duplicated across `fmcsa.score_carrier`, `fmcsa.vet_carrier_strict`, and
   `scripts/vetting_sweep_20260414.py` — three sources of truth that could
   drift.
3. There was no quarantine. Failed carriers either silently passed (when
   data was stale) or had to be deleted from the sheet, losing the audit
   trail.

## The architecture

```
                       ┌──────────────────┐
                       │  RULES (frozen)  │
                       │ app/vetting/rules│
                       └────────┬─────────┘
                                │ thresholds
                       ┌────────▼─────────┐
                       │   vet_complete   │  app/vetting/gate.py
                       │   (canonical)    │
                       └────────┬─────────┘
                                │
        ┌───────────────────────┼─────────────────────────┐
        │                       │                         │
┌───────▼─────────┐   ┌─────────▼──────────┐   ┌──────────▼──────────┐
│ is_carrier_     │   │ validate_before_   │   │ sweep_carrier_      │
│ vetted          │   │ write              │   │ database            │
│ (fast gate,     │   │ (split passes &    │   │ (re-vet every row,  │
│  workflows)     │   │  quarantines)      │   │  optional FMCSA     │
│                 │   │                    │   │  re-fetch)          │
└─────────────────┘   └──────────┬─────────┘   └─────────────────────┘
                                 │
                       ┌─────────▼──────────┐
                       │  write_validated   │  app/vetting/writer.py
                       │  (write + post-    │
                       │   verify)          │
                       └─────────┬──────────┘
                                 │
                ┌────────────────┼────────────────┐
                │                                 │
       ┌────────▼────────┐              ┌─────────▼─────────┐
       │ insert_carrier  │              │ append_to_        │
       │ (passes only)   │              │ quarantine        │
       └─────────────────┘              └───────────────────┘
                │                                 │
                ▼                                 ▼
       ┌─────────────────┐              ┌──────────────────┐
       │ Carrier         │              │ Carrier          │
       │ Database tab    │              │ Quarantine tab   │
       │ (33 cols A-AG)  │              │ (37 cols A-AK)   │
       └─────────────────┘              └──────────────────┘
```

## The rules (single source of truth)

`app/vetting/rules.py`:

| Rule                     | Threshold            | Status code on fail        |
|--------------------------|----------------------|----------------------------|
| Fleet size               | ≥ 3 power units      | `fail_fleet_size`          |
| Liability insurance      | ≥ $1,000,000         | `fail_insurance_liability` |
| Cargo insurance          | ≥ $100,000           | `fail_insurance_cargo`     |
| Safety rating            | ≠ Unsatisfactory     | `fail_safety_rating`       |
| Vehicle OOS rate         | ≤ 30%                | `fail_vehicle_oos`         |
| Driver OOS rate          | ≤ 15%                | `fail_driver_oos`          |
| Crash rate per 100 units | ≤ 30                 | `fail_crash_rate`          |
| Reefer + any vehicle OOS inspection | zero tolerance | `fail_reefer_maintenance` |
| Shell carrier (units > 0, drivers = 0) | reject  | `fail_shell_carrier`      |

If any of fleet / liability / cargo is **missing** (blank or 0) the row is
flagged `needs_review` instead of pass — we don't pass on absence.

A carrier that clears every rule above gets `pass_basic`. **Only `pass_basic`
allows entry into the Carrier Database.**

To change a threshold, edit `app/vetting/rules.py` and re-run the daily
sweep. Every gate, sweep, and writer reads from `RULES` so the change
propagates with no other edits needed.

## When each gate fires

| Trigger                     | Gate function          | Side effect on fail            |
|-----------------------------|------------------------|---------------------------------|
| Workflow needs fast yes/no  | `is_carrier_vetted`    | none (read-only)                |
| New carrier write           | `insert_carrier`       | route to Quarantine tab         |
| Bulk re-vet of main tab     | `sweep_carrier_database` | update col AG, log diffs      |
| Quarantine review           | `sweep_quarantine`     | release rows that now pass      |
| Daily 04:00 cron            | `run_vetting_sweep --all` | both sweeps                  |

## Sheet structure

**Carrier Database** (main tab, 33 columns A–AG)
- A–AE: existing carrier fields (CARRIER_MASTER_COLUMNS)
- AF: Classification — Derek's taxonomy, **never touched by the pipeline**
- AG: Vetting Status — written by sweep / writer

**Carrier Quarantine** (37 columns A–AK)
- A–AG: same shape as Carrier Database
- AH: Quarantine Reason  (status code + human-readable reason)
- AI: Quarantined At     (ISO timestamp)
- AJ: Original Row Number (where in main tab the row lived)
- AK: Last Re-checked    (ISO timestamp of last sweep_quarantine attempt)

The quarantine tab is created on demand by
`app.vetting.quarantine.ensure_quarantine_tab_exists` — the function is
idempotent so any code path can call it without checking first.

## Quarantine and release flow

1. **Quarantine** — A failing row is written to the Quarantine tab with the
   original row number stamped in col AJ. Idempotent on DOT Number — appending
   the same DOT twice updates the existing row instead of duplicating.
2. **Release** — `sweep_quarantine()` runs `vet_complete()` on every
   quarantine row. If a row now passes, it is appended back to Carrier Database
   (A–AG only — the AH–AK metadata is dropped) and deleted from Quarantine.
3. **Manual data fix** — To rescue a row before the next sweep, edit the
   Quarantine row in the sheet (e.g., correct the Insurance Liability cell)
   and run `PYTHONPATH=. python scripts/run_vetting_sweep.py --quarantine`.

## How rules are changed

1. Edit `app/vetting/rules.py` — bump or relax a threshold.
2. Run `pytest tests/test_vetting_module.py` to make sure the unit tests
   still encode your intent (some tests check `RULES` directly).
3. Run a full re-vet:
   ```bash
   PYTHONPATH=. python scripts/run_vetting_sweep.py --all
   ```
4. If the rule change should immediately reflect upstream FMCSA data:
   ```bash
   PYTHONPATH=. python scripts/run_vetting_sweep.py --main --refetch
   ```
   ⚠️ This re-hits FMCSA at 1 req/sec — ~2 min per 128 rows. Don't do it more
   than once per rule change.

## Daily sweep scheduled task

| Field | Value |
|---|---|
| Task name | `BrokerOps-Vetting-Daily-Sweep` |
| Schedule | Daily, 04:00 local time |
| Run as | `DDEBOER\Owner` (current user) |
| Logon mode | Interactive only |
| Action | `C:\Users\Owner\brokerops-ai\scripts\run_vetting_sweep.bat` |
| Wrapper log | `scripts\logs\vetting_sweep.log` |
| Python log | `scripts\logs\vetting_sweep_python.log` |

The wrapper batch file (`run_vetting_sweep.bat`) calls
`run_vetting_sweep.py --all` which runs both `sweep_carrier_database()` and
`sweep_quarantine()`. The default flags do **not** re-fetch FMCSA — that's a
separate, explicit `--refetch` invocation.

### Operations

```bat
schtasks /change /tn BrokerOps-Vetting-Daily-Sweep /disable    REM pause
schtasks /change /tn BrokerOps-Vetting-Daily-Sweep /enable     REM resume
schtasks /run /tn BrokerOps-Vetting-Daily-Sweep                REM fire once
schtasks /query /tn BrokerOps-Vetting-Daily-Sweep /fo LIST /v  REM check status
schtasks /delete /tn BrokerOps-Vetting-Daily-Sweep /f          REM remove
```

Tail logs:

```bash
tail -f /c/Users/Owner/brokerops-ai/scripts/logs/vetting_sweep.log
tail -f /c/Users/Owner/brokerops-ai/scripts/logs/vetting_sweep_python.log
```

## Tests

| Test file                          | Coverage                                |
|------------------------------------|-----------------------------------------|
| `tests/test_vetting_gates.py`      | Legacy gate regression suite            |
| `tests/test_vetting_module.py`     | New `app/vetting/*` module unit tests   |
| `tests/test_vetting_writer_coverage.py` | Lint-style: every direct Carrier Database write must go through `insert_carrier` (which gates) |

Run them all:
```bash
PYTHONPATH=. python -m pytest tests/test_vetting_module.py tests/test_vetting_gates.py tests/test_vetting_writer_coverage.py -v
```

## Things that are deliberately NOT in this pipeline

- **Apollo enrichment / Playwright scrape** — those are read-side, run after
  vetting passes. They cannot insert rows directly.
- **Outreach send** — requires `is_carrier_vetted == True`, gated in the
  outreach workflow, but does not write to the Carrier Database.
- **Classification (col AF)** — Derek's manual taxonomy. The pipeline reads
  it through but never writes it.
- **MDL Vendor Outreach sheet** — completely separate spreadsheet
  (`1zRh5bIjPK2R0CSG7kVich4tGYdEIC6FGIBgdI-voxLw`). Do not cross-wire.

## Change log

- **2026-04-14** — Initial rebuild. Created `app/vetting/` module, Carrier
  Quarantine tab, daily sweep task. Migrated 128 existing rows: after fresh
  FMCSA refetch, all 128 fail vetting (78 fleet, 37 liability, 13
  needs_review) — main tab is empty until new vetted carriers are added.
