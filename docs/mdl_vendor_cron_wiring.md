# MDL Vendor Outreach — Scheduled Task Wiring (MVP)

Operational notes for the MDL Vendor Outreach dispatcher + reply sweep
running on Derek's Windows workstation via Windows Task Scheduler.

**Status:** Live as of 2026-04-14
**Platform:** Windows 11 Pro Education, Derek's workstation (`DDEBOER`)
**Owner:** Derek deBoer (`DDEBOER\Owner`)

---

## Task overview

| Field | Value |
|---|---|
| Task name | `BrokerOps-MDL-Vendor-Dispatcher` |
| Cadence | Every 5 minutes, indefinitely |
| Run as | `DDEBOER\Owner` (current user) |
| Logon mode | Interactive only (user session must be active) |
| Runs when user is logged off | **No** |
| Action | `powershell.exe -NoProfile -WindowStyle Hidden -Command "& 'C:\Users\Owner\brokerops-ai\scripts\run_mdl_vendor_loop.bat'"` |
| Runner script | `C:\Users\Owner\brokerops-ai\scripts\run_mdl_vendor_loop.bat` |
| Log file | `C:\Users\Owner\brokerops-ai\scripts\logs\mdl_vendor_loop.log` (append mode) |

Each tick runs two single-pass CLIs:

1. `scripts/dispatch_mdl_vendor_outreach.py --once` — scans the sheet, sends any row with col K checked and col H empty.
2. `scripts/process_mdl_vendor_replies.py --once` — sweeps inbound Gmail for vendor replies on tracked threads and stamps col I.

The batch file exits with the dispatcher's return code; reply sweep failures are logged but do not fail the task.

## Python

Uses the hardcoded `C:\Python314\python.exe` from the runner, so the task does
not depend on `PATH` in the scheduled context. `PYTHONPATH=.` is set inside
the batch file so `app.*` imports resolve against the project root.

## Log format

Every tick appends:

```
[YYYY-MM-DDTHH:MM:SS] ===== MDL vendor loop tick =====
[ts] dispatcher --once
<dispatcher stdout+stderr>
[ts] dispatcher exit=<rc>
[ts] reply sweep --once
<reply sweep stdout+stderr>
[ts] reply sweep exit=<rc>
[ts] ===== tick complete (dispatch=<rc> reply=<rc>) =====
```

Tail the log at any time:

```bash
tail -f /c/Users/Owner/brokerops-ai/scripts/logs/mdl_vendor_loop.log
```

There is no log rotation yet — if the file gets large, truncate manually or
wire up `logrotate`-equivalent in a follow-up task.

## Operations

All commands below run from any shell on Derek's machine.

### Pause

```bat
schtasks /change /tn "BrokerOps-MDL-Vendor-Dispatcher" /disable
```

### Resume

```bat
schtasks /change /tn "BrokerOps-MDL-Vendor-Dispatcher" /enable
```

### Fire once manually (for testing)

```bat
schtasks /run /tn "BrokerOps-MDL-Vendor-Dispatcher"
```

Wait ~15 seconds, then check the log file.

### Check status / next run time / last result

```bat
schtasks /query /tn "BrokerOps-MDL-Vendor-Dispatcher" /fo LIST /v
```

Key fields: `Status`, `Last Run Time`, `Last Result` (want `0`), `Next Run Time`.

### Delete the task

```bat
schtasks /delete /tn "BrokerOps-MDL-Vendor-Dispatcher" /f
```

### Modify the schedule (e.g. bump to every 2 minutes)

```bat
schtasks /change /tn "BrokerOps-MDL-Vendor-Dispatcher" /ri 2
```

## Known limitations (MVP)

1. **Requires Derek's workstation to be powered on** with the user session active. If the machine is locked, the task still fires. If it is suspended/hibernating, the task is skipped until wake.
2. **Does not run while Derek is logged out** — logon mode is `Interactive only`. Switching to S4U or password-stored mode would require elevation and is deferred.
3. **No log rotation** — `mdl_vendor_loop.log` grows unbounded. Plan to truncate during migration to Cloud Run.
4. **No retry on transient failures** — if Gmail API has a hiccup, the tick fails and the next tick 5 minutes later retries naturally. Good enough for MVP.
5. **Reply sweep failure does not fail the task** — the dispatcher is the critical path. Reply sweep errors show up in the log and in the sweep's own exit code but `schtasks` only sees the dispatcher's RC.
6. **token.json path is relative to the project working dir** — the `.bat` wrapper cd's into `C:\Users\Owner\brokerops-ai` before invoking Python, so this works. Do not move `token.json`.

## Migration path to Cloud Run

This Task Scheduler wiring is the MVP, explicitly chosen so Nina's first-touch
outreach can go live without waiting on GCP plumbing. Production wiring is a
separate deferred task:

- Containerize dispatcher + reply sweep (Dockerfile already exists at project root)
- Deploy to Cloud Run with a Cloud Scheduler trigger (every 5 min)
- Move `token.json` into Secret Manager or use a service account with
  domain-wide delegation for `sales@deboerfreight.com`
- Wire Cloud Logging as the sink (replacing `mdl_vendor_loop.log`)
- Decommission this scheduled task with `schtasks /delete` once Cloud Run is
  green for 48 hours

When that task is picked up, delete this doc or fold it into the Cloud Run
runbook as "prior art."

## Change log

- **2026-04-14** — Task created, verified end-to-end with 0 sends (sheet has 499 rows, 498 unchecked, 0 ready). Initial wiring.
