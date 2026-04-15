"""
Vetting sweep — 2026-04-14  [DEPRECATED 2026-04-14 PM]

⚠️  DO NOT RUN THIS SCRIPT.

This is the FIRST-generation vetting sweep, written before the vetting
rebuild. It has an inline `vet()` function with HARDCODED thresholds
($1M liability, $100K cargo, fleet<3) that DO NOT match the canonical
source of truth in app/vetting/rules.py::RULES.

The canonical sweep is now:

    scripts/run_vetting_sweep.py

which calls app.vetting.sweep.sweep_carrier_database — that path reads
RULES directly, applies the full gate (including the new rate-based
reefer rules shipped 2026-04-15), and is the only supported way to
re-sweep the Carrier Database going forward.

This file is kept only for historical reference / audit trail of the
one-shot sweep run on 2026-04-14. It should never be re-executed.

Original docstring follows:
──────────────────────────────────────────────────────────────────
Vetting sweep — 2026-04-14

Applies hard-reject rules to every for_hire carrier in the DB and writes the
result to a new column AG "Vetting Status".

Rules checked from sheet data (what we can verify without re-fetching FMCSA):
  - Fleet size < 3 power units                       → fail_fleet_size
  - Safety rating = "Unsatisfactory"                 → fail_safety_rating
  - Insurance Liability < $1,000,000                 → fail_insurance_liability
  - Insurance Cargo < $100,000                       → fail_insurance_cargo

Rules NOT checked here (would need FMCSA re-fetch):
  - V OOS > 30% / D OOS > 15% / Crash > 30/100u (some in Notes, inconsistent)
  - Shell carrier (0 drivers with >0 units) — driver count not in sheet
  - Reefer vehicle maintenance OOS (need per-inspection data)

These are future work — flagged in Notes as "partial vet".

Idempotent: running twice produces same result, writes only if current cell differs.
"""
raise RuntimeError(
    "scripts/vetting_sweep_20260414.py is DEPRECATED — use "
    "scripts/run_vetting_sweep.py (app.vetting.sweep) instead. "
    "See module docstring for details."
)
import sys, io, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from app.google_auth import get_sheets_service
from app.config import get_settings

SHEET = get_settings().CARRIER_MASTER_SHEET_ID
TAB = 'Carrier Database'
svc = get_sheets_service()

# Pull all rows with AG included in case it already exists
resp = svc.spreadsheets().values().get(
    spreadsheetId=SHEET,
    range=f'{TAB}!A1:AG200',
).execute()
rows = resp.get('values', [])
header = rows[0]
data = rows[1:]
print(f'Loaded {len(data)} data rows, {len(header)} columns')

# Ensure Vetting Status column exists at AG (index 32)
if len(header) < 33 or (len(header) >= 33 and header[32] != 'Vetting Status'):
    print('Adding Vetting Status column at AG1')
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET,
        body={'requests': [{
            'appendDimension': {'sheetId': 0, 'dimension': 'COLUMNS', 'length': 1}
        }]}
    ).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET,
        range=f'{TAB}!AG1',
        valueInputOption='USER_ENTERED',
        body={'range': f'{TAB}!AG1', 'values': [['Vetting Status']]},
    ).execute()


def parse_money(raw):
    if not raw:
        return 0
    s = re.sub(r'[^\d.]', '', str(raw))
    try:
        return float(s) if s else 0
    except ValueError:
        return 0


def parse_int(raw):
    if not raw:
        return 0
    s = re.sub(r'[^\d-]', '', str(raw))
    try:
        return int(s) if s else 0
    except ValueError:
        return 0


def vet(row):
    """Return (status, reason) tuple."""
    cls = row[31] if len(row) > 31 else ''
    if cls != 'for_hire':
        return ('skip', f'classification={cls or "blank"}')

    fleet = parse_int(row[16] if len(row) > 16 else '')
    if fleet > 0 and fleet < 3:
        return ('fail_fleet_size', f'{fleet} power units < 3 min')
    if fleet == 0:
        return ('needs_review', 'fleet size unknown')

    safety = (row[22] if len(row) > 22 else '').strip().lower()
    if safety == 'unsatisfactory':
        return ('fail_safety_rating', 'unsatisfactory safety rating')

    liab = parse_money(row[17] if len(row) > 17 else '')
    if liab > 0 and liab < 1_000_000:
        return ('fail_insurance_liability', f'liability ${liab:,.0f} < $1M min')

    cargo = parse_money(row[18] if len(row) > 18 else '')
    if cargo > 0 and cargo < 100_000:
        return ('fail_insurance_cargo', f'cargo ${cargo:,.0f} < $100K min')

    return ('pass_basic', 'fleet>=3, safety ok, insurance ok where recorded')


counts = {}
status_values = []
for row in data:
    status, reason = vet(row)
    counts[status] = counts.get(status, 0) + 1
    status_values.append([status])

last_row = len(data) + 1
svc.spreadsheets().values().update(
    spreadsheetId=SHEET,
    range=f'{TAB}!AG2:AG{last_row}',
    valueInputOption='USER_ENTERED',
    body={'range': f'{TAB}!AG2:AG{last_row}', 'values': status_values},
).execute()

print()
print('=== VETTING SWEEP SUMMARY ===')
for k, v in sorted(counts.items(), key=lambda x: -x[1]):
    print(f'  {k}: {v}')
print(f'  TOTAL: {sum(counts.values())}')
print()

# Show carriers that fail
fails = [(i+2, row, vet(row)) for i, row in enumerate(data)
         if vet(row)[0] not in ('pass_basic', 'skip', 'needs_review')]
print(f'=== {len(fails)} CARRIERS FAILING VETTING ===')
for row_num, row, (status, reason) in fails[:30]:
    name = row[2] if len(row) > 2 else ''
    dot = row[4] if len(row) > 4 else ''
    fleet = row[16] if len(row) > 16 else ''
    print(f'  Row {row_num:3} [{status:26}] {name[:40]:<42} DOT={dot:<8} fleet={fleet}')
if len(fails) > 30:
    print(f'  ... and {len(fails) - 30} more')
