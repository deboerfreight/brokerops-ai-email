"""
Inline cleanup — quarantine 4 confirmed-bad carriers identified in the
2026-04-14 main-tab audit.

Targets (by DOT + rejection reason):
  1537209 TRI STATE CARRIERS → NEW LINE TRANSPORT LLC
          -> chameleon (broker, not carrier)
  4503715 PROCO COMPANY → PROCO TIRE RECYCLERS LLC
          -> private fleet (tire recycler)
  3588046 COPTIC TRANSPORTATION → AMIR'S CAKES INC
          -> wrong entity (bakery)
  1898767 TRUE HAULING → WRIGHT CHOICE AUTO SALES LLC
          -> wrong entity (auto dealership)

Idempotent: safe to re-run. Skips DOTs already in quarantine.
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from datetime import datetime, timezone

from app.google_auth import get_sheets_service
from app.config import get_settings
from app.vetting.gate import VettingResult
from app.vetting.quarantine import append_to_quarantine, get_quarantine_rows

SHEET = get_settings().CARRIER_MASTER_SHEET_ID
MAIN_TAB = 'Carrier Database'

TARGETS = {
    '1537209': {
        'status': 'fail_chameleon_broker',
        'reason': 'FMCSA legal name is NEW LINE TRANSPORT LLC (BROKER_STAT=A, COMMON_STAT=N). '
                  'Reauthorized as a broker; no longer operates as a carrier. '
                  'Confirmed via L&I bulk data 2026-04-14.',
    },
    '4503715': {
        'status': 'fail_private_fleet_confirmed',
        'reason': 'FMCSA legal name is PROCO TIRE RECYCLERS LLC. Private fleet for tire '
                  'recycling operation — will not accept third-party freight. '
                  'Name mismatch: sheet said "PROCO COMPANY".',
    },
    '3588046': {
        'status': 'fail_wrong_entity',
        'reason': 'FMCSA legal name is AMIR\'S CAKES INC (bakery). Sheet listed it as '
                  '"COPTIC TRANSPORTATION" — name pairing is wrong. Bakery private fleet, '
                  'not a for-hire carrier.',
    },
    '1898767': {
        'status': 'fail_wrong_entity',
        'reason': 'FMCSA legal name is WRIGHT CHOICE AUTO SALES LLC (auto dealership). '
                  'Sheet listed it as "TRUE HAULING" — name pairing is wrong. '
                  'Auto sales operation, not a for-hire freight carrier.',
    },
}

svc = get_sheets_service()

# Read current main tab
resp = svc.spreadsheets().values().get(
    spreadsheetId=SHEET,
    range=f'{MAIN_TAB}!A1:AG',
).execute()
rows = resp.get('values', [])
header = rows[0] if rows else []
data = rows[1:]
print(f'Loaded {len(data)} rows from {MAIN_TAB}')

# Get existing quarantine DOTs for idempotency
quarantine_rows = get_quarantine_rows(svc, SHEET)
existing_q_dots = set()
for qr in quarantine_rows:
    dot_val = qr.get('DOT Number') or qr.get('DOT_Number') or ''
    if dot_val:
        existing_q_dots.add(str(dot_val).strip())
print(f'Quarantine tab already has {len(existing_q_dots)} DOTs')

# Find target rows in main tab
rows_to_delete = []  # list of (1-indexed row number, dot) tuples
now_iso = datetime.now(timezone.utc).isoformat()

for idx, row in enumerate(data, start=2):  # start=2 because row 1 is header
    if len(row) < 5:
        continue
    dot = (row[4] or '').strip() if len(row) > 4 else ''
    if dot not in TARGETS:
        continue
    target = TARGETS[dot]
    row_dict = {header[i] if i < len(header) else f'col{i}': row[i] if i < len(row) else ''
                for i in range(max(len(header), len(row)))}
    # build VettingResult
    result = VettingResult(
        passed=False,
        status=target['status'],
        reason=target['reason'],
        checked_at=now_iso,
    )
    if dot in existing_q_dots:
        print(f'  [=] DOT {dot} already in quarantine — will still delete from main tab')
    else:
        try:
            append_to_quarantine(svc, SHEET, row_dict, result)
            print(f'  [+] DOT {dot} ({row[2] if len(row)>2 else ""}) appended to quarantine')
        except Exception as exc:
            print(f'  [!] Failed to quarantine DOT {dot}: {exc}')
            continue
    rows_to_delete.append((idx, dot, row[2] if len(row) > 2 else ''))

print()
print(f'Target rows identified: {len(rows_to_delete)}')

# Find the sheet ID for Carrier Database tab
meta = svc.spreadsheets().get(spreadsheetId=SHEET).execute()
main_sheet_id = None
for s in meta.get('sheets', []):
    if s['properties']['title'] == MAIN_TAB:
        main_sheet_id = s['properties']['sheetId']
        break
if main_sheet_id is None:
    print(f'ERROR: Could not find sheetId for tab {MAIN_TAB}')
    sys.exit(1)

# Delete in REVERSE order to avoid row shift during batch
rows_to_delete.sort(key=lambda t: -t[0])
delete_requests = []
for row_num, dot, name in rows_to_delete:
    delete_requests.append({
        'deleteDimension': {
            'range': {
                'sheetId': main_sheet_id,
                'dimension': 'ROWS',
                'startIndex': row_num - 1,  # 0-indexed
                'endIndex': row_num,
            }
        }
    })

if delete_requests:
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET,
        body={'requests': delete_requests},
    ).execute()
    print(f'Deleted {len(delete_requests)} rows from main tab')
    for row_num, dot, name in rows_to_delete:
        print(f'  - row {row_num}: DOT {dot} {name}')

# Final counts
resp = svc.spreadsheets().values().get(spreadsheetId=SHEET, range=f'{MAIN_TAB}!A:A').execute()
new_main = len(resp.get('values', [])) - 1
resp = svc.spreadsheets().values().get(spreadsheetId=SHEET, range='Carrier Quarantine!A:A').execute()
new_q = len(resp.get('values', [])) - 1
print()
print(f'Final main tab: {new_main}')
print(f'Final quarantine: {new_q}')
