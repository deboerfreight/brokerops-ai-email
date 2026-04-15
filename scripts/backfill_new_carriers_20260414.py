"""
Targeted backfill for the ~119 new carriers added by today's L&I sourcing run.

Problem: prospect_carriers.py::enrich_and_store only passed a subset of
hydrated FMCSA fields to the sheet. City/State/ZIP/Phone/Address columns
were left blank on all 119 new inserts. The agent noted this as "cosmetic"
but they're actually unreachable without a phone.

This script:
  1. Reads all main-tab rows
  2. Identifies rows where City OR State OR Phone is blank (our signal for
     "needs backfill")
  3. For each, calls get_carrier_details(dot) (QCMobile, now correctly
     parsing insurance × 1000) and extracts Phone, Address, City, State, ZIP
  4. Writes back to the sheet via a single batchUpdate, skipping any cell
     that's already populated (never overwrites Derek's/other real data)
  5. Also replaces "PHONE_ONLY" sentinel in col G with blank where the phone
     column now has a real value (so the email column isn't lying)
  6. Rate limit: 1 req/sec to FMCSA
"""
import sys, io, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from app.google_auth import get_sheets_service
from app.config import get_settings
from app.fmcsa import get_carrier_details

SHEET = get_settings().CARRIER_MASTER_SHEET_ID
TAB = 'Carrier Database'
svc = get_sheets_service()

resp = svc.spreadsheets().values().get(
    spreadsheetId=SHEET,
    range=f'{TAB}!A1:AG',
).execute()
rows = resp.get('values', [])
header = rows[0]
data = rows[1:]
print(f'Loaded {len(data)} rows from {TAB}')

# Column indices (0-based)
COL_DOT = 4
COL_CONTACT_EMAIL = 6
COL_CONTACT_PHONE = 7
COL_ADDRESS = 11
COL_CITY = 12
COL_STATE = 13
COL_ZIP = 14


def is_blank(v):
    return not v or not str(v).strip()


def needs_backfill(row):
    """A row needs backfill if City OR State OR Phone is blank."""
    city = row[COL_CITY] if len(row) > COL_CITY else ''
    state = row[COL_STATE] if len(row) > COL_STATE else ''
    phone = row[COL_CONTACT_PHONE] if len(row) > COL_CONTACT_PHONE else ''
    return is_blank(city) or is_blank(state) or is_blank(phone)


# Identify targets
targets = []
for idx, row in enumerate(data, start=2):  # row 1 is header
    dot = row[COL_DOT] if len(row) > COL_DOT else ''
    if not dot or not str(dot).strip():
        continue
    if needs_backfill(row):
        targets.append((idx, str(dot).strip(), row))

print(f'Rows needing backfill: {len(targets)}')
if not targets:
    print('Nothing to do.')
    sys.exit(0)

# Fetch FMCSA details for each, rate limited
updates = {}  # row_num → dict of {col_letter: value}
errors = []
start = time.time()

for i, (row_num, dot, row) in enumerate(targets, 1):
    if i > 1:
        time.sleep(1.1)  # FMCSA rate limit
    try:
        details = get_carrier_details(dot)
    except Exception as exc:
        errors.append((row_num, dot, f'fetch error: {exc}'))
        continue
    if not details:
        errors.append((row_num, dot, 'no details returned'))
        continue

    # Pad row so indexing doesn't IndexError
    while len(row) < 32:
        row.append('')

    row_updates = {}

    # Phone
    if is_blank(row[COL_CONTACT_PHONE]):
        phone = details.get('Phone') or details.get('Contact_Phone') or ''
        if phone:
            row_updates['H'] = phone

    # Address
    if is_blank(row[COL_ADDRESS]):
        addr = details.get('Phy_Street') or details.get('Address') or ''
        if addr:
            row_updates['L'] = addr

    # City
    if is_blank(row[COL_CITY]):
        city = details.get('Phy_City') or details.get('City') or ''
        if city:
            row_updates['M'] = city

    # State
    if is_blank(row[COL_STATE]):
        state = details.get('Phy_State') or details.get('State') or ''
        if state:
            row_updates['N'] = state

    # ZIP
    if is_blank(row[COL_ZIP]):
        zipc = details.get('Phy_Zip') or details.get('ZIP') or ''
        if zipc:
            row_updates['O'] = zipc

    # Clean up PHONE_ONLY sentinel in email col IF we just got a phone
    current_email = row[COL_CONTACT_EMAIL] if len(row) > COL_CONTACT_EMAIL else ''
    if str(current_email).strip().upper() == 'PHONE_ONLY' and row_updates.get('H'):
        row_updates['G'] = ''

    if row_updates:
        updates[row_num] = row_updates

    if i % 20 == 0:
        elapsed = time.time() - start
        print(f'  [{i}/{len(targets)}] elapsed {elapsed:.0f}s')

print()
print(f'Prepared updates for {len(updates)} rows')
print(f'Errors: {len(errors)}')

# Build batchUpdate requests
batch_data = []
for row_num, row_updates in sorted(updates.items()):
    for col, val in row_updates.items():
        batch_data.append({
            'range': f'{TAB}!{col}{row_num}',
            'values': [[val]],
        })

if batch_data:
    # Chunk into 500 at a time to be safe
    CHUNK = 500
    for i in range(0, len(batch_data), CHUNK):
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET,
            body={
                'valueInputOption': 'USER_ENTERED',
                'data': batch_data[i:i+CHUNK],
            },
        ).execute()
    print(f'Batched {len(batch_data)} cell updates written')

print()
print('=== ERROR DETAIL ===')
for row_num, dot, err in errors[:10]:
    print(f'  row {row_num} DOT {dot}: {err}')
if len(errors) > 10:
    print(f'  ...and {len(errors) - 10} more')
