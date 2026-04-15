"""
Phase 2 backfill: pull Phone, Address, and ZIP from the FMCSA bulk Carrier CSV
for every row in the main Carrier Database tab that's missing them.

Phase 1 (scripts/backfill_new_carriers_20260414.py) hit QCMobile REST for
City + State but discovered that QCMobile's carrier-detail endpoint does NOT
return phone at all. The bulk Carrier CSV (li_carrier_20260414.csv) has
BUS_TELNO (phone), BUS_STREET_PO (address), BUS_ZIP_CODE. We use it for what
QCMobile can't provide.

Idempotent: never overwrites non-blank cells. Strips the PHONE_ONLY sentinel
in col G once a real phone lands in col H.
"""
import sys, io, csv
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from pathlib import Path

from app.google_auth import get_sheets_service
from app.config import get_settings

SHEET = get_settings().CARRIER_MASTER_SHEET_ID
TAB = 'Carrier Database'
CSV_PATH = Path('C:/Users/Owner/brokerops-ai/data/fmcsa_li/li_carrier_20260414.csv')

svc = get_sheets_service()

# Column indices in main tab (0-based)
COL_DOT = 4
COL_CONTACT_EMAIL = 6
COL_CONTACT_PHONE = 7
COL_ADDRESS = 11
COL_CITY = 12
COL_STATE = 13
COL_ZIP = 14


def is_blank(v):
    return not v or not str(v).strip()


def fmt_phone(raw):
    """Format a 10-digit phone as (XXX) XXX-XXXX."""
    digits = ''.join(c for c in str(raw or '') if c.isdigit())
    if len(digits) == 10:
        return f'({digits[:3]}) {digits[3:6]}-{digits[6:]}'
    if len(digits) == 11 and digits[0] == '1':
        return f'({digits[1:4]}) {digits[4:7]}-{digits[7:]}'
    return raw  # unformatted fallback


# Read main tab
resp = svc.spreadsheets().values().get(
    spreadsheetId=SHEET,
    range=f'{TAB}!A1:AG',
).execute()
rows = resp.get('values', [])
data = rows[1:]
print(f'Loaded {len(data)} rows from {TAB}')

# Identify DOTs that need backfill
needed = {}  # dot (int, unpadded) -> row_num
for idx, row in enumerate(data, start=2):
    dot = row[COL_DOT] if len(row) > COL_DOT else ''
    if not dot or not str(dot).strip():
        continue
    while len(row) < 32:
        row.append('')
    needs = (is_blank(row[COL_CONTACT_PHONE])
             or is_blank(row[COL_ADDRESS])
             or is_blank(row[COL_ZIP]))
    if needs:
        try:
            d = str(int(str(dot).strip()))
        except ValueError:
            continue
        needed[d] = (idx, row)

print(f'Rows needing phone/address/zip backfill: {len(needed)}')

if not needed:
    print('Nothing to do.')
    sys.exit(0)

# Stream the CSV, matching DOTs (CSV has DOTs zero-padded to 8 digits)
print(f'Scanning {CSV_PATH.name} (318 MB, 1.85M rows)...')
matches = {}  # dot -> dict of {Phone, Address, Zip}
with open(CSV_PATH, 'r', encoding='utf-8', errors='replace', newline='') as f:
    reader = csv.DictReader(f)
    seen = 0
    for row in reader:
        seen += 1
        if seen % 500_000 == 0:
            print(f'  scanned {seen:,} rows, matched {len(matches)}')
        csv_dot_raw = (row.get('DOT_NUMBER') or '').strip()
        if not csv_dot_raw:
            continue
        try:
            csv_dot = str(int(csv_dot_raw))  # strip leading zeros
        except ValueError:
            continue
        if csv_dot not in needed:
            continue
        # Found a match — grab whichever has data (one DOT may have multiple
        # docket rows; prefer the one with actual phone/address)
        existing = matches.get(csv_dot, {})
        phone = (row.get('BUS_TELNO') or '').strip()
        street = (row.get('BUS_STREET_PO') or '').strip()
        zipc = (row.get('BUS_ZIP_CODE') or '').strip()
        # Prefer this record if it has more data than existing
        new_data = {
            'Phone': phone or existing.get('Phone', ''),
            'Address': street or existing.get('Address', ''),
            'Zip': zipc or existing.get('Zip', ''),
        }
        matches[csv_dot] = new_data

print(f'Scan complete: matched {len(matches)} of {len(needed)} needed DOTs')

# Build updates
batch_data = []
stats = {'phone': 0, 'address': 0, 'zip': 0, 'sentinel_cleared': 0}

for dot, (row_num, row) in needed.items():
    m = matches.get(dot)
    if not m:
        continue
    # Phone
    if is_blank(row[COL_CONTACT_PHONE]) and m.get('Phone'):
        formatted = fmt_phone(m['Phone'])
        batch_data.append({'range': f'{TAB}!H{row_num}', 'values': [[formatted]]})
        stats['phone'] += 1
        # Clear PHONE_ONLY sentinel in email col if we just got a real phone
        current_email = row[COL_CONTACT_EMAIL] if len(row) > COL_CONTACT_EMAIL else ''
        if str(current_email).strip().upper() == 'PHONE_ONLY':
            batch_data.append({'range': f'{TAB}!G{row_num}', 'values': [['']]})
            stats['sentinel_cleared'] += 1
    # Address
    if is_blank(row[COL_ADDRESS]) and m.get('Address'):
        batch_data.append({'range': f'{TAB}!L{row_num}', 'values': [[m['Address']]]})
        stats['address'] += 1
    # ZIP
    if is_blank(row[COL_ZIP]) and m.get('Zip'):
        batch_data.append({'range': f'{TAB}!O{row_num}', 'values': [[m['Zip']]]})
        stats['zip'] += 1

print()
print(f'Cell updates prepared: {len(batch_data)}')
print(f'  Phone:             {stats["phone"]}')
print(f'  Address:           {stats["address"]}')
print(f'  ZIP:               {stats["zip"]}')
print(f'  Sentinels cleared: {stats["sentinel_cleared"]}')

if batch_data:
    CHUNK = 500
    for i in range(0, len(batch_data), CHUNK):
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET,
            body={
                'valueInputOption': 'USER_ENTERED',
                'data': batch_data[i:i+CHUNK],
            },
        ).execute()
    print(f'Written to sheet.')
