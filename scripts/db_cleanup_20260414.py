"""
DB cleanup pass — 2026-04-14

1. Patches 5 spot-check findings (rows 13, 37, 38, 39, 46)
2. Adds 'Classification' column (AF) and runs heuristic classification
3. Outputs a summary report

Idempotent: running twice will not double-patch notes (checks for CORRECTION marker).
"""
import sys, io, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from app.google_auth import get_sheets_service
from app.config import get_settings

SHEET = get_settings().CARRIER_MASTER_SHEET_ID
TAB = 'Carrier Database'
CORRECTION_MARK = '[CORRECTION 2026-04-14]'

svc = get_sheets_service()

# pull everything
resp = svc.spreadsheets().values().get(
    spreadsheetId=SHEET,
    range=f'{TAB}!A1:AE200',
).execute()
rows = resp.get('values', [])
header = rows[0]
data = rows[1:]
print(f'Loaded {len(data)} data rows, {len(header)} columns')

# ─── Spot-check patches ─────────────────────────────────────────────────────

SPOT_PATCHES = {
    # row_number (1-indexed incl header): (company_fragment, correction_note)
    13: ('COLONIAL FUEL', 'Possible private-fleet classification flagged by audit 2026-04-13. '
                           'Cargo list is broad (11 categories) which is unusual for a pure private fleet. '
                           'Needs human verification before outreach. DO NOT send carrier outreach until reviewed.'),
    37: ('CIRCUIT', 'CHAMELEON CARRIER SIGNATURE: 192 power units + 318 drivers + ZERO inspections in 24-month SMS period. '
                    'Cargo combo "Fresh Produce + Passengers" is internally inconsistent. '
                    'AUTO-REJECT — do not contact, do not quote.'),
    38: ('TRI STATE CARRIERS', 'Legal entity is NEW LINE TRANSPORT LLC dba Tri State Carriers. '
                                'Website "tristatecarrier.com" referenced in prior enrichment is a DIFFERENT company '
                                '(Mason, OH — DOT unrelated). Correct HQ: 1204 NW 137TH AVE MIAMI FL 33182. '
                                'Correct phone: (561) 803-6155. Elevated V.OOS 22.9% and crash 8.6/100u — use with risk mitigation.'),
    39: ('SUNSTATE', 'Website "sunstate.com" referenced in prior enrichment is Sunstate EQUIPMENT (construction rental), NOT this carrier. '
                      'Correct trucking domain is sunstatecarriers.com. V.OOS 27.1% — just under auto-reject threshold (30%), '
                      'use with risk mitigation. HR contact hr2@sunstatecarriers.com found but not for dispatch.'),
    46: ('SHELTON TRUCKING', 'Subsidiary of PS LOGISTICS. Carrier outreach should route to PS Logistics '
                              'carrier-relations, not Shelton directly. V.OOS 27.2% — elevated, use with risk mitigation.'),
}

patches_applied = 0
patches_skipped = 0
for row_num, (fragment, correction) in SPOT_PATCHES.items():
    idx = row_num - 2  # data index (0-based, skipping header)
    if idx >= len(data):
        print(f'  [!] Row {row_num} out of range, skipping')
        continue
    row = data[idx]
    name = row[2] if len(row) > 2 else ''
    if fragment.upper() not in name.upper():
        print(f'  [!] Row {row_num}: expected {fragment}, got {name} — SKIPPING')
        patches_skipped += 1
        continue
    # pad row to 31 cols
    while len(row) < 31:
        row.append('')
    current_notes = row[30]
    if CORRECTION_MARK in current_notes:
        print(f'  [=] Row {row_num}: already patched, skipping')
        patches_skipped += 1
        continue
    new_notes = f'{current_notes} | {CORRECTION_MARK} {correction}'
    row[30] = new_notes
    data[idx] = row
    patches_applied += 1
    print(f'  [+] Row {row_num}: {name[:40]} — patched')

print(f'Spot patches: {patches_applied} applied, {patches_skipped} skipped')
print()

# ─── Classification heuristic ───────────────────────────────────────────────

PRIVATE_FLEET_KEYWORDS = [
    # fuel / lubricant / energy distributors
    'FUEL', 'PETROLEUM', 'LUBRICANT', 'PROPANE', 'LPG',
    # building materials manufacturers / distributors
    'LUMBER CO', 'LUMBER INC', 'BUILDING SUPPLY', 'BUILDING MATERIALS INC',
    'BRICK', 'BLOCK CO', 'CEMENT CO', 'CONCRETE INC', 'CONCRETE CO',
    'STEEL CO', 'STEEL INC', 'GLASS CO', 'WINDOW CO', 'WINDOW INC',
    'DOOR CO', 'ROOFING', 'INSULATION', 'PAVING', 'AGGREGATE',
    # retailers with private fleets
    'WALMART', 'COSTCO', 'HOME DEPOT', 'LOWES', 'KROGER', 'TARGET CORP',
    'PUBLIX', 'SAFEWAY', 'ALBERTSONS',
    # window/door manufacturers
    'JELD-WEN', 'BORAL', 'ANDERSEN', 'PELLA', 'MARVIN',
]
# Words that if ONLY these appear might be fleet-operator names (not triggers)
FOR_HIRE_HINTS = ['LOGISTICS', 'TRANSPORT', 'CARRIER', 'TRUCKING', 'FREIGHT',
                  'EXPRESS', 'HAULING', 'DISTRIBUTION', 'DELIVERY', 'DRAYAGE']

PASSENGER_KEYWORDS = ['BUS CO', 'BUS INC', 'COACH', 'CHARTER', 'TOURS',
                      'LIMO', 'TRANSIT AUTHORITY']

def classify(row):
    name = (row[2] if len(row) > 2 else '').upper()
    notes = (row[30] if len(row) > 30 else '').upper()
    fleet_size_str = row[16] if len(row) > 16 else ''
    try:
        fleet_size = int(fleet_size_str) if fleet_size_str else 0
    except:
        fleet_size = 0

    # Fleet size below the 3-truck minimum → not eligible for outreach or
    # matching regardless of carrier type. Downstream consumers that filter
    # on Classification=for_hire will now correctly skip these rows.
    if 0 < fleet_size < 3:
        return 'fleet_too_small'

    # passenger/bus reject
    for kw in PASSENGER_KEYWORDS:
        if kw in name:
            return 'passenger_review'

    # chameleon signature
    has_zero_insp = 'ZERO INSPECTION' in notes
    if has_zero_insp and fleet_size >= 50:
        return 'chameleon_review'

    # private fleet candidate
    has_pf_kw = any(kw in name for kw in PRIVATE_FLEET_KEYWORDS)
    has_fh_hint = any(kw in name for kw in FOR_HIRE_HINTS)
    if has_pf_kw and not has_fh_hint:
        return 'private_fleet_review'

    # explicit chameleon marker (only the exact spot-check marker for CIRCUIT)
    if 'CHAMELEON CARRIER SIGNATURE' in notes:
        return 'chameleon_review'
    # explicit private-fleet review marker (only the exact spot-check marker for COLONIAL FUEL)
    if 'POSSIBLE PRIVATE-FLEET CLASSIFICATION FLAGGED BY AUDIT' in notes:
        return 'private_fleet_review'

    return 'for_hire'

# ensure header has Classification column at index 31 (column AF)
if len(header) < 32 or header[31] != 'Classification':
    print('Adding Classification column header to AF1')
    # first expand the grid — Carrier Database sheetId is 0
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET,
        body={'requests': [{
            'appendDimension': {
                'sheetId': 0,
                'dimension': 'COLUMNS',
                'length': 1,
            }
        }]}
    ).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET,
        range=f'{TAB}!AF1',
        valueInputOption='USER_ENTERED',
        body={'range': f'{TAB}!AF1', 'values': [['Classification']]},
    ).execute()
    header.append('Classification')

# classify every row
classification_counts = {}
classification_values = []
for idx, row in enumerate(data):
    cls = classify(row)
    classification_values.append([cls])
    classification_counts[cls] = classification_counts.get(cls, 0) + 1

# ─── Write all changes in a batch ────────────────────────────────────────────

batch_updates = []

# patched notes (column AE = index 30)
for row_num in SPOT_PATCHES:
    idx = row_num - 2
    if idx < len(data):
        batch_updates.append({
            'range': f'{TAB}!AE{row_num}',
            'values': [[data[idx][30]]]
        })

# classification column (AF)
last_row = len(data) + 1
batch_updates.append({
    'range': f'{TAB}!AF2:AF{last_row}',
    'values': classification_values
})

print(f'Submitting {len(batch_updates)} batch updates...')
svc.spreadsheets().values().batchUpdate(
    spreadsheetId=SHEET,
    body={'valueInputOption': 'USER_ENTERED', 'data': batch_updates},
).execute()

print()
print('=== CLASSIFICATION SUMMARY ===')
for cls, n in sorted(classification_counts.items(), key=lambda x: -x[1]):
    print(f'  {cls}: {n}')
print(f'  TOTAL: {sum(classification_counts.values())}')
print()
print('=== SPOT PATCHES ===')
print(f'  Applied: {patches_applied}')
print(f'  Skipped (already patched or mismatch): {patches_skipped}')
print()
print('DONE.')
