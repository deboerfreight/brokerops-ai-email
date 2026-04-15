"""
Normalize case on the main Carrier Database tab.

- Smart title-case Company Name (col C), Address (col L), City (col M)
- Lowercase Contact Email (col G) and Dispatcher Email (col J)
- Preserve State (N), ZIP (O), Phone (H), numeric fields, and all metadata columns

Idempotent: running twice is a no-op on already-normalized rows.

Usage:
  --dry-run  preview changes without writing (DEFAULT when run without flags)
  --apply    actually write the batchUpdate
"""
import sys, io, re, argparse
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from app.google_auth import get_sheets_service
from app.config import get_settings

SHEET = get_settings().CARRIER_MASTER_SHEET_ID
TAB = 'Carrier Database'

# Tokens that stay ALL CAPS regardless of position.
# NOTE: state codes that conflict with common English/Spanish words are
# excluded (DE=de, LA=la, IN=in, OR=or, IA, NE, AL, OH, CO, KY=Kentucky vs
# 'ky' short form, MD, MI) — these are ambiguous inside addresses and
# company names. State codes only appear in the State column (col N)
# which this script does not touch.
UPPER_TOKENS = {
    # Entity suffixes
    'LLC','INC','CORP','LTD','LP','LLP','PLC','PLLC',
    # Country/acronyms
    'USA','US','USDOT','DOT','DBA','AKA',
    # Directionals (these are commonly uppercase in USPS address format)
    'N','S','E','W','NW','NE','SW','SE','NNE','NNW','SSE','SSW',
    'ENE','ESE','WNW','WSW',
    # Roman numerals (truly unambiguous ones only — 'I' and 'V' conflict
    # with the English word 'I' and letter 'V', so excluded)
    'II','III','IV','VI','VII','VIII','IX',
    # Street suffixes (USPS common abbreviations)
    'HWY','RTE','RD','ST','AVE','BLVD','DR','LN','PKWY','PL',
    'TRL','CIR','SQ','WAY','TER','PT','XING','LOOP','ALY','BND','CYN',
    'FWY','HBR','JCT','MDWS','MNR','PLZ','RDG','RNCH',
    'SKWY','SPG','TPKE','VLG','VLY','XRD',
    # Industry acronyms (trucking / brokerage)
    'BMC','EDI','GPS','ELD','HOS','CDL','FMCSA','HVAC',
    # Ampersand
    '&',
}
# Words that lowercase when not at start (mid-title articles/prepositions)
LOWER_IF_MID = {'AND','OF','THE','FOR','IN','ON','AT','A','AN','TO','BY','OR','WITH'}


def smart_title(s: str) -> str:
    if not s:
        return s
    s = str(s).strip()
    # SAFETY RULE: only process strings that are fully uppercase (allowing
    # digits, spaces, punctuation). If ANY lowercase letter is present, the
    # input already has a deliberate case and we leave it alone. This
    # prevents regressions like "MCI Express" → "Mci Express" or
    # "Pro Transport Inc" → "Pro Transport INC".
    if any(c.islower() for c in s):
        return s
    # Split preserving whitespace and basic punctuation
    tokens = re.split(r'(\s+|[,.()])', s)
    out = []
    word_position = 0
    for tok in tokens:
        if not tok or tok.isspace() or tok in (',', '.', '(', ')'):
            out.append(tok)
            continue
        upper = tok.upper()
        stripped = re.sub(r'[^A-Z0-9\'&-]', '', upper)

        # All-uppercase preserved tokens
        if stripped in UPPER_TOKENS:
            # Special case: "CO" is also an abbreviation for "Company" — preserve
            # but also handle Colorado / Connecticut context — safe to keep uppercase
            out.append(upper)
            word_position += 1
            continue

        # Mid-word lower: and, of, the, etc. But not at position 0
        if stripped in LOWER_IF_MID and word_position > 0:
            out.append(tok.lower())
            word_position += 1
            continue

        # Apostrophe handling (O'BRIEN, D'ANGELO)
        if "'" in tok:
            parts = tok.split("'", 1)
            if len(parts) == 2 and 1 <= len(parts[0]) <= 2 and parts[0].isalpha():
                out.append(parts[0].capitalize() + "'" + parts[1].capitalize())
                word_position += 1
                continue

        # Hyphen handling (COCA-COLA, MULTI-STATE)
        if "-" in tok:
            out.append('-'.join(smart_word(p) for p in tok.split('-')))
            word_position += 1
            continue

        # Mc/Mac pattern
        if re.match(r'^MC[A-Z][A-Z]+$', upper):
            out.append('Mc' + upper[2:].capitalize())
            word_position += 1
            continue
        if re.match(r'^MAC[A-Z][A-Z]+$', upper):
            out.append('Mac' + upper[3:].capitalize())
            word_position += 1
            continue

        out.append(tok.capitalize())
        word_position += 1

    return ''.join(out)


def smart_word(w: str) -> str:
    """Title-case a single word (no whitespace)."""
    if not w:
        return w
    u = w.upper()
    stripped = re.sub(r'[^A-Z0-9]', '', u)
    if stripped in UPPER_TOKENS:
        return u
    return w.capitalize()


def normalize_email(s: str) -> str:
    """Lowercase an email address."""
    if not s:
        return s
    s = str(s).strip()
    if '@' not in s:
        return s
    return s.lower()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true', help='Write changes to sheet')
    parser.add_argument('--limit', type=int, default=0, help='Preview first N rows only')
    args = parser.parse_args()

    mode = 'APPLY' if args.apply else 'DRY-RUN'
    print(f'=== NORMALIZE CASE — {mode} ===')
    print()

    svc = get_sheets_service()
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET,
        range=f'{TAB}!A1:AG',
    ).execute()
    rows = resp.get('values', [])
    header = rows[0]
    data = rows[1:]
    print(f'Loaded {len(data)} rows')

    COL_NAME = 2       # C
    COL_CONTACT_EMAIL = 6  # G
    COL_DISPATCHER_EMAIL = 9  # J
    COL_ADDRESS = 11   # L
    COL_CITY = 12      # M

    batch_data = []
    changes_preview = []
    for idx, row in enumerate(data, start=2):
        while len(row) < 32:
            row.append('')
        # Name
        name_old = row[COL_NAME]
        name_new = smart_title(name_old)
        if name_new != name_old:
            batch_data.append({'range': f'{TAB}!C{idx}', 'values': [[name_new]]})
            changes_preview.append(('C', idx, 'Name', name_old, name_new))
        # Address
        addr_old = row[COL_ADDRESS]
        addr_new = smart_title(addr_old)
        if addr_new != addr_old:
            batch_data.append({'range': f'{TAB}!L{idx}', 'values': [[addr_new]]})
            changes_preview.append(('L', idx, 'Addr', addr_old, addr_new))
        # City
        city_old = row[COL_CITY]
        city_new = smart_title(city_old)
        if city_new != city_old:
            batch_data.append({'range': f'{TAB}!M{idx}', 'values': [[city_new]]})
            changes_preview.append(('M', idx, 'City', city_old, city_new))
        # Contact Email
        email_old = row[COL_CONTACT_EMAIL]
        email_new = normalize_email(email_old)
        if email_new != email_old:
            batch_data.append({'range': f'{TAB}!G{idx}', 'values': [[email_new]]})
            changes_preview.append(('G', idx, 'Email', email_old, email_new))
        # Dispatcher Email
        demail_old = row[COL_DISPATCHER_EMAIL]
        demail_new = normalize_email(demail_old)
        if demail_new != demail_old:
            batch_data.append({'range': f'{TAB}!J{idx}', 'values': [[demail_new]]})
            changes_preview.append(('J', idx, 'DEmail', demail_old, demail_new))

    print(f'Total cell changes proposed: {len(batch_data)}')
    print()
    # Show sample of changes
    sample_n = args.limit if args.limit else 30
    print(f'Sample of first {min(sample_n, len(changes_preview))} changes:')
    print(f'{"Col":<5} {"Row":<5} {"Field":<7} {"BEFORE":<45} {"AFTER"}')
    print('-' * 115)
    for col, idx, field, old, new in changes_preview[:sample_n]:
        print(f'{col:<5} {idx:<5} {field:<7} {old[:43]:<45} {new[:50]}')

    if args.apply and batch_data:
        print()
        print('APPLYING changes...')
        CHUNK = 500
        for i in range(0, len(batch_data), CHUNK):
            svc.spreadsheets().values().batchUpdate(
                spreadsheetId=SHEET,
                body={
                    'valueInputOption': 'USER_ENTERED',
                    'data': batch_data[i:i+CHUNK],
                },
            ).execute()
        print(f'Wrote {len(batch_data)} cells in {(len(batch_data) + CHUNK - 1) // CHUNK} batch(es)')
    elif batch_data:
        print()
        print('DRY-RUN — no changes written. Re-run with --apply to commit.')


if __name__ == '__main__':
    main()
