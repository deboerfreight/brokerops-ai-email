#!/usr/bin/env python3
"""One-time bootstrap: create the MDL Vendor Outreach workbook.

Creates a standalone spreadsheet with the `Vendors` tab, header row,
data validation on col K (checkbox), protected ranges on G/H/I/J,
red-tinted col F ("Derek's Notes — private scratchpad") and frozen
header row.

Shares with:
  - sales@deboerfreight.com  (writer)
  - derekndeboer@gmail.com   (writer)

Prints the new sheet ID and URL to stdout.
"""
from __future__ import annotations

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.google_auth import get_sheets_service, get_drive_service

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("create_mdl_vendor_sheet")

TITLE = "MDL Vendor Outreach"
TAB_NAME = "Vendors"

HEADERS = [
    "Vendor Company",            # A
    "Bidding Contact First Name", # B
    "Bidding Contact Last Name",  # C
    "Bidding Contact Email",      # D
    "Referring Contact Name",     # E
    "Derek's Notes (PRIVATE)",    # F
    "Date Added",                 # G
    "Initial Email Sent At",      # H
    "Status",                     # I
    "Thread ID",                  # J
    "Start Outreach",             # K
]

PROTECTED_EDITORS = ["sales@deboerfreight.com"]


def main() -> None:
    sheets = get_sheets_service().spreadsheets()
    drive = get_drive_service()

    # 1) Create the spreadsheet
    create_body = {
        "properties": {"title": TITLE},
        "sheets": [
            {
                "properties": {
                    "title": TAB_NAME,
                    "gridProperties": {
                        "rowCount": 500,
                        "columnCount": 11,
                        "frozenRowCount": 1,
                    },
                }
            }
        ],
    }
    created = sheets.create(body=create_body).execute()
    sheet_id = created["spreadsheetId"]
    sheet_url = created["spreadsheetUrl"]
    tab_id = created["sheets"][0]["properties"]["sheetId"]
    logger.info("Created spreadsheet: %s", sheet_url)
    logger.info("  id=%s tab_id=%s", sheet_id, tab_id)

    # 2) Write headers
    sheets.values().update(
        spreadsheetId=sheet_id,
        range=f"{TAB_NAME}!A1:K1",
        valueInputOption="RAW",
        body={"values": [HEADERS]},
    ).execute()
    logger.info("Wrote header row.")

    # 3) Batch: header formatting, col K checkbox validation, col F tint,
    #    protected ranges on G:J, protected range on col F visual-only note.
    requests = [
        # Bold + frozen header styling (bold background)
        {
            "repeatCell": {
                "range": {
                    "sheetId": tab_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 11,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.85},
                        "textFormat": {"bold": True},
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        },
        # Col F red tint on body rows (rows 2..500) so it visually reads "private"
        {
            "repeatCell": {
                "range": {
                    "sheetId": tab_id,
                    "startRowIndex": 1,
                    "endRowIndex": 500,
                    "startColumnIndex": 5,  # col F
                    "endColumnIndex": 6,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 1.0, "green": 0.90, "blue": 0.90},
                    }
                },
                "fields": "userEnteredFormat.backgroundColor",
            }
        },
        # Col F header darker red + note
        {
            "repeatCell": {
                "range": {
                    "sheetId": tab_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 5,
                    "endColumnIndex": 6,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.95, "green": 0.70, "blue": 0.70},
                        "textFormat": {"bold": True},
                    },
                    "note": (
                        "PRIVATE SCRATCHPAD — Derek only. Nina's dispatcher, "
                        "reply catcher, and all downstream agents are walled "
                        "off from this column by design. Do not treat anything "
                        "here as context for outgoing email."
                    ),
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat),note",
            }
        },
        # Col K (Start Outreach) -> native checkbox data validation
        {
            "setDataValidation": {
                "range": {
                    "sheetId": tab_id,
                    "startRowIndex": 1,
                    "endRowIndex": 500,
                    "startColumnIndex": 10,  # col K
                    "endColumnIndex": 11,
                },
                "rule": {
                    "condition": {"type": "BOOLEAN"},
                    "strict": True,
                    "showCustomUi": True,
                },
            }
        },
        # Col I (Status) data validation: enum
        {
            "setDataValidation": {
                "range": {
                    "sheetId": tab_id,
                    "startRowIndex": 1,
                    "endRowIndex": 500,
                    "startColumnIndex": 8,  # col I
                    "endColumnIndex": 9,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [
                            {"userEnteredValue": "pending"},
                            {"userEnteredValue": "awaiting_reply"},
                            {"userEnteredValue": "replied"},
                            {"userEnteredValue": "rfq_received"},
                            {"userEnteredValue": "stalled"},
                            {"userEnteredValue": "send_failed"},
                        ],
                    },
                    "strict": False,
                    "showCustomUi": True,
                },
            }
        },
        # Protected range on G:J -> editable ONLY by sales@deboerfreight.com
        {
            "addProtectedRange": {
                "protectedRange": {
                    "range": {
                        "sheetId": tab_id,
                        "startRowIndex": 1,
                        "endRowIndex": 500,
                        "startColumnIndex": 6,  # G
                        "endColumnIndex": 10,   # through J (exclusive end)
                    },
                    "description": (
                        "System-owned columns — written by the MDL Vendor "
                        "Outreach dispatcher. Do not edit manually."
                    ),
                    "warningOnly": False,
                    "editors": {"users": PROTECTED_EDITORS},
                }
            }
        },
        # Auto-resize columns
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": tab_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": 11,
                }
            }
        },
    ]
    sheets.batchUpdate(
        spreadsheetId=sheet_id, body={"requests": requests}
    ).execute()
    logger.info("Applied formatting, validation, and protected ranges.")

    # 4) Share with sales@ and derekndeboer@
    for addr in ["sales@deboerfreight.com", "derekndeboer@gmail.com"]:
        try:
            drive.permissions().create(
                fileId=sheet_id,
                body={"type": "user", "role": "writer", "emailAddress": addr},
                sendNotificationEmail=False,
                fields="id",
            ).execute()
            logger.info("Shared with %s (writer)", addr)
        except Exception as e:
            logger.warning("Could not share with %s: %s", addr, e)

    print("\n" + "=" * 60)
    print("  MDL Vendor Outreach sheet created")
    print("=" * 60)
    print(f"  SHEET_ID: {sheet_id}")
    print(f"  URL:      {sheet_url}")
    print("=" * 60)
    print("\nAdd to .env:")
    print(f"MDL_VENDOR_SHEET_ID={sheet_id}")


if __name__ == "__main__":
    main()
