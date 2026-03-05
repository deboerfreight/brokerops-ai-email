#!/usr/bin/env python3
"""Seed Carrier_Master sheet with Miami reefer carriers.

Usage:
    # Ensure env vars are set (CARRIER_MASTER_SHEET_ID, GCP creds, etc.)
    python -m scripts.seed_carriers
"""
from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.sheets import get_all_carriers, append_row, read_range, CARRIER_MASTER_COLUMNS
from app.config import get_settings


MIAMI_REEFER_CARRIERS = [
    {
        "MC_Number": "834058",
        "DOT_Number": "2421537",
        "Legal_Name": "D & M International Express LLC",
        "Primary_Email": "dmexpress305@gmail.com",
        "Primary_Phone": "(786) 615-7786",
        "Equipment_Type": "Reefer",
        "Preferred_Lanes": "Miami, FL",
        "Active": "TRUE",
        "Authority_Status": "ACTIVE",
        "Onboarding_Status": "NEW",
        "Internal_Notes": "Seeded from FMCSA/public directories. 8 trucks, dry van & reefer.",
    },
    {
        "MC_Number": "732813",
        "DOT_Number": "2094291",
        "Legal_Name": "Reyes Best Trucking Inc",
        "Primary_Email": "reyesbest1@hotmail.com",
        "Primary_Phone": "(786) 399-1288",
        "Equipment_Type": "Reefer",
        "Preferred_Lanes": "Miami, FL",
        "Active": "TRUE",
        "Authority_Status": "ACTIVE",
        "Onboarding_Status": "NEW",
        "Internal_Notes": "Seeded from FMCSA/public directories. 12 trucks, produce & cold food.",
    },
    {
        "MC_Number": "1194273",
        "DOT_Number": "3546246",
        "Legal_Name": "Reef Logistics Enterprises LLC",
        "Primary_Email": "",
        "Primary_Phone": "(305) 495-3210",
        "Equipment_Type": "Reefer",
        "Preferred_Lanes": "Miami, FL",
        "Active": "TRUE",
        "Authority_Status": "ACTIVE",
        "Onboarding_Status": "NEW",
        "Internal_Notes": "Seeded from FMCSA/public directories. 50 trucks. Email needed.",
    },
    {
        "MC_Number": "453152",
        "DOT_Number": "1097043",
        "Legal_Name": "Florida Beauty Express Inc",
        "Primary_Email": "",
        "Primary_Phone": "(305) 477-7611",
        "Equipment_Type": "Reefer",
        "Preferred_Lanes": "Miami, FL",
        "Active": "TRUE",
        "Authority_Status": "ACTIVE",
        "Onboarding_Status": "NEW",
        "Internal_Notes": "Seeded from FMCSA/public directories. 235 trucks, produce/flowers/reefer. Satisfactory safety rating. Email needed.",
    },
    {
        "MC_Number": "458655",
        "DOT_Number": "1069647",
        "Legal_Name": "C P Y Transport Inc",
        "Primary_Email": "",
        "Primary_Phone": "(305) 252-3305",
        "Equipment_Type": "Reefer",
        "Preferred_Lanes": "Miami, FL",
        "Active": "TRUE",
        "Authority_Status": "ACTIVE",
        "Onboarding_Status": "NEW",
        "Internal_Notes": "Seeded from FMCSA/public directories. Email needed.",
    },
    {
        "MC_Number": "454786",
        "DOT_Number": "1103850",
        "Legal_Name": "Proline Trucking Corp",
        "Primary_Email": "",
        "Primary_Phone": "(305) 439-0100",
        "Equipment_Type": "Reefer",
        "Preferred_Lanes": "Miami, FL",
        "Active": "TRUE",
        "Authority_Status": "ACTIVE",
        "Onboarding_Status": "NEW",
        "Internal_Notes": "Seeded from FMCSA/public directories. Email needed.",
    },
    {
        "MC_Number": "491707",
        "DOT_Number": "1079148",
        "Legal_Name": "L & M Transportation Express Inc",
        "Primary_Email": "",
        "Primary_Phone": "(305) 970-2749",
        "Equipment_Type": "Reefer",
        "Preferred_Lanes": "Miami, FL",
        "Active": "TRUE",
        "Authority_Status": "ACTIVE",
        "Onboarding_Status": "NEW",
        "Internal_Notes": "Seeded from FMCSA/public directories. Email needed.",
    },
    {
        "MC_Number": "514956",
        "DOT_Number": "1333940",
        "Legal_Name": "ADP Transport",
        "Primary_Email": "",
        "Primary_Phone": "(305) 257-0001",
        "Equipment_Type": "Reefer",
        "Preferred_Lanes": "Miami, FL",
        "Active": "TRUE",
        "Authority_Status": "ACTIVE",
        "Onboarding_Status": "NEW",
        "Internal_Notes": "Seeded from FMCSA/public directories. Email needed.",
    },
    {
        "MC_Number": "401610",
        "DOT_Number": "857069",
        "Legal_Name": "S & M Transport",
        "Primary_Email": "",
        "Primary_Phone": "(305) 621-5617",
        "Equipment_Type": "Reefer",
        "Preferred_Lanes": "Miami, FL",
        "Active": "TRUE",
        "Authority_Status": "ACTIVE",
        "Onboarding_Status": "NEW",
        "Internal_Notes": "Seeded from FMCSA/public directories. Email needed.",
    },
    {
        "MC_Number": "523155",
        "DOT_Number": "1352771",
        "Legal_Name": "J & J Carriers",
        "Primary_Email": "",
        "Primary_Phone": "(786) 357-0213",
        "Equipment_Type": "Reefer",
        "Preferred_Lanes": "Miami, FL",
        "Active": "TRUE",
        "Authority_Status": "ACTIVE",
        "Onboarding_Status": "NEW",
        "Internal_Notes": "Seeded from FMCSA/public directories. Email needed.",
    },
]


def seed():
    settings = get_settings()
    sheet_id = settings.CARRIER_MASTER_SHEET_ID
    if not sheet_id:
        print("ERROR: CARRIER_MASTER_SHEET_ID not set in environment.")
        sys.exit(1)

    # Check for existing carriers to avoid duplicates
    existing = get_all_carriers()
    existing_mcs = {c.get("MC_Number") for c in existing}
    print(f"Found {len(existing)} existing carriers in sheet.")

    added = 0
    skipped = 0
    for carrier in MIAMI_REEFER_CARRIERS:
        mc = carrier["MC_Number"]
        if mc in existing_mcs:
            print(f"  SKIP  MC {mc} ({carrier['Legal_Name']}) — already in sheet")
            skipped += 1
            continue

        row = [carrier.get(col, "") for col in CARRIER_MASTER_COLUMNS]
        append_row(sheet_id, "Sheet1!A:W", row)
        print(f"  ADDED MC {mc} ({carrier['Legal_Name']})")
        added += 1

    print(f"\nDone: {added} added, {skipped} skipped (duplicates).")


if __name__ == "__main__":
    seed()
