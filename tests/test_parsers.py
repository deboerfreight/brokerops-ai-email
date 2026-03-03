"""
Test suite for BrokerOps AI parsers.

Covers:
  - Load email parsing
  - Time window normalisation
  - Date normalisation
  - Equipment type normalisation
  - Quote / RFQ reply parsing
  - Approval reply parsing
"""
import pytest
from app.parsers import (
    parse_load_email,
    normalise_time_window,
    normalise_date,
    normalise_equipment,
    parse_quote_reply,
    parse_approval_reply,
)


# ── Time window normalisation ────────────────────────────────────────────────

class TestNormaliseTimeWindow:
    def test_24h_range(self):
        assert normalise_time_window("08:00-17:00") == "08:00-17:00"

    def test_ampm_range(self):
        assert normalise_time_window("8am-5pm") == "08:00-17:00"

    def test_ampm_with_colon(self):
        assert normalise_time_window("8:00AM to 5:00PM") == "08:00-17:00"

    def test_open(self):
        assert normalise_time_window("OPEN") == "OPEN"

    def test_fcfs(self):
        assert normalise_time_window("FCFS") == "OPEN"

    def test_empty(self):
        assert normalise_time_window("") == "OPEN"

    def test_flexible(self):
        assert normalise_time_window("Flexible") == "OPEN"

    def test_12pm_range(self):
        assert normalise_time_window("12pm-4pm") == "12:00-16:00"

    def test_12am(self):
        assert normalise_time_window("12am-6am") == "00:00-06:00"


# ── Date normalisation ───────────────────────────────────────────────────────

class TestNormaliseDate:
    def test_mm_dd_yyyy(self):
        assert normalise_date("03/15/2026") == "2026-03-15"

    def test_iso_format(self):
        assert normalise_date("2026-03-15") == "2026-03-15"

    def test_month_name(self):
        assert normalise_date("March 15, 2026") == "2026-03-15"

    def test_abbrev_month(self):
        assert normalise_date("Mar 15, 2026") == "2026-03-15"

    def test_mm_dd_yy(self):
        assert normalise_date("03/15/26") == "2026-03-15"


# ── Equipment normalisation ──────────────────────────────────────────────────

class TestNormaliseEquipment:
    def test_dry_van_variants(self):
        assert normalise_equipment("Dry Van") == "DRY_VAN"
        assert normalise_equipment("dryvan") == "DRY_VAN"
        assert normalise_equipment("van") == "DRY_VAN"

    def test_flatbed(self):
        assert normalise_equipment("flatbed") == "FLATBED"
        assert normalise_equipment("Flat Bed") == "FLATBED"

    def test_reefer(self):
        assert normalise_equipment("Reefer") == "REEFER"
        assert normalise_equipment("refrigerated") == "REEFER"

    def test_canonical(self):
        assert normalise_equipment("DRY_VAN") == "DRY_VAN"
        assert normalise_equipment("HOTSHOT") == "HOTSHOT"


# ── Load email parsing ───────────────────────────────────────────────────────

class TestParseLoadEmail:
    SAMPLE_EMAIL = """
Customer: Acme Manufacturing
Origin: Miami, FL 33101
Destination: Dallas, TX 75201
Pickup Date: 03/20/2026
Pickup Time: 8am-5pm
Delivery Date: 03/22/2026
Delivery Time: OPEN
Equipment: Dry Van
Commodity: Auto Parts
Weight: 42,000 lbs
Temp Control: No
Hazmat: No
Target Rate: $2,500
"""

    def test_basic_extraction(self):
        result = parse_load_email(self.SAMPLE_EMAIL)
        assert result["Customer_Name"] == "Acme Manufacturing"
        assert result["Origin_City"] == "Miami"
        assert result["Origin_State"] == "FL"
        assert result["Destination_City"] == "Dallas"
        assert result["Destination_State"] == "TX"
        assert result["Equipment_Type"] == "DRY_VAN"
        assert result["Commodity"] == "Auto Parts"
        assert result["Weight_Lbs"] == "42000"
        assert result["Target_Buy_Rate"] == "2500"
        assert result["Temp_Control_Required"] == "FALSE"
        assert result["Hazmat"] == "FALSE"

    def test_time_windows_normalised(self):
        result = parse_load_email(self.SAMPLE_EMAIL)
        assert result["Pickup_Time_Window"] == "08:00-17:00"
        assert result["Delivery_Time_Window"] == "OPEN"

    def test_date_normalised(self):
        result = parse_load_email(self.SAMPLE_EMAIL)
        assert result["Pickup_Date"] == "2026-03-20"
        assert result["Delivery_Date"] == "2026-03-22"

    def test_hazmat_yes(self):
        email = "Hazmat: Yes\nCommodity: Chemicals"
        result = parse_load_email(email)
        assert result["Hazmat"] == "TRUE"

    def test_temp_control_required(self):
        email = "Temp Control: Required\nEquipment: Reefer"
        result = parse_load_email(email)
        assert result["Temp_Control_Required"] == "TRUE"

    ANOTHER_FORMAT = """
Shipper: Global Foods Inc
From: Jacksonville, FL
To: Atlanta, GA
Pick up date: March 18, 2026
Pick up window: 6:00AM to 2:00PM
Delivery date: March 19, 2026
Delivery window: flexible
Equipment type: Reefer
Commodity: Frozen Seafood
Weight: 38000 lbs
Temp: Required
Rate: $1,800
"""

    def test_alternate_format(self):
        result = parse_load_email(self.ANOTHER_FORMAT)
        assert result["Customer_Name"] == "Global Foods Inc"
        assert result["Origin_City"] == "Jacksonville"
        assert result["Origin_State"] == "FL"
        assert result["Destination_City"] == "Atlanta"
        assert result["Destination_State"] == "GA"
        assert result["Pickup_Date"] == "2026-03-18"
        assert result["Pickup_Time_Window"] == "06:00-14:00"
        assert result["Delivery_Time_Window"] == "OPEN"
        assert result["Equipment_Type"] == "REEFER"
        assert result["Temp_Control_Required"] == "TRUE"
        assert result["Target_Buy_Rate"] == "1800"


# ── Quote / RFQ reply parsing ────────────────────────────────────────────────

class TestParseQuoteReply:
    def test_structured_reply(self):
        body = """
Thanks for the load opportunity.

Rate: $2,450
Availability: Yes
Transit Time: 2 days
Any restrictions: None
"""
        result = parse_quote_reply(body)
        assert result["rate"] == 2450.0
        assert result["availability"] == "Yes"
        assert result["transit_time"] == "2 days"
        assert result["restrictions"] == "None"

    def test_first_dollar_rule(self):
        body = "We can do this for $1,950. Let me know."
        result = parse_quote_reply(body)
        assert result["rate"] == 1950.0

    def test_no_rate(self):
        body = "We are not available for this lane at this time."
        result = parse_quote_reply(body)
        assert result["rate"] is None

    def test_rate_with_cents(self):
        body = "Rate: $2,100.50"
        result = parse_quote_reply(body)
        assert result["rate"] == 2100.50

    def test_unstructured_dollar(self):
        body = "I can haul it for $3200 and be there on time."
        result = parse_quote_reply(body)
        assert result["rate"] == 3200.0


# ── Approval reply parsing ───────────────────────────────────────────────────

class TestParseApprovalReply:
    def test_approve(self):
        body = "APPROVE 2026-0001"
        result = parse_approval_reply(body)
        assert result["action"] == "APPROVE"
        assert result["load_id"] == "2026-0001"

    def test_reject(self):
        body = "REJECT 2026-0015"
        result = parse_approval_reply(body)
        assert result["action"] == "REJECT"
        assert result["load_id"] == "2026-0015"

    def test_approve_in_reply_context(self):
        body = """Thanks for the update.

APPROVE 2026-0003

Best,
Derek
"""
        result = parse_approval_reply(body)
        assert result["action"] == "APPROVE"
        assert result["load_id"] == "2026-0003"

    def test_no_action(self):
        body = "I'll review this later."
        result = parse_approval_reply(body)
        assert result["action"] is None
        assert result["load_id"] is None

    def test_wrong_format_not_matched(self):
        body = "Please approve load 2026-0001"
        result = parse_approval_reply(body)
        # "approve" (lowercase) with "Please" prefix should NOT match
        assert result["action"] is None
