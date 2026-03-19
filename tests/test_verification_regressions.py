import unittest
from unittest.mock import patch

from api.main import VerifyHomeownerRequest, map_verification_result
from scripts.verification.address_matching import parse_input_address


class AddressParsingRegressionTests(unittest.TestCase):
    def test_ocala_hemlock_terrace_drive_keeps_full_street_name(self):
        parsed = parse_input_address("20 Hemlock Terrace Drive Ocala FL 34472")

        self.assertEqual(parsed.canonical_street, "20 HEMLOCK TER DR")
        self.assertEqual(parsed.canonical_city, "OCALA")
        self.assertEqual(parsed.zip_code, "34472")

    def test_cape_coral_address_splits_street_and_city(self):
        parsed = parse_input_address("2913 NW 13th street Cape Coral FL 33993")

        self.assertEqual(parsed.canonical_street, "2913 NW 13 ST")
        self.assertEqual(parsed.canonical_city, "CAPE CORAL")
        self.assertEqual(parsed.zip_code, "33993")

    def test_st_pete_duplicate_locality_normalizes_city(self):
        parsed = parse_input_address(
            "5251 48th terrace n st pete fl 33709 Saint Petersburg FL 33709"
        )

        self.assertEqual(parsed.canonical_street, "5251 48 TER N")
        self.assertEqual(parsed.canonical_city, "ST PETERSBURG")
        self.assertEqual(parsed.zip_code, "33709")


class VerificationRegressionTests(unittest.TestCase):
    def test_ocala_hemlock_terrace_drive_is_eligible_with_homestead(self):
        result = map_verification_result(
            VerifyHomeownerRequest(
                address="20 Hemlock Terrace Drive Ocala FL 34472",
            )
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["verification_status"], "eligible")
        self.assertTrue(result["eligible"])
        self.assertTrue(result["homestead_exemption"])
        self.assertEqual(result["matched_address"], "20 HEMLOCK TER DR OCALA 34472")

    def test_cape_coral_address_is_eligible(self):
        result = map_verification_result(
            VerifyHomeownerRequest(
                address="2913 NW 13th street Cape Coral FL 33993",
                homeowner_name="Andrew Asselin",
            )
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["verification_status"], "eligible")
        self.assertEqual(result["owner_name"], "ASSELIN ANDREW")
        self.assertEqual(result["county"], "Lee")
        self.assertTrue(result["eligible"])

    def test_exact_address_match_does_not_fail_on_owner_name_difference(self):
        result = map_verification_result(
            VerifyHomeownerRequest(
                address="2913 NW 13th street Cape Coral FL 33993",
                homeowner_name="Jamie Martin",
            )
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["verification_status"], "eligible")
        self.assertEqual(result["owner_name"], "ASSELIN ANDREW")

    def test_st_pete_address_remains_manual_review_when_not_found(self):
        result = map_verification_result(
            VerifyHomeownerRequest(
                address="5251 48th terrace n st pete fl 33709 Saint Petersburg FL 33709",
                homeowner_name="Helene Martin Bush",
            )
        )

        self.assertEqual(result["verification_status"], "manual_review")
        self.assertEqual(result["manual_review_reason"], "Ambiguous property match")
        self.assertIsNone(result["eligible"])

    def test_property_type_falls_back_to_alternate_lookup_key(self):
        with patch("api.main.lookup_property") as mock_lookup_property:
            mock_lookup_property.return_value = {
                "property_row": {
                    "homestead_flag": 1,
                    "year_built": 2001,
                    "property_type": "Townhouse",
                    "property_value": 325000,
                },
                "normalized_address": "123 MAIN ST TAMPA 33602",
                "matched_address": "123 MAIN ST TAMPA 33602",
                "address_corrected": False,
                "match_confidence": 1.0,
                "match_method": "exact_normalized",
                "decision": "PASS",
                "reason": "Eligible",
                "county": "Hillsborough",
                "owner_name": "DOE JOHN",
            }

            result = map_verification_result(
                VerifyHomeownerRequest(
                    address="123 Main St Tampa FL 33602",
                    homeowner_name="John Doe",
                )
            )

        self.assertEqual(result["property_type"], "Townhouse")
        self.assertEqual(result["review_property_type"], "Townhome")
        self.assertTrue(result["property_type_pass"])


if __name__ == "__main__":
    unittest.main()
