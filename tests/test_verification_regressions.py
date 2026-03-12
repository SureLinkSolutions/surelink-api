import unittest

from api.main import VerifyHomeownerRequest, map_verification_result
from Scripts.address_matching import parse_input_address


class AddressParsingRegressionTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
