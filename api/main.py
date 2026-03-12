from datetime import datetime, timezone
from typing import Any, Optional, Union

from fastapi import FastAPI
from pydantic import BaseModel, Field

from Scripts.lookup_and_decide import APPROVED_PROPERTY_TYPES, lookup_property


app = FastAPI(title="SureLink API")


class VerifyHomeownerRequest(BaseModel):
    address: str = Field(..., min_length=1, description="Property address to verify")
    record_id: Optional[str] = Field(default=None, description="Optional source record id")
    homeowner_name: Optional[str] = Field(default=None, description="Optional homeowner name")
    email: Optional[str] = Field(default=None, description="Optional contact email")
    phone: Optional[str] = Field(default=None, description="Optional contact phone")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def to_review_yes_no(value: Optional[bool]) -> Optional[str]:
    if value is True:
        return "Yes"
    if value is False:
        return "No"
    return None


def normalize_review_property_type(property_type: Optional[str]) -> Optional[str]:
    if not property_type:
        return None

    normalized = " ".join(property_type.strip().lower().replace("-", " ").split())

    exact_map = {
        "single family": "Single Family Home",
        "single family home": "Single Family Home",
        "sfr": "Single Family Home",
        "townhouse": "Townhome",
        "townhome": "Townhome",
        "condo": "Condo",
        "condominium": "Condo",
        "multi family": "Multi-Family",
        "multifamily": "Multi-Family",
        "mobile home": "Mobile Home",
        "manufactured home": "Mobile Home",
    }
    if normalized in exact_map:
        return exact_map[normalized]

    if "single family" in normalized:
        return "Single Family Home"
    if "townhome" in normalized or "townhouse" in normalized:
        return "Townhome"
    if "condo" in normalized or "condominium" in normalized:
        return "Condo"
    if "multi" in normalized and "family" in normalized:
        return "Multi-Family"
    if "mobile" in normalized or "manufactured" in normalized:
        return "Mobile Home"

    return None


def normalize_final_decision(
    eligible: Optional[bool],
    verification_status: str,
    manual_review_required: bool,
) -> Optional[str]:
    if manual_review_required or verification_status == "manual_review" or eligible is None:
        return None
    return "Pass" if eligible else "Fail"


def normalize_fail_reason(
    *,
    eligible: Optional[bool],
    verification_status: str,
    manual_review_required: bool,
    message: str,
) -> Optional[str]:
    if manual_review_required or verification_status == "manual_review" or eligible is not False:
        return None

    normalized = (message or "").strip().lower()
    if not normalized:
        return None

    if normalized.startswith("property type not eligible"):
        return "Property Type"
    if normalized.startswith("property value exceeds"):
        return "Property Value"
    if normalized == "not homestead property":
        return "No Homestead Exemption"
    if normalized.startswith("home built after"):
        return "Year Built"
    if "out of state" in normalized:
        return "Out of State"

    return None


def build_response(
    *,
    status: str,
    record_id: Optional[str],
    address: str,
    input_address: str,
    matched_address: Optional[str],
    address_corrected: bool,
    match_confidence: Optional[float],
    match_method: str,
    county: Optional[str],
    owner_name: Optional[str],
    verified: bool,
    eligible: Optional[bool],
    verification_status: str,
    message: str,
    homestead_exemption: Optional[bool],
    year_built: Optional[int],
    year_built_pass: Optional[bool],
    property_type: Optional[str],
    property_type_pass: Optional[bool],
    property_value: Optional[Union[float, int]],
    property_value_pass: Optional[bool],
    manual_review_required: bool,
    manual_review_reason: Optional[str],
) -> dict[str, Any]:
    return {
        "status": status,
        "record_id": record_id,
        "address": address,
        "input_address": input_address,
        "matched_address": matched_address,
        "address_corrected": address_corrected,
        "match_confidence": match_confidence,
        "match_method": match_method,
        "county": county,
        "owner_name": owner_name,
        "verified": verified,
        "eligible": eligible,
        "verification_status": verification_status,
        "message": message,
        "homestead_exemption": homestead_exemption,
        "year_built": year_built,
        "year_built_pass": year_built_pass,
        "property_type": property_type,
        "property_type_pass": property_type_pass,
        "property_value": property_value,
        "property_value_pass": property_value_pass,
        "manual_review_required": manual_review_required,
        "manual_review_reason": manual_review_reason,
        "review_homestead": to_review_yes_no(homestead_exemption),
        "review_owner_match": None,
        "review_property_type": normalize_review_property_type(property_type),
        "final_decision": normalize_final_decision(
            eligible,
            verification_status,
            manual_review_required,
        ),
        "fail_reason": normalize_fail_reason(
            eligible=eligible,
            verification_status=verification_status,
            manual_review_required=manual_review_required,
            message=message,
        ),
        "checked_at": utc_now_iso(),
    }


def normalize_property_value(value: Optional[float]) -> Optional[Union[float, int]]:
    if value is None:
        return None
    if float(value).is_integer():
        return int(value)
    return value


def extract_property_type(property_row: dict[str, Any]) -> Optional[str]:
    property_type = property_row.get("property_type_label")
    if property_type:
        return property_type

    fallback_property_type = property_row.get("property_type")
    if isinstance(fallback_property_type, str):
        cleaned = fallback_property_type.strip()
        return cleaned or None

    return None


def map_verification_result(payload: VerifyHomeownerRequest) -> dict[str, Any]:
    input_address = payload.address.strip()
    lookup = lookup_property(input_address, homeowner_name=payload.homeowner_name)
    property_row = lookup["property_row"]
    normalized_address = lookup["normalized_address"] or input_address
    matched_address = lookup.get("matched_address")
    address_corrected = bool(lookup.get("address_corrected"))
    match_confidence = lookup.get("match_confidence")
    match_method = lookup.get("match_method", "normalize_only")

    if not property_row:
        return build_response(
            status="error",
            record_id=payload.record_id,
            address=normalized_address,
            input_address=input_address,
            matched_address=matched_address,
            address_corrected=address_corrected,
            match_confidence=match_confidence,
            match_method=match_method,
            county=None,
            owner_name=None,
            verified=False,
            eligible=None,
            verification_status="manual_review",
            message="Verification could not be completed automatically.",
            homestead_exemption=None,
            year_built=None,
            year_built_pass=None,
            property_type=None,
            property_type_pass=None,
            property_value=None,
            property_value_pass=None,
            manual_review_required=True,
            manual_review_reason=lookup["reason"],
        )

    homestead_flag = property_row.get("homestead_flag")
    year_built = property_row.get("year_built")
    property_type = extract_property_type(property_row)
    property_value = property_row.get("property_value")
    year_built_pass = None if year_built is None else year_built <= 2008
    property_type_pass = None if property_type is None else property_type in APPROVED_PROPERTY_TYPES
    property_value_pass = None if property_value is None else property_value <= 700000

    eligible = lookup["decision"] == "PASS"
    verification_status = "eligible" if eligible else "ineligible"
    message = (
        "Property appears to meet current SureLink screening requirements."
        if eligible
        else lookup["reason"]
    )
    manual_review_required = False
    manual_review_reason = None

    if match_method == "fuzzy" and (match_confidence is None or match_confidence < 0.92):
        verification_status = "manual_review"
        message = "Verification requires manual review."
        manual_review_required = True
        manual_review_reason = "Address match confidence below automatic verification threshold."
        eligible = None

    if homestead_flag is None:
        verification_status = "manual_review"
        message = "Verification requires manual review."
        manual_review_required = True
        manual_review_reason = "Homestead exemption could not be verified from the property record."

    return build_response(
        status="success",
        record_id=payload.record_id,
        address=normalized_address,
        input_address=input_address,
        matched_address=matched_address,
        address_corrected=address_corrected,
        match_confidence=match_confidence,
        match_method=match_method,
        county=lookup["county"],
        owner_name=lookup.get("owner_name"),
        verified=True,
        eligible=eligible if verification_status != "manual_review" else None,
        verification_status=verification_status,
        message=message,
        homestead_exemption=True if homestead_flag == 1 else False if homestead_flag == 0 else None,
        year_built=year_built,
        year_built_pass=year_built_pass,
        property_type=property_type,
        property_type_pass=property_type_pass,
        property_value=normalize_property_value(property_value),
        property_value_pass=property_value_pass,
        manual_review_required=manual_review_required,
        manual_review_reason=manual_review_reason,
    )


@app.get("/")
def read_root() -> dict[str, str]:
    return {"status": "ok", "service": "surelink-api"}


@app.post("/verify-homeowner")
def verify_homeowner(payload: VerifyHomeownerRequest) -> dict[str, Any]:
    raw_address = payload.address.strip()

    try:
        return map_verification_result(payload)
    except FileNotFoundError as exc:
        return build_response(
            status="error",
            record_id=payload.record_id,
            address=raw_address,
            input_address=raw_address,
            matched_address=None,
            address_corrected=False,
            match_confidence=None,
            match_method="normalize_only",
            county=None,
            owner_name=None,
            verified=False,
            eligible=None,
            verification_status="manual_review",
            message="Verification could not be completed automatically.",
            homestead_exemption=None,
            year_built=None,
            year_built_pass=None,
            property_type=None,
            property_type_pass=None,
            property_value=None,
            property_value_pass=None,
            manual_review_required=True,
            manual_review_reason=str(exc),
        )
    except Exception as exc:
        return build_response(
            status="error",
            record_id=payload.record_id,
            address=raw_address,
            input_address=raw_address,
            matched_address=None,
            address_corrected=False,
            match_confidence=None,
            match_method="normalize_only",
            county=None,
            owner_name=None,
            verified=False,
            eligible=None,
            verification_status="manual_review",
            message="Verification could not be completed automatically.",
            homestead_exemption=None,
            year_built=None,
            year_built_pass=None,
            property_type=None,
            property_type_pass=None,
            property_value=None,
            property_value_pass=None,
            manual_review_required=True,
            manual_review_reason="Unexpected verification error: {0}".format(exc),
        )
