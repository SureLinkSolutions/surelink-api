import os
import sqlite3
import sys
from difflib import SequenceMatcher
from pathlib import Path

try:
    from .address_matching import (
        canonicalize_component,
        canonicalize_full_address,
        parse_input_address,
        score_candidate,
    )
except ImportError:
    from address_matching import (
        canonicalize_component,
        canonicalize_full_address,
        parse_input_address,
        score_candidate,
    )

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "florida_property_runtime.db"
DB_PATH = Path(os.getenv("SURELINK_DB_PATH", str(DEFAULT_DB_PATH))).expanduser()

APPROVED_PROPERTY_TYPES = {"Single Family", "Townhouse"}
MAX_PROPERTY_VALUE = 700000
MAX_YEAR_BUILT = 2008
MAX_CANDIDATES = 500
HIGH_CONFIDENCE_THRESHOLD = 0.92
REVIEW_CONFIDENCE_THRESHOLD = 0.80


def format_currency(value):
    if value is None:
        return "Unknown"
    return "${0:,.0f}".format(float(value))


def decide_eligibility(row):
    homestead = row.get("homestead_flag")
    year_built = row.get("year_built")
    property_type = row.get("property_type_label")
    property_value = row.get("property_value")

    if homestead != 1:
        return "FAIL", "Not homestead property"

    if year_built is not None and year_built > MAX_YEAR_BUILT:
        return "FAIL", "Home built after {0}".format(MAX_YEAR_BUILT)

    if property_value is not None and property_value > MAX_PROPERTY_VALUE:
        return "FAIL", "Property value exceeds ${0:,}".format(MAX_PROPERTY_VALUE)

    if property_type not in APPROVED_PROPERTY_TYPES:
        return "FAIL", "Property type not eligible: {0}".format(property_type)

    return "PASS", "Eligible"


def build_eligibility_details(row):
    homestead = row.get("homestead_flag")
    year_built = row.get("year_built")
    property_type = row.get("property_type_label")
    property_value = row.get("property_value")

    return {
        "homestead_check": {
            "status": "PASS" if homestead == 1 else "FAIL",
            "value": homestead,
            "expected": 1,
        },
        "year_built_check": {
            "status": "PASS" if year_built is None or year_built <= MAX_YEAR_BUILT else "FAIL",
            "value": year_built,
            "max_allowed": MAX_YEAR_BUILT,
        },
        "property_value_check": {
            "status": "PASS" if property_value is None or property_value <= MAX_PROPERTY_VALUE else "FAIL",
            "value": property_value,
            "max_allowed": MAX_PROPERTY_VALUE,
        },
        "property_type_check": {
            "status": "PASS" if property_type is None or property_type in APPROVED_PROPERTY_TYPES else "FAIL",
            "value": property_type,
            "allowed": sorted(APPROVED_PROPERTY_TYPES),
        },
    }


def get_available_columns(conn):
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(properties)")
    return {row[1] for row in cursor.fetchall()}


def row_to_property(columns, row):
    result = dict(zip(columns, row))

    if result["year_built"] is not None:
        result["year_built"] = int(float(result["year_built"]))

    if result["homestead_flag"] is not None:
        result["homestead_flag"] = int(result["homestead_flag"])

    if result["property_value"] is not None:
        result["property_value"] = float(result["property_value"])

    return result


def normalized_core(value):
    tokens = (value or "").split()
    if tokens and tokens[-1].isdigit() and len(tokens[-1]) == 5:
        return " ".join(tokens[:-1])
    return value or ""


def calculate_match_confidence(parsed, candidate_address, method):
    normalized_input = (parsed.canonical + " " + parsed.zip_code).strip()
    candidate_full = candidate_address or ""

    if method == "exact_normalized":
        return 1.0

    candidate_core = normalized_core(candidate_full)
    input_core = parsed.canonical
    core_similarity = SequenceMatcher(None, input_core, candidate_core).ratio()
    full_similarity = SequenceMatcher(None, normalized_input, candidate_full).ratio()
    house_bonus = 0.08 if parsed.house_number and candidate_core.startswith(parsed.house_number + " ") else 0.0
    zip_bonus = 0.05 if parsed.zip_code and candidate_full.endswith(" " + parsed.zip_code) else 0.0
    return min(0.99, round((core_similarity * 0.7) + (full_similarity * 0.3) + house_bonus + zip_bonus, 4))


def fetch_related_owners(conn, normalized_address, available_columns):
    if "owner_name" not in available_columns:
        return []

    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT DISTINCT owner_name
        FROM properties
        WHERE normalized_address = ?
          AND owner_name IS NOT NULL
          AND TRIM(owner_name) != ''
        ORDER BY owner_name
        """,
        (normalized_address,),
    )
    return [row[0] for row in cursor.fetchall()]


def base_columns():
    return [
        "parcel_id",
        "normalized_address",
        "year_built",
        "homestead_flag",
        "property_value",
        "county_source",
        "property_type_label",
    ]


def build_select_map(available_columns):
    return {
        "parcel_id": "parcel_id",
        "normalized_address": "normalized_address",
        "year_built": "year_built",
        "homestead_flag": "homestead_flag" if "homestead_flag" in available_columns else "homestead_exemption AS homestead_flag",
        "property_value": "property_value",
        "county_source": "county_source" if "county_source" in available_columns else "county AS county_source",
        "property_type_label": "property_type_label" if "property_type_label" in available_columns else "NULL AS property_type_label",
    }


def enrich_runtime_row(row):
    row.setdefault("street_address", None)
    row.setdefault("city", None)
    row.setdefault("zip", None)
    row.setdefault("owner_name", None)
    row.setdefault("dor_uc", None)
    return row


def fetch_exact_row(conn, normalized_address, available_columns):
    columns = base_columns()
    query = """
    SELECT
        {parcel_id},
        {normalized_address},
        {year_built},
        {homestead_flag},
        {property_value},
        {county_source},
        {property_type_label}
    FROM properties
    WHERE normalized_address = ?
    LIMIT 1
    """.format(**build_select_map(available_columns))

    cursor = conn.cursor()
    cursor.execute(query, (normalized_address,))
    row = cursor.fetchone()
    if not row:
        return None
    return enrich_runtime_row(row_to_property(columns, row))


def fetch_candidate_rows(conn, parsed, available_columns):
    columns = [
        *base_columns(),
    ]

    query = """
    SELECT
        {parcel_id},
        {normalized_address},
        {year_built},
        {homestead_flag},
        {property_value},
        {county_source},
        {property_type_label}
    FROM properties
    WHERE normalized_address LIKE ?
    """.format(**build_select_map(available_columns))

    params = [parsed.house_number + " %"] if parsed.house_number else ["%"]

    if parsed.zip_code:
        query += " AND normalized_address LIKE ?"
        params.append("% " + parsed.zip_code)

    query += " LIMIT ?"
    params.append(MAX_CANDIDATES)

    cursor = conn.cursor()
    cursor.execute(query, params)
    return [enrich_runtime_row(row_to_property(columns, row)) for row in cursor.fetchall()]


def score_rows(parsed, rows):
    scored = []
    input_with_zip = (parsed.canonical + " " + parsed.zip_code).strip()

    for row in rows:
        canonical_db = row["normalized_address"]
        candidate_core = normalized_core(canonical_db)
        street_tokens = candidate_core.split()
        city_tokens = []

        score = score_candidate(parsed.tokens, street_tokens, city_tokens)
        if parsed.zip_code and canonical_db.endswith(" " + parsed.zip_code):
            score += 150

        if parsed.zip_code and canonical_db == input_with_zip:
            score += 100

        scored.append(
            {
                "row": row,
                "score": score,
                "canonical_full": canonical_db,
                "canonical_core": " ".join(street_tokens + city_tokens),
                "street_tokens": street_tokens,
                "city_tokens": city_tokens,
            }
        )

    scored.sort(key=lambda item: (-item["score"], item["canonical_full"]))
    return scored


def choose_best_match(parsed, rows):
    if not rows:
        return None, "Property not found", 0.0, "no_match"

    scored = score_rows(parsed, rows)
    input_with_zip = (parsed.canonical + " " + parsed.zip_code).strip()
    input_core = parsed.canonical

    exact_matches = [item for item in scored if item["canonical_full"] == input_with_zip]
    if len(exact_matches) == 1:
        return exact_matches[0]["row"], None, 1.0, "exact_normalized"

    exact_core_matches = [item for item in scored if item["canonical_core"] == input_core]
    if len(exact_core_matches) == 1:
        confidence = calculate_match_confidence(parsed, exact_core_matches[0]["row"]["normalized_address"], "fuzzy")
        return exact_core_matches[0]["row"], None, confidence, "fuzzy"
    if len(exact_core_matches) > 1:
        return None, "Ambiguous property match", 0.0, "ambiguous"

    best = scored[0]
    confidence = calculate_match_confidence(parsed, best["row"]["normalized_address"], "fuzzy")
    if best["score"] < 500 or confidence < REVIEW_CONFIDENCE_THRESHOLD:
        return None, "Property not found", confidence, "fuzzy"

    ties = [item for item in scored if item["score"] == best["score"]]
    if len(ties) > 1:
        return None, "Ambiguous property match", confidence, "ambiguous"

    if len(scored) > 1 and best["score"] - scored[1]["score"] < 80:
        return None, "Ambiguous property match", confidence, "ambiguous"

    return best["row"], None, confidence, "fuzzy"


def derive_output_parse(parsed, property_row):
    if property_row:
        return property_row["normalized_address"], ""

    return parsed.display_core, ""


def print_readable_result(result, property_row=None):
    print("Address: {0}".format(result["input_address"]))
    print("Match found: {0}".format("Yes" if result["match_found"] else "No"))
    print("Parsed street: {0}".format(result["parsed_street"]))
    print("Parsed city: {0}".format(result["parsed_city"]))
    print("Parsed ZIP: {0}".format(result["parsed_zip"]))
    print("Normalized address: {0}".format(result["normalized_address"]))

    if not result["match_found"]:
        print("Decision: {0}".format(result["decision"]))
        print("Reason: {0}".format(result["reason"]))
        return

    owners = result.get("owners") or []
    print("County: {0}".format(result["county"]))
    print("Owners: {0}".format(", ".join(owners) if owners else "Unknown"))
    print("Year built: {0}".format(property_row["year_built"] if property_row["year_built"] is not None else "Unknown"))
    print("Property type: {0}".format(property_row["property_type_label"] or "Unknown"))
    print("Homestead: {0}".format("Yes" if property_row["homestead_flag"] == 1 else "No"))
    print("Property value: {0}".format(format_currency(property_row["property_value"])))
    print("Decision: {0}".format(result["decision"]))
    print("Reason: {0}".format(result["reason"]))


def lookup_property(full_address, db_path=None):
    parsed = parse_input_address(full_address)
    resolved_db_path = Path(db_path).expanduser() if db_path else DB_PATH
    normalized_input = (parsed.canonical + " " + parsed.zip_code).strip()

    if not resolved_db_path.exists():
        raise FileNotFoundError("SQLite database not found: {0}".format(resolved_db_path))

    conn = sqlite3.connect(str(resolved_db_path))
    try:
        available_columns = get_available_columns(conn)
        property_row = fetch_exact_row(conn, normalized_input, available_columns)
        lookup_error = None
        match_confidence = 0.0
        match_method = "normalize_only"

        if property_row:
            match_confidence = 1.0
            match_method = "exact_normalized"
        else:
            candidate_rows = fetch_candidate_rows(conn, parsed, available_columns)
            property_row, lookup_error, match_confidence, match_method = choose_best_match(parsed, candidate_rows)

        owner_names = (
            fetch_related_owners(conn, property_row["normalized_address"], available_columns)
            if property_row
            else []
        )
    finally:
        conn.close()

    parsed_street, parsed_city = derive_output_parse(parsed, property_row)
    normalized_address = property_row["normalized_address"] if property_row else normalized_input
    matched_address = property_row["normalized_address"] if property_row else None
    address_corrected = bool(property_row and matched_address != normalized_input)

    if not property_row:
        return {
            "input_address": full_address,
            "parsed_street": parsed_street,
            "parsed_city": parsed_city,
            "parsed_zip": parsed.zip_code,
            "normalized_address": normalized_address,
            "match_found": False,
            "decision": "FAIL",
            "reason": lookup_error,
            "property_row": None,
            "owners": [],
            "county": None,
            "eligibility_details": None,
            "matched_address": None,
            "address_corrected": False,
            "match_confidence": match_confidence,
            "match_method": match_method,
        }

    decision, reason = decide_eligibility(property_row)
    if match_method == "fuzzy" and match_confidence < HIGH_CONFIDENCE_THRESHOLD:
        decision = "FAIL"
        reason = "Address match confidence too low for automatic verification"

    return {
        "input_address": full_address,
        "parsed_street": parsed_street,
        "parsed_city": parsed_city,
        "parsed_zip": parsed.zip_code,
        "normalized_address": property_row["normalized_address"],
        "match_found": True,
        "decision": decision,
        "reason": reason,
        "property_row": property_row,
        "owners": owner_names,
        "county": property_row["county_source"],
        "eligibility_details": build_eligibility_details(property_row),
        "matched_address": matched_address,
        "address_corrected": address_corrected,
        "match_confidence": match_confidence,
        "match_method": match_method,
    }


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError("SQLite database not found: {0}".format(DB_PATH))

    if len(sys.argv) < 2:
        print("Usage:")
        print('python3 lookup_and_decide.py "234 Garden Grove Parkway, Vero Beach FL 32962"')
        sys.exit(1)

    result = lookup_property(sys.argv[1])
    property_row = result["property_row"]

    if not property_row:
        print_readable_result(result)
        return

    result = {
        "input_address": result["input_address"],
        "parsed_street": result["parsed_street"],
        "parsed_city": result["parsed_city"],
        "parsed_zip": result["parsed_zip"],
        "normalized_address": property_row["normalized_address"],
        "match_found": True,
        "county": result["county"],
        "owners": result["owners"],
        "decision": result["decision"],
        "reason": result["reason"],
    }

    print_readable_result(result, property_row=property_row)


if __name__ == "__main__":
    main()
