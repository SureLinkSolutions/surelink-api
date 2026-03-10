import sqlite3
import sys
from pathlib import Path

from address_matching import (
    canonicalize_component,
    canonicalize_full_address,
    parse_input_address,
    score_candidate,
)

DB_PATH = Path("/Users/ericbrown/SureLink/data/florida_property_lookup.db")

APPROVED_PROPERTY_TYPES = {"Single Family", "Townhouse"}
MAX_PROPERTY_VALUE = 700000
MAX_YEAR_BUILT = 2008
MAX_CANDIDATES = 500


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
            "status": "PASS" if property_type in APPROVED_PROPERTY_TYPES else "FAIL",
            "value": property_type,
            "allowed": sorted(APPROVED_PROPERTY_TYPES),
        },
    }


def row_to_property(columns, row):
    result = dict(zip(columns, row))

    if result["year_built"] is not None:
        result["year_built"] = int(float(result["year_built"]))

    if result["homestead_flag"] is not None:
        result["homestead_flag"] = int(result["homestead_flag"])

    if result["property_value"] is not None:
        result["property_value"] = float(result["property_value"])

    return result


def fetch_related_owners(conn, normalized_address):
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


def fetch_candidate_rows(conn, parsed):
    columns = [
        "parcel_id",
        "street_address",
        "normalized_address",
        "city",
        "zip",
        "owner_name",
        "year_built",
        "dor_uc",
        "property_type_label",
        "homestead_flag",
        "property_value",
        "county_source",
    ]

    query = """
    SELECT
        parcel_id,
        street_address,
        normalized_address,
        city,
        zip,
        owner_name,
        year_built,
        dor_uc,
        property_type_label,
        homestead_flag,
        property_value,
        county_source
    FROM properties
    WHERE normalized_address LIKE ?
    """

    params = [parsed.house_number + " %"] if parsed.house_number else ["%"]

    if parsed.zip_code:
        query += " AND zip = ?"
        params.append(parsed.zip_code)

    query += " LIMIT ?"
    params.append(MAX_CANDIDATES)

    cursor = conn.cursor()
    cursor.execute(query, params)
    return [row_to_property(columns, row) for row in cursor.fetchall()]


def score_rows(parsed, rows):
    scored = []
    input_with_zip = (parsed.canonical + " " + parsed.zip_code).strip()

    for row in rows:
        street_tokens = canonicalize_component(row["street_address"]).split()
        city_tokens = canonicalize_component(row["city"], city_mode=True).split()
        canonical_db = canonicalize_full_address(row["street_address"], row["city"], row["zip"])

        score = score_candidate(parsed.tokens, street_tokens, city_tokens)
        if parsed.zip_code and str(row["zip"]).split(".")[0] == parsed.zip_code:
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
        return None, "Property not found"

    scored = score_rows(parsed, rows)
    input_with_zip = (parsed.canonical + " " + parsed.zip_code).strip()
    input_core = parsed.canonical

    exact_matches = [item for item in scored if item["canonical_full"] == input_with_zip]
    if len(exact_matches) == 1:
        return exact_matches[0]["row"], None

    exact_core_matches = [item for item in scored if item["canonical_core"] == input_core]
    if len(exact_core_matches) == 1:
        return exact_core_matches[0]["row"], None
    if len(exact_core_matches) > 1:
        return None, "Ambiguous property match"

    best = scored[0]
    if best["score"] < 500:
        return None, "Property not found"

    ties = [item for item in scored if item["score"] == best["score"]]
    if len(ties) > 1:
        return None, "Ambiguous property match"

    if len(scored) > 1 and best["score"] - scored[1]["score"] < 80:
        return None, "Ambiguous property match"

    return best["row"], None


def derive_output_parse(parsed, property_row):
    if property_row:
        return property_row["street_address"], property_row["city"]

    tokens = parsed.tokens
    if len(tokens) < 2:
        return parsed.display_core, ""

    return " ".join(tokens[:-1]), tokens[-1]


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


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError("SQLite database not found: {0}".format(DB_PATH))

    if len(sys.argv) < 2:
        print("Usage:")
        print('python3 lookup_and_decide.py "234 Garden Grove Parkway, Vero Beach FL 32962"')
        sys.exit(1)

    full_address = sys.argv[1]
    parsed = parse_input_address(full_address)

    conn = sqlite3.connect(str(DB_PATH))
    candidate_rows = fetch_candidate_rows(conn, parsed)
    property_row, lookup_error = choose_best_match(parsed, candidate_rows)
    owner_names = fetch_related_owners(conn, property_row["normalized_address"]) if property_row else []
    conn.close()

    parsed_street, parsed_city = derive_output_parse(parsed, property_row)
    normalized_address = canonicalize_full_address(parsed_street, parsed_city, parsed.zip_code)

    if not property_row:
        result = {
            "input_address": full_address,
            "parsed_street": parsed_street,
            "parsed_city": parsed_city,
            "parsed_zip": parsed.zip_code,
            "normalized_address": normalized_address,
            "match_found": False,
            "decision": "FAIL",
            "reason": lookup_error,
        }
        print_readable_result(result)
        return

    decision, reason = decide_eligibility(property_row)

    result = {
        "input_address": full_address,
        "parsed_street": parsed_street,
        "parsed_city": parsed_city,
        "parsed_zip": parsed.zip_code,
        "normalized_address": property_row["normalized_address"],
        "match_found": True,
        "county": property_row["county_source"],
        "owners": owner_names,
        "decision": decision,
        "reason": reason,
    }

    print_readable_result(result, property_row=property_row)


if __name__ == "__main__":
    main()
