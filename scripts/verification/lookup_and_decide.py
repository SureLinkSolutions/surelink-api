import os
import sqlite3
import sys
import logging
import re
from difflib import SequenceMatcher
from pathlib import Path

try:
    from .address_matching import (
        canonicalize_component,
        canonicalize_full_address,
        parse_input_address,
        score_candidate,
        split_normalized_address,
    )
except ImportError:
    from address_matching import (
        canonicalize_component,
        canonicalize_full_address,
        parse_input_address,
        score_candidate,
        split_normalized_address,
    )

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "florida_property_runtime.db"
DEFAULT_DIAGNOSTIC_DB_PATH = PROJECT_ROOT / "data" / "florida_property_diagnostic.db"
DEFAULT_SOURCE_DB_PATH = PROJECT_ROOT / "data" / "florida_property_lookup.db"
DB_PATH = Path(os.getenv("SURELINK_DB_PATH", str(DEFAULT_DB_PATH))).expanduser()
DIAGNOSTIC_DB_PATH = Path(
    os.getenv("SURELINK_DIAGNOSTIC_DB_PATH", str(DEFAULT_DIAGNOSTIC_DB_PATH))
).expanduser()
SOURCE_DB_PATH = Path(os.getenv("SURELINK_SOURCE_DB_PATH", str(DEFAULT_SOURCE_DB_PATH))).expanduser()

APPROVED_PROPERTY_TYPES = {"Single Family", "Townhouse"}
MAX_PROPERTY_VALUE = 700000
MAX_YEAR_BUILT = 2008
MAX_CANDIDATES = 500
HIGH_CONFIDENCE_THRESHOLD = 0.92
REVIEW_CONFIDENCE_THRESHOLD = 0.80
logger = logging.getLogger(__name__)
GENERIC_OWNER_WORDS = {
    "JR",
    "SR",
    "II",
    "III",
    "IV",
    "TRUST",
    "TR",
    "ET",
    "AL",
    "LIVING",
    "REVOCABLE",
    "FAMILY",
    "THE",
}

PROPERTY_TYPE_CASE_SQL = """
CASE
    WHEN dor_uc IS NULL THEN NULL
    WHEN printf('%02d', CAST(dor_uc AS INTEGER)) = '01' THEN 'Single Family'
    WHEN printf('%02d', CAST(dor_uc AS INTEGER)) = '02' THEN 'Mobile Home'
    WHEN printf('%02d', CAST(dor_uc AS INTEGER)) = '04' THEN 'Condo'
    WHEN printf('%02d', CAST(dor_uc AS INTEGER)) = '05' THEN 'Townhouse'
    WHEN printf('%02d', CAST(dor_uc AS INTEGER)) = '06' THEN 'Cooperative'
    WHEN printf('%02d', CAST(dor_uc AS INTEGER)) = '07' THEN 'Vacant'
    WHEN printf('%02d', CAST(dor_uc AS INTEGER)) = '08' THEN 'Multi Family'
    ELSE 'Other'
END
""".strip()


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

    if property_type is not None and property_type not in APPROVED_PROPERTY_TYPES:
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


def normalize_owner_name(value):
    cleaned = re.sub(r"[^A-Z0-9 ]+", " ", str(value or "").upper())
    tokens = [token for token in cleaned.split() if token and token not in GENERIC_OWNER_WORDS]
    return tokens


def owner_match_score(input_owner_name, candidate_owner_name):
    input_tokens = normalize_owner_name(input_owner_name)
    candidate_tokens = normalize_owner_name(candidate_owner_name)
    if not input_tokens or not candidate_tokens:
        return 0.0

    input_set = set(input_tokens)
    candidate_set = set(candidate_tokens)
    overlap = input_set & candidate_set
    if not overlap:
        return 0.0

    ratio = len(overlap) / max(1, min(len(input_set), len(candidate_set)))
    last_name_bonus = 0.2 if input_tokens[-1] in candidate_set else 0.0
    first_name_bonus = 0.1 if input_tokens[0] in candidate_set else 0.0
    return min(1.0, round(ratio + last_name_bonus + first_name_bonus, 4))


def calculate_match_confidence(parsed, candidate_address, method):
    normalized_input = (parsed.canonical + " " + parsed.zip_code).strip()
    candidate_full = candidate_address or ""
    candidate_parts = split_normalized_address(candidate_full)
    input_street = parsed.canonical_street or ""
    candidate_street = candidate_parts["street"]
    house_exact = bool(parsed.house_number and candidate_street.startswith(parsed.house_number + " "))
    street_similarity = SequenceMatcher(None, input_street, candidate_street).ratio()

    if method == "exact_normalized":
        return 1.0

    candidate_core = normalized_core(candidate_full)
    input_core = parsed.canonical
    core_similarity = SequenceMatcher(None, input_core, candidate_core).ratio()
    full_similarity = SequenceMatcher(None, normalized_input, candidate_full).ratio()
    house_bonus = 0.08 if house_exact else -0.12
    zip_bonus = 0.05 if parsed.zip_code and candidate_full.endswith(" " + parsed.zip_code) else 0.0
    confidence = (core_similarity * 0.45) + (full_similarity * 0.2) + (street_similarity * 0.25) + house_bonus + zip_bonus
    if not house_exact:
        confidence = min(confidence, 0.89)
    return min(0.99, round(confidence, 4))


def salient_street_tokens(parsed):
    tokens = []
    for token in parsed.street_tokens[1:]:
        if token in {"N", "S", "E", "W", "NE", "NW", "SE", "SW", "ST", "AVE", "TER", "RD", "DR", "CT", "PL", "LN", "BLVD", "HWY", "PKWY", "WAY", "TRL", "ALY", "CIR"}:
            continue
        if token not in tokens:
            tokens.append(token)
    return tokens[:2]


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
        "owner_name",
        "city",
        "zip",
    ]


def build_select_map(available_columns):
    if "property_type_label" in available_columns:
        property_type_select = "property_type_label"
    elif "property_type" in available_columns:
        property_type_select = "property_type AS property_type_label"
    elif "dor_uc" in available_columns:
        property_type_select = PROPERTY_TYPE_CASE_SQL + " AS property_type_label"
    else:
        property_type_select = "NULL AS property_type_label"

    return {
        "parcel_id": "parcel_id",
        "normalized_address": "normalized_address",
        "year_built": "year_built",
        "homestead_flag": "homestead_flag" if "homestead_flag" in available_columns else "homestead_exemption AS homestead_flag",
        "property_value": "property_value",
        "county_source": "county_source" if "county_source" in available_columns else "county AS county_source",
        "property_type_label": property_type_select,
        "owner_name": "owner_name" if "owner_name" in available_columns else "NULL AS owner_name",
        "city": "city" if "city" in available_columns else "NULL AS city",
        "zip": "zip" if "zip" in available_columns else "NULL AS zip",
    }


def enrich_runtime_row(row):
    row.setdefault("street_address", None)
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
        {property_type_label},
        {owner_name},
        {city},
        {zip}
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
    cursor = conn.cursor()

    base_query = """
    SELECT
        {parcel_id},
        {normalized_address},
        {year_built},
        {homestead_flag},
        {property_value},
        {county_source},
        {property_type_label},
        {owner_name},
        {city},
        {zip}
    FROM properties
    WHERE 1 = 1
    """.format(**build_select_map(available_columns))

    query_specs = []

    primary_filters = []
    primary_params = []
    if parsed.house_number:
        primary_filters.append("AND normalized_address LIKE ?")
        primary_params.append(parsed.house_number + " %")
    if parsed.zip_code:
        primary_filters.append("AND normalized_address LIKE ?")
        primary_params.append("% " + parsed.zip_code)
    elif parsed.canonical_city:
        primary_filters.append("AND normalized_address LIKE ?")
        primary_params.append("% " + parsed.canonical_city + " %")
    query_specs.append((primary_filters, primary_params, MAX_CANDIDATES))

    expanded_filters = []
    expanded_params = []
    if parsed.zip_code:
        expanded_filters.append("AND normalized_address LIKE ?")
        expanded_params.append("% " + parsed.zip_code)
    elif parsed.canonical_city:
        expanded_filters.append("AND normalized_address LIKE ?")
        expanded_params.append("% " + parsed.canonical_city + " %")

    for token in salient_street_tokens(parsed):
        expanded_filters.append("AND normalized_address LIKE ?")
        expanded_params.append("%" + token + "%")

    if expanded_filters:
        query_specs.append((expanded_filters, expanded_params, MAX_CANDIDATES))

    rows_by_address = {}
    for filters, params, limit in query_specs:
        query = base_query + "\n".join(filters) + "\nLIMIT ?"
        cursor.execute(query, [*params, limit])
        for row in cursor.fetchall():
            property_row = enrich_runtime_row(row_to_property(columns, row))
            rows_by_address[property_row["normalized_address"]] = property_row
            if len(rows_by_address) >= MAX_CANDIDATES:
                break
        if len(rows_by_address) >= MAX_CANDIDATES:
            break

    return list(rows_by_address.values())


def score_rows(parsed, rows, homeowner_name=None):
    scored = []
    input_with_zip = (parsed.canonical + " " + parsed.zip_code).strip()

    for row in rows:
        canonical_db = row["normalized_address"]
        candidate = split_normalized_address(canonical_db)
        street_tokens = candidate["street_tokens"]
        city_tokens = candidate["city_tokens"]
        candidate_street = candidate["street"]
        candidate_city = candidate["city"]
        candidate_zip = candidate["zip_code"]
        house_exact = bool(parsed.house_number and street_tokens and parsed.house_number == street_tokens[0])
        street_body_exact = bool(parsed.street_tokens[1:] and parsed.street_tokens[1:] == street_tokens[1:])
        owner_score = owner_match_score(homeowner_name, row.get("owner_name"))

        score = score_candidate(parsed.street_tokens or parsed.tokens, street_tokens, city_tokens)

        street_exact = bool(parsed.canonical_street and parsed.canonical_street == candidate_street)
        city_exact = bool(parsed.canonical_city and parsed.canonical_city == candidate_city)
        zip_exact = bool(parsed.zip_code and parsed.zip_code == candidate_zip)
        full_exact = bool(input_with_zip and canonical_db == input_with_zip)

        if zip_exact:
            score += 150

        if city_exact:
            score += 250

        if house_exact:
            score += 425

        if street_body_exact:
            score += 350

        if street_exact:
            score += 500

        if street_exact and city_exact:
            score += 400

        if full_exact:
            score += 100

        score += int(owner_score * 180)

        scored.append(
            {
                "row": row,
                "score": score,
                "canonical_full": canonical_db,
                "canonical_core": normalized_core(canonical_db),
                "street_tokens": street_tokens,
                "city_tokens": city_tokens,
                "candidate_street": candidate_street,
                "candidate_city": candidate_city,
                "candidate_zip": candidate_zip,
                "house_exact": house_exact,
                "street_body_exact": street_body_exact,
                "street_exact": street_exact,
                "city_exact": city_exact,
                "zip_exact": zip_exact,
                "full_exact": full_exact,
                "owner_score": owner_score,
            }
        )

    scored.sort(key=lambda item: (-item["score"], item["canonical_full"]))
    return scored


def log_ambiguous_match(parsed, reason, candidates):
    if not logger.isEnabledFor(logging.DEBUG):
        return

    logger.debug(
        "Ambiguous property match: reason=%s input=%s parsed_street=%s parsed_city=%s parsed_state=%s parsed_zip=%s candidates=%s",
        reason,
        parsed.full_address,
        parsed.canonical_street,
        parsed.canonical_city,
        parsed.state,
        parsed.zip_code,
        [
            {
                "address": item["canonical_full"],
                "score": item["score"],
                "street_exact": item["street_exact"],
                "city_exact": item["city_exact"],
                "zip_exact": item["zip_exact"],
            }
            for item in candidates[:5]
        ],
    )


def choose_best_match(parsed, rows, homeowner_name=None):
    if not rows:
        return None, "Property not found", 0.0, "no_match"

    scored = score_rows(parsed, rows, homeowner_name=homeowner_name)
    input_with_zip = (parsed.canonical + " " + parsed.zip_code).strip()
    input_core = parsed.canonical

    exact_matches = [item for item in scored if item["canonical_full"] == input_with_zip]
    if len(exact_matches) == 1:
        return exact_matches[0]["row"], None, 1.0, "exact_normalized"
    if len(exact_matches) > 1:
        log_ambiguous_match(parsed, "multiple exact normalized matches", exact_matches)
        return None, "Ambiguous property match", 1.0, "ambiguous"

    component_exact_matches = [
        item for item in scored
        if item["street_exact"] and item["city_exact"] and item["zip_exact"]
    ]
    if len(component_exact_matches) == 1:
        return component_exact_matches[0]["row"], None, 0.99, "exact_components"
    if len(component_exact_matches) > 1:
        log_ambiguous_match(parsed, "multiple street+city+zip matches", component_exact_matches)
        return None, "Ambiguous property match", 0.99, "ambiguous"

    street_city_matches = [
        item for item in scored
        if item["street_exact"] and item["city_exact"]
    ]
    if len(street_city_matches) == 1:
        confidence = calculate_match_confidence(parsed, street_city_matches[0]["row"]["normalized_address"], "fuzzy")
        confidence = min(0.99, round(confidence + (street_city_matches[0]["owner_score"] * 0.05), 4))
        return street_city_matches[0]["row"], None, confidence, "component_match"
    if len(street_city_matches) > 1:
        log_ambiguous_match(parsed, "multiple street+city matches", street_city_matches)
        return None, "Ambiguous property match", 0.0, "ambiguous"

    street_zip_matches = [
        item for item in scored
        if item["street_exact"] and item["zip_exact"]
    ]
    if len(street_zip_matches) == 1:
        confidence = calculate_match_confidence(parsed, street_zip_matches[0]["row"]["normalized_address"], "fuzzy")
        confidence = min(0.99, round(confidence + (street_zip_matches[0]["owner_score"] * 0.05), 4))
        return street_zip_matches[0]["row"], None, confidence, "component_match"
    if len(street_zip_matches) > 1:
        log_ambiguous_match(parsed, "multiple street+zip matches", street_zip_matches)
        return None, "Ambiguous property match", 0.0, "ambiguous"

    exact_core_matches = [item for item in scored if item["canonical_core"] == input_core]
    if len(exact_core_matches) == 1:
        confidence = calculate_match_confidence(parsed, exact_core_matches[0]["row"]["normalized_address"], "fuzzy")
        confidence = min(0.99, round(confidence + (exact_core_matches[0]["owner_score"] * 0.05), 4))
        return exact_core_matches[0]["row"], None, confidence, "fuzzy"
    if len(exact_core_matches) > 1:
        log_ambiguous_match(parsed, "multiple exact core matches", exact_core_matches)
        return None, "Ambiguous property match", 0.0, "ambiguous"

    best = scored[0]
    confidence = calculate_match_confidence(parsed, best["row"]["normalized_address"], "fuzzy")
    confidence = min(0.99, round(confidence + (best["owner_score"] * 0.05), 4))
    if best["score"] < 500 or confidence < REVIEW_CONFIDENCE_THRESHOLD:
        return None, "Property not found", confidence, "fuzzy"

    ties = [item for item in scored if item["score"] == best["score"]]
    if len(ties) > 1:
        log_ambiguous_match(parsed, "top score tie", ties)
        return None, "Ambiguous property match", confidence, "ambiguous"

    if len(scored) > 1 and best["score"] - scored[1]["score"] < 80:
        log_ambiguous_match(parsed, "top two scores too close", scored[:2])
        return None, "Ambiguous property match", confidence, "ambiguous"

    if not best["house_exact"] and confidence < HIGH_CONFIDENCE_THRESHOLD:
        return None, "Ambiguous property match", confidence, "ambiguous"

    return best["row"], None, confidence, "fuzzy"


def derive_output_parse(parsed, property_row):
    if property_row:
        return property_row["normalized_address"], ""

    return parsed.display_core, ""


def lookup_in_database(parsed, db_path, homeowner_name=None):
    normalized_input = (parsed.canonical + " " + parsed.zip_code).strip()
    conn = sqlite3.connect(str(db_path))
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
            property_row, lookup_error, match_confidence, match_method = choose_best_match(
                parsed,
                candidate_rows,
                homeowner_name=homeowner_name,
            )

        owner_names = (
            fetch_related_owners(conn, property_row["normalized_address"], available_columns)
            if property_row
            else []
        )
    finally:
        conn.close()

    return {
        "property_row": property_row,
        "lookup_error": lookup_error,
        "match_confidence": match_confidence,
        "match_method": match_method,
        "owner_names": owner_names,
    }


def iter_fallback_db_paths(primary_db_path):
    seen = set()
    for candidate in (primary_db_path, DIAGNOSTIC_DB_PATH, SOURCE_DB_PATH):
        if not candidate:
            continue
        candidate = Path(candidate).expanduser()
        if candidate in seen or not candidate.exists():
            continue
        seen.add(candidate)
        yield candidate


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


def lookup_property(full_address, db_path=None, homeowner_name=None):
    parsed = parse_input_address(full_address)
    resolved_db_path = Path(db_path).expanduser() if db_path else DB_PATH
    normalized_input = (parsed.canonical + " " + parsed.zip_code).strip()

    if not resolved_db_path.exists():
        raise FileNotFoundError("SQLite database not found: {0}".format(resolved_db_path))

    result = None
    property_row = None
    lookup_error = "Property not found"
    match_confidence = 0.0
    match_method = "normalize_only"
    owner_name = None
    owner_names = []

    for candidate_db_path in iter_fallback_db_paths(resolved_db_path):
        try:
            result = lookup_in_database(parsed, candidate_db_path, homeowner_name=homeowner_name)
        except sqlite3.Error as exc:
            logger.warning("Skipping unreadable lookup database %s: %s", candidate_db_path, exc)
            continue

        property_row = result["property_row"]
        lookup_error = result["lookup_error"]
        match_confidence = result["match_confidence"]
        match_method = result["match_method"]
        owner_name = property_row.get("owner_name") if property_row else None
        owner_names = result["owner_names"]

        if property_row:
            break

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
            "owner_name": None,
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
        "owner_name": owner_name,
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
        print('python3 scripts/verification/lookup_and_decide.py "234 Garden Grove Parkway, Vero Beach FL 32962"')
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
