import re
from dataclasses import dataclass
from difflib import SequenceMatcher


STREET_TYPE_ALIASES = {
    "ALLEY": "ALY",
    "ALLY": "ALY",
    "ALY": "ALY",
    "AVENUE": "AVE",
    "AVE": "AVE",
    "BOULEVARD": "BLVD",
    "BLVD": "BLVD",
    "CIRCLE": "CIR",
    "CIR": "CIR",
    "COURT": "CT",
    "CT": "CT",
    "DRIVE": "DR",
    "DR": "DR",
    "HIGHWAY": "HWY",
    "HWY": "HWY",
    "LANE": "LN",
    "LN": "LN",
    "PARKWAY": "PKWY",
    "PKWY": "PKWY",
    "PLACE": "PL",
    "PL": "PL",
    "ROAD": "RD",
    "RD": "RD",
    "STREET": "ST",
    "ST": "ST",
    "TERRACE": "TER",
    "TER": "TER",
    "TRAIL": "TRL",
    "TRL": "TRL",
    "WAY": "WAY",
}

CITY_ALIASES = {
    "FORT": "FT",
    "FT": "FT",
    "SAINT": "ST",
    "ST": "ST",
}

CITY_PHRASE_ALIASES = {
    ("ST", "PETE"): ("ST", "PETERSBURG"),
    ("SAINT", "PETE"): ("ST", "PETERSBURG"),
}

STATE_WORDS = {"FL", "FLORIDA"}
UNIT_WORDS = {"APT", "UNIT", "STE", "SUITE"}
DIRECTIONAL_WORDS = {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}


@dataclass
class ParsedAddress:
    full_address: str
    street: str
    city: str
    state: str
    zip_code: str
    house_number: str
    tokens: list[str]
    street_tokens: list[str]
    city_tokens: list[str]
    canonical: str
    canonical_street: str
    canonical_city: str
    display_core: str


def _clean_text(value):
    if not value:
        return ""

    s = str(value).upper().replace(",", " ").strip()
    s = re.sub(r"[^A-Z0-9# ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _canonicalize_token(token, city_mode=False):
    if not token:
        return ""

    if token in STATE_WORDS or token in UNIT_WORDS:
        return ""

    token = token.replace("#", "")
    if not token:
        return ""

    # Treat street ordinals consistently during matching: 20 and 20TH should compare equal.
    ordinal_match = re.fullmatch(r"(\d+)(?:ST|ND|RD|TH)", token)
    if ordinal_match:
        token = ordinal_match.group(1)

    if city_mode:
        return CITY_ALIASES.get(token, token)

    return STREET_TYPE_ALIASES.get(token, token)


def canonicalize_component(value, city_mode=False):
    raw_tokens = _clean_text(value).split()
    if city_mode:
        normalized_tokens = []
        index = 0
        while index < len(raw_tokens):
            phrase = tuple(raw_tokens[index:index + 2])
            if len(phrase) == 2 and phrase in CITY_PHRASE_ALIASES:
                normalized_tokens.extend(CITY_PHRASE_ALIASES[phrase])
                index += 2
                continue
            normalized_tokens.append(raw_tokens[index])
            index += 1
        raw_tokens = normalized_tokens

    tokens = []
    for token in raw_tokens:
        canonical = _canonicalize_token(token, city_mode=city_mode)
        if canonical:
            tokens.append(canonical)

    if city_mode and tokens:
        deduped = []
        for token in tokens:
            deduped.append(token)
            if len(deduped) >= 4 and deduped[-4:-2] == deduped[-2:]:
                deduped = deduped[:-2]
        tokens = deduped

    return " ".join(tokens)


def canonicalize_full_address(street, city, zip_code=""):
    parts = [canonicalize_component(street), canonicalize_component(city, city_mode=True)]
    if zip_code:
        parts.append(str(zip_code).split(".")[0].strip())
    return " ".join(part for part in parts if part).strip()


def _extract_zip_and_state(full_address):
    raw = str(full_address or "").upper().strip()
    raw = re.sub(r"\b(?:FL|FLORIDA)\b\s+\d{5}(?:-\d{4})?\b(?=\s+[A-Z])", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()

    zip_match = re.search(r"(\d{5})(?:-\d{4})?\s*$", raw)
    zip_code = zip_match.group(1) if zip_match else ""
    if zip_match:
        raw = raw[:zip_match.start()].strip(" ,")

    state = ""
    state_match = re.search(r"\b(FL|FLORIDA)\b\s*$", raw)
    if state_match:
        state = state_match.group(1)
        raw = raw[:state_match.start()].strip(" ,")

    return raw, zip_code, state


def _split_street_city(raw_without_zip_state):
    if not raw_without_zip_state:
        return "", ""

    comma_parts = [part.strip() for part in raw_without_zip_state.split(",") if part.strip()]
    if len(comma_parts) >= 2:
        return comma_parts[0], comma_parts[1]

    cleaned = _clean_text(raw_without_zip_state)
    tokens = cleaned.split()
    if not tokens:
        return "", ""

    street_type_values = set(STREET_TYPE_ALIASES.values()) | set(STREET_TYPE_ALIASES.keys())
    boundary = None
    for index, token in enumerate(tokens):
        if token in street_type_values:
            boundary = index + 1
            break

    if boundary is not None:
        while boundary < len(tokens) and tokens[boundary] in DIRECTIONAL_WORDS:
            boundary += 1
        while boundary < len(tokens) and re.search(r"\d", tokens[boundary]):
            boundary += 1
        street_tokens = tokens[:boundary]
        city_tokens = tokens[boundary:]
        return " ".join(street_tokens), " ".join(city_tokens)

    return " ".join(tokens), ""


def split_normalized_address(normalized_address):
    cleaned = _clean_text(normalized_address)
    tokens = cleaned.split()
    if not tokens:
        return {"street": "", "city": "", "zip_code": "", "street_tokens": [], "city_tokens": []}

    zip_code = tokens[-1] if re.fullmatch(r"\d{5}", tokens[-1]) else ""
    core_tokens = tokens[:-1] if zip_code else tokens

    street_type_values = set(STREET_TYPE_ALIASES.values())
    boundary = None
    for index, token in enumerate(core_tokens):
        if token in street_type_values:
            boundary = index + 1
            break

    if boundary is None:
        return {
            "street": " ".join(core_tokens),
            "city": "",
            "zip_code": zip_code,
            "street_tokens": core_tokens,
            "city_tokens": [],
        }

    while boundary < len(core_tokens) and core_tokens[boundary] in DIRECTIONAL_WORDS:
        boundary += 1

    while boundary < len(core_tokens) and re.search(r"\d", core_tokens[boundary]):
        boundary += 1

    street_tokens = core_tokens[:boundary]
    city_tokens = core_tokens[boundary:]
    canonical_street = canonicalize_component(" ".join(street_tokens))
    canonical_city = canonicalize_component(" ".join(city_tokens), city_mode=True)
    street_tokens = canonical_street.split() if canonical_street else []
    city_tokens = canonical_city.split() if canonical_city else []
    return {
        "street": canonical_street,
        "city": canonical_city,
        "zip_code": zip_code,
        "street_tokens": street_tokens,
        "city_tokens": city_tokens,
    }


def parse_input_address(full_address):
    raw_without_zip_state, zip_code, state = _extract_zip_and_state(full_address)
    street, city = _split_street_city(raw_without_zip_state)

    canonical_street = canonicalize_component(street)
    canonical_city = canonicalize_component(city, city_mode=True)
    street_tokens = canonical_street.split() if canonical_street else []
    city_tokens = canonical_city.split() if canonical_city else []
    canonical_tokens = street_tokens + city_tokens
    house_number = street_tokens[0] if street_tokens else ""
    display_core = " ".join(part for part in [street, city] if part).strip()
    canonical = " ".join(canonical_tokens)

    return ParsedAddress(
        full_address=full_address,
        street=street,
        city=city,
        state=state,
        zip_code=zip_code,
        house_number=house_number,
        tokens=canonical_tokens,
        street_tokens=street_tokens,
        city_tokens=city_tokens,
        canonical=canonical,
        canonical_street=canonical_street,
        canonical_city=canonical_city,
        display_core=display_core,
    )


def common_prefix_len(left, right):
    count = 0
    for l_token, r_token in zip(left, right):
        if l_token != r_token:
            break
        count += 1
    return count


def common_suffix_len(left, right):
    count = 0
    for l_token, r_token in zip(reversed(left), reversed(right)):
        if l_token != r_token:
            break
        count += 1
    return count


def score_candidate(input_tokens, street_tokens, city_tokens):
    candidate_tokens = street_tokens + city_tokens
    input_street_tokens = input_tokens
    city_exact = False

    if city_tokens and len(input_tokens) >= len(city_tokens):
        if input_tokens[-len(city_tokens):] == city_tokens:
            city_exact = True
            input_street_tokens = input_tokens[:-len(city_tokens)]

    score = 0
    if input_tokens == candidate_tokens:
        score += 1000

    if city_exact:
        score += 250
    else:
        score += 35 * len(set(input_tokens) & set(city_tokens))

    if input_street_tokens == street_tokens:
        score += 500
    elif input_street_tokens and street_tokens[:len(input_street_tokens)] == input_street_tokens:
        score += 320 - 15 * (len(street_tokens) - len(input_street_tokens))
    elif street_tokens and input_street_tokens[:len(street_tokens)] == street_tokens:
        score += 280 - 15 * (len(input_street_tokens) - len(street_tokens))
    else:
        score += 30 * len(set(input_street_tokens) & set(street_tokens))

    score += 30 * common_prefix_len(input_tokens, candidate_tokens)
    score += 25 * common_suffix_len(input_tokens, candidate_tokens)

    similarity = SequenceMatcher(None, " ".join(input_tokens), " ".join(candidate_tokens)).ratio()
    score += int(similarity * 200)

    return score
