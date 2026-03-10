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

STATE_WORDS = {"FL", "FLORIDA"}
UNIT_WORDS = {"APT", "UNIT", "STE", "SUITE"}


@dataclass
class ParsedAddress:
    full_address: str
    zip_code: str
    house_number: str
    tokens: list[str]
    canonical: str
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
    tokens = []
    for token in _clean_text(value).split():
        canonical = _canonicalize_token(token, city_mode=city_mode)
        if canonical:
            tokens.append(canonical)
    return " ".join(tokens)


def canonicalize_full_address(street, city, zip_code=""):
    parts = [canonicalize_component(street), canonicalize_component(city, city_mode=True)]
    if zip_code:
        parts.append(str(zip_code).split(".")[0].strip())
    return " ".join(part for part in parts if part).strip()


def parse_input_address(full_address):
    cleaned = _clean_text(full_address)
    zip_match = re.search(r"(\d{5})(?:-\d{4})?$", cleaned)
    zip_code = zip_match.group(1) if zip_match else ""

    if zip_code:
        cleaned = re.sub(r"(\d{5})(?:-\d{4})?$", "", cleaned).strip()

    tokens = cleaned.split()
    while tokens and tokens[-1] in STATE_WORDS:
        tokens.pop()

    canonical_tokens = []
    for token in tokens:
        canonical = _canonicalize_token(token, city_mode=False)
        if canonical:
            canonical_tokens.append(canonical)

    house_number = canonical_tokens[0] if canonical_tokens else ""
    display_core = " ".join(tokens)
    canonical = " ".join(canonical_tokens)

    return ParsedAddress(
        full_address=full_address,
        zip_code=zip_code,
        house_number=house_number,
        tokens=canonical_tokens,
        canonical=canonical,
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
