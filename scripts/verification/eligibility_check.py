import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "florida_property_lookup.db"

address = "7605 NW 4 PL 107 MARGATE 33063"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

query = """
SELECT owner_name,
       year_built,
       property_type_label,
       homestead_flag,
       property_value,
       dor_uc
FROM properties
WHERE normalized_address = ?
LIMIT 1
"""

cursor.execute(query, (address,))
row = cursor.fetchone()

if not row:
    print("Property not found")
    raise SystemExit

owner, year, ptype, homestead, value, dor_uc = row

decision = "PASS"
reason = "Eligible"

approved_types = {"Single Family", "Townhouse"}

if homestead == 0:
    decision = "FAIL"
    reason = "Not homestead property"

elif year and year > 2008:
    decision = "FAIL"
    reason = "Home built after 2008"

elif value and value > 700000:
    decision = "FAIL"
    reason = "Property value exceeds $700,000"

elif ptype not in approved_types:
    decision = "FAIL"
    reason = f"Property type not eligible: {ptype}"

result = {
    "owner": owner,
    "year_built": year,
    "property_type": ptype,
    "dor_uc": dor_uc,
    "homestead": homestead,
    "property_value": value,
    "decision": decision,
    "reason": reason
}

print(result)

conn.close()

