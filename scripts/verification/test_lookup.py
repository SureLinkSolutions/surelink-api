import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "florida_property_lookup.db"

normalized_address = "7605 NW 4 PL 107 MARGATE 33063"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

query = """
SELECT parcel_id, owner_name, year_built, property_type_label, homestead_flag, property_value, county_source
FROM properties
WHERE normalized_address = ?
LIMIT 5
"""

cursor.execute(query, (normalized_address,))
rows = cursor.fetchall()

for row in rows:
    print(row)

conn.close()

