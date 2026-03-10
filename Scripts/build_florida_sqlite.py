import sqlite3
from pathlib import Path
import pandas as pd

# Input CSV
MASTER_CSV = Path("/Users/ericbrown/Google Drive/Shared drives/My Safe Florida Home/Property Intelligence/States/Florida/Master Lookup/florida_property_master.csv")

# Output SQLite DB
DB_PATH = Path("/Users/ericbrown/SureLink/data/florida_property_lookup.db")

TABLE_NAME = "properties"
CHUNK_SIZE = 100_000

def main():
    if not MASTER_CSV.exists():
        raise FileNotFoundError(f"Master CSV not found: {MASTER_CSV}")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Faster bulk load settings
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute("PRAGMA temp_store=MEMORY;")
    cursor.execute("PRAGMA cache_size=-200000;")

    # Start fresh
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
    conn.commit()

    print(f"Loading CSV into SQLite from: {MASTER_CSV}")

    first_chunk = True
    total_rows = 0

    for chunk in pd.read_csv(MASTER_CSV, chunksize=CHUNK_SIZE, low_memory=False):
        chunk.to_sql(TABLE_NAME, conn, if_exists="replace" if first_chunk else "append", index=False)
        total_rows += len(chunk)
        first_chunk = False
        print(f"Loaded rows: {total_rows:,}")

    print("Creating indexes...")

    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_normalized_address ON {TABLE_NAME}(normalized_address);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_zip ON {TABLE_NAME}(zip);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_parcel_id ON {TABLE_NAME}(parcel_id);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_city ON {TABLE_NAME}(city);")
    conn.commit()

    # Quick sanity check
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
    row_count = cursor.fetchone()[0]

    print(f"\nDone.")
    print(f"Database: {DB_PATH}")
    print(f"Table: {TABLE_NAME}")
    print(f"Total rows in SQLite: {row_count:,}")

    conn.close()

if __name__ == "__main__":
    main()

