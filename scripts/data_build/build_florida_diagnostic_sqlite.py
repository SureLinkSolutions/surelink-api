import sqlite3
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DB_PATH = PROJECT_ROOT / "data" / "florida_property_lookup.db"
DIAGNOSTIC_DB_PATH = PROJECT_ROOT / "data" / "florida_property_diagnostic.db"
TABLE_NAME = "properties"

APPROVED_PROPERTY_TYPES = ("Single Family", "Townhouse")
MAX_PROPERTY_VALUE = 700000
MAX_YEAR_BUILT = 2008


def get_available_columns(conn: sqlite3.Connection) -> set[str]:
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA main.table_info({TABLE_NAME})")
    return {row[1] for row in cursor.fetchall()}


def file_size_mb(path: Path) -> float:
    return path.stat().st_size / (1024 * 1024)


def main():
    if not SOURCE_DB_PATH.exists():
        raise FileNotFoundError(f"Source database not found: {SOURCE_DB_PATH}")

    DIAGNOSTIC_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DIAGNOSTIC_DB_PATH.exists():
        DIAGNOSTIC_DB_PATH.unlink()

    conn = sqlite3.connect(str(SOURCE_DB_PATH))
    try:
        source_columns = get_available_columns(conn)
        owner_name_select = "owner_name" if "owner_name" in source_columns else "NULL AS owner_name"

        conn.execute("ATTACH DATABASE ? AS diagnostic_db", (str(DIAGNOSTIC_DB_PATH),))
        conn.execute("PRAGMA diagnostic_db.journal_mode=OFF;")
        conn.execute("PRAGMA diagnostic_db.synchronous=OFF;")
        conn.execute("PRAGMA diagnostic_db.temp_store=MEMORY;")

        conn.execute(f"DROP TABLE IF EXISTS diagnostic_db.{TABLE_NAME}")
        conn.execute(
            f"""
            CREATE TABLE diagnostic_db.{TABLE_NAME} AS
            SELECT
                parcel_id,
                normalized_address,
                city,
                zip,
                {owner_name_select},
                year_built,
                homestead_flag,
                property_value,
                county_source,
                property_type_label
            FROM main.{TABLE_NAME}
            WHERE NOT (
                homestead_flag = 1
                AND property_type_label IN (?, ?)
                AND property_value IS NOT NULL
                AND property_value <= ?
                AND (year_built IS NULL OR year_built <= ?)
            )
            """.format(owner_name_select=owner_name_select),
            (*APPROVED_PROPERTY_TYPES, MAX_PROPERTY_VALUE, MAX_YEAR_BUILT),
        )
        conn.execute(
            f"""
            CREATE INDEX diagnostic_db.idx_normalized_address
            ON {TABLE_NAME}(normalized_address)
            """
        )
        conn.commit()

        row_count = conn.execute(
            f"SELECT COUNT(*) FROM diagnostic_db.{TABLE_NAME}"
        ).fetchone()[0]
        conn.execute("DETACH DATABASE diagnostic_db")
    finally:
        conn.close()

    diagnostic_conn = sqlite3.connect(str(DIAGNOSTIC_DB_PATH))
    try:
        diagnostic_conn.execute("VACUUM")
    finally:
        diagnostic_conn.close()

    print(f"Diagnostic database: {DIAGNOSTIC_DB_PATH}")
    print(f"Total rows written: {row_count:,}")
    print(f"Final file size: {file_size_mb(DIAGNOSTIC_DB_PATH):.2f} MB")


if __name__ == "__main__":
    main()
