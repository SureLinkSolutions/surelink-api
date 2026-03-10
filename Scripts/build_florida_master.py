from pathlib import Path
import pandas as pd
from address_matching import canonicalize_component, canonicalize_full_address

BASE_DIR = Path("/Users/ericbrown/Google Drive/Shared drives/My Safe Florida Home/Property Intelligence/States/Florida")

RAW_DIR = BASE_DIR / "Raw County Data"
CLEAN_DIR = BASE_DIR / "Cleaned County Data"
MASTER_DIR = BASE_DIR / "Master Lookup"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)
MASTER_DIR.mkdir(parents=True, exist_ok=True)

KEEP_COLS = [
    "PARCEL_ID",
    "PHY_ADDR1",
    "PHY_ADDR2",
    "PHY_CITY",
    "PHY_ZIPCD",
    "OWN_NAME",
    "ACT_YR_BLT",
    "DOR_UC",
    "JV",
    "EXMPT_01",
    "EXMPT_02",
]

DOR_UC_MAP = {
    "01": "Single Family",
    "02": "Mobile Home",
    "04": "Condo",
    "05": "Townhouse",
    "06": "Cooperative",
    "07": "Vacant",
    "08": "Multi Family",
}

def normalize_text(value: str, city_mode: bool = False) -> str:
    if pd.isna(value):
        return ""

    return canonicalize_component(value, city_mode=city_mode)

def build_street_address(addr1, addr2):
    a1 = "" if pd.isna(addr1) else str(addr1).strip()
    a2 = "" if pd.isna(addr2) else str(addr2).strip()
    return f"{a1} {a2}".strip()

def build_normalized_address(row):
    street = normalize_text(build_street_address(row["PHY_ADDR1"], row["PHY_ADDR2"]))
    city = normalize_text(row["PHY_CITY"], city_mode=True)
    zip_code = "" if pd.isna(row["PHY_ZIPCD"]) else str(row["PHY_ZIPCD"]).split(".")[0].strip()
    return canonicalize_full_address(street, city, zip_code)

def to_num(series):
    return pd.to_numeric(series, errors="coerce").fillna(0)

all_frames = []

for county_folder in RAW_DIR.iterdir():
    if not county_folder.is_dir():
        continue

    csv_files = list(county_folder.glob("*.csv"))
    if not csv_files:
        continue

    csv_path = csv_files[0]
    county_name = county_folder.name

    print(f"Processing {county_name}: {csv_path.name}")

    df = pd.read_csv(csv_path, dtype=str, low_memory=False)

    missing = [c for c in KEEP_COLS if c not in df.columns]
    if missing:
        print(f"Skipping {county_name} - missing columns: {missing}")
        continue

    df = df[KEEP_COLS].copy()

    df["street_address"] = df.apply(
        lambda r: build_street_address(r["PHY_ADDR1"], r["PHY_ADDR2"]),
        axis=1
    )
    df["normalized_address"] = df.apply(build_normalized_address, axis=1)

    df["dor_uc_clean"] = (
        pd.to_numeric(df["DOR_UC"], errors="coerce")
        .fillna(0)
        .astype(int)
        .astype(str)
        .str.zfill(2)
    )

    df["property_type_label"] = df["dor_uc_clean"].map(DOR_UC_MAP).fillna("Other")

    ex1 = to_num(df["EXMPT_01"])
    ex2 = to_num(df["EXMPT_02"])
    df["homestead_flag"] = ((ex1 > 0) | (ex2 > 0)).astype(int)

    df["year_built"] = pd.to_numeric(df["ACT_YR_BLT"], errors="coerce")
    df["property_value"] = pd.to_numeric(df["JV"], errors="coerce")
    df["county_source"] = county_name

    cleaned = df[
        [
            "PARCEL_ID",
            "street_address",
            "normalized_address",
            "PHY_CITY",
            "PHY_ZIPCD",
            "OWN_NAME",
            "year_built",
            "dor_uc_clean",
            "property_type_label",
            "homestead_flag",
            "property_value",
            "county_source",
        ]
    ].rename(
        columns={
            "PARCEL_ID": "parcel_id",
            "PHY_CITY": "city",
            "PHY_ZIPCD": "zip",
            "OWN_NAME": "owner_name",
            "dor_uc_clean": "dor_uc",
        }
    )

    cleaned_path = CLEAN_DIR / f"{county_name.lower()}_cleaned.csv"
    cleaned.to_csv(cleaned_path, index=False)

    all_frames.append(cleaned)

if not all_frames:
    raise RuntimeError("No county files were processed. Check your folder structure and CSV files.")

master_df = pd.concat(all_frames, ignore_index=True).drop_duplicates(
    subset=["normalized_address", "parcel_id"]
)

master_path = MASTER_DIR / "florida_property_master.csv"
master_df.to_csv(master_path, index=False)

print(f"\nDone.")
print(f"Master rows: {len(master_df):,}")
print(f"Master file: {master_path}")
