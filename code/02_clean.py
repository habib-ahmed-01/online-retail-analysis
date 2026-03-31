"""
Loads the raw UCI Online Retail Excel file, performs all cleaning and
preprocessing steps using Pandas and NumPy, and saves the cleaned data
as a CSV ready for MySQL loading.

Steps performed:
  1. Load raw .xlsx
  2. Log initial shape and null counts (baseline report)
  3. Remove cancelled transactions  (InvoiceNo starts with 'C')
  4. Drop rows with null CustomerID
  5. Remove rows with Quantity <= 0 or UnitPrice <= 0
  6. Convert and standardise data types
  7. Feature engineering  (TotalLineValue, date parts)
  8. Remove duplicates
  9. Log final shape and null counts (post-clean report)
 10. Save cleaned CSV
"""

import os
import numpy as np
import pandas as pd

# ── Configuration ───────────────────
DATA_DIR       = "data"
RAW_EXCEL      = os.path.join(DATA_DIR, "Online Retail.xlsx")
CLEANED_CSV    = os.path.join(DATA_DIR, "online_retail_clean.csv")

def print_line():
    print("="*50)

# ── Step 1: Load ────────────────────
def load_raw() -> pd.DataFrame:
    print("STEP 1 — Loading raw dataset")
    if not os.path.exists(RAW_EXCEL):
        raise FileNotFoundError(
            f"[ERROR] Raw file not found: '{RAW_EXCEL}'\n"
            f"        Please run 01_download.py first."
        )
    print(f"[INFO] Reading '{RAW_EXCEL}'")
    df = pd.read_excel(RAW_EXCEL, dtype={"CustomerID": str, "InvoiceNo": str})
    print(f"[OK]   Loaded  →  {df.shape[0]:,} rows  ×  {df.shape[1]} columns")
    print_line()
    return df


# ── Step 2: Initial report ─────────────────
def baseline_report(df: pd.DataFrame):
    print("STEP 2 — Baseline data profile")
    print(f"{'Column':<20} {'Dtype':<15} {'Non-Null':>10} {'Null':>8} {'Null %':>8}")
    print("─" * 65)
    for col in df.columns:
        non_null = df[col].notna().sum()
        null     = df[col].isna().sum()
        pct      = null / len(df) * 100
        print(f"{col:<20} {str(df[col].dtype):<15} {non_null:>10,} {null:>8,} {pct:>7.1f}%")
    print_line()
    print(f"\n[INFO] Dtypes summary:\n{df.dtypes.value_counts().to_string()}")
    print_line()


# ── Step 3: Remove cancellations ──────────────────────────────────────────────
def remove_cancellations(df: pd.DataFrame) -> pd.DataFrame:
    print("STEP 3 — Removing cancelled transactions")
    mask      = df["InvoiceNo"].str.startswith("C", na=False)
    cancelled = mask.sum()
    df        = df[~mask].copy()
    print(f"[OK]   Removed {cancelled:,} cancelled rows  →  {len(df):,} rows remaining")
    print_line()
    return df


# ── Step 4: Drop null CustomerID ──────────────────────────────────────────────
def drop_null_customers(df: pd.DataFrame) -> pd.DataFrame:
    print("STEP 4 — Dropping rows with null CustomerID")
    before = len(df)
    df     = df.dropna(subset=["CustomerID"]).copy()
    dropped = before - len(df)
    print(f"[OK]   Dropped {dropped:,} rows  →  {len(df):,} rows remaining")
    print_line()
    return df


# ── Step 5: Filter invalid Quantity / UnitPrice ───────────────────────────────
def filter_invalid_values(df: pd.DataFrame) -> pd.DataFrame:
    print("STEP 5 — Filtering invalid Quantity and UnitPrice values")
    before = len(df)
    df     = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)].copy()
    removed = before - len(df)
    print(f"[OK]   Removed {removed:,} invalid rows  →  {len(df):,} rows remaining")
    print_line()
    return df


# ── Step 6: Data type standardisation ────────────────────────────────────────
def fix_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    print("STEP 6 — Standardising data types")

    # Numeric columns
    df["Quantity"]   = pd.to_numeric(df["Quantity"],   errors="coerce").astype(np.int32)
    df["UnitPrice"]  = pd.to_numeric(df["UnitPrice"],  errors="coerce").astype(np.float64)

    # DateTime
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")

    # String clean-up
    df["CustomerID"]  = df["CustomerID"].astype(str).str.strip().str.zfill(5)
    df["InvoiceNo"]   = df["InvoiceNo"].astype(str).str.strip()
    df["StockCode"]   = df["StockCode"].astype(str).str.strip().str.upper()
    df["Description"] = df["Description"].astype(str).str.strip().str.title()
    df["Country"]     = df["Country"].astype(str).str.strip()

    # Drop any rows where date parse failed
    null_dates = df["InvoiceDate"].isna().sum()
    if null_dates:
        print(f"[WARN] Dropping {null_dates} rows with unparseable dates")
        df = df.dropna(subset=["InvoiceDate"])

    print(f"[OK]   Dtypes standardised")
    print(df.dtypes.to_string())
    print_line()
    return df

# ── Step 7: Feature engineering ──────────────────────────────────────────────
def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    print("STEP 7 — Engineering new features")

    # Core revenue column
    df["TotalLineValue"] = (df["Quantity"] * df["UnitPrice"]).round(2)

    # Date-part breakdowns for SQL and visualisation
    df["Year"]      = df["InvoiceDate"].dt.year.astype(np.int16)
    df["Month"]     = df["InvoiceDate"].dt.month.astype(np.int8)
    df["MonthName"] = df["InvoiceDate"].dt.strftime("%b")      # Jan, Feb …
    df["DayOfWeek"] = df["InvoiceDate"].dt.day_name()           # Monday …
    df["Hour"]      = df["InvoiceDate"].dt.hour.astype(np.int8)
    df["WeekNo"]    = df["InvoiceDate"].dt.isocalendar().week.astype(np.int8)

    print(f"[OK]   New columns added: TotalLineValue, Year, Month, MonthName,")
    print(f"       DayOfWeek, Hour, WeekNo")
    print(f"\n[INFO] TotalLineValue sample stats:")
    print(df["TotalLineValue"].describe().round(2).to_string())
    print_line()
    return df


# ── Step 8: Remove duplicates ─────────────────────────────────────────────────
def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    ("STEP 8 — Removing duplicate rows")
    before = len(df)
    df     = df.drop_duplicates().copy()
    removed = before - len(df)
    print(f"[OK]   Removed {removed:,} exact duplicate rows  →  {len(df):,} rows remaining")
    print_line()
    return df


# ── Step 9: Post-clean report ─────────────────────────────────────────────────
def post_clean_report(df: pd.DataFrame):
    print("STEP 9 — Post-clean data profile")
    print(f"[INFO] Final shape: {df.shape[0]:,} rows  ×  {df.shape[1]} columns")
    null_totals = df.isnull().sum()
    if null_totals.sum() == 0:
        print("[OK]   No null values remain in the dataset.")
    else:
        print("[WARN] Remaining nulls:")
        print(null_totals[null_totals > 0].to_string())

    print(f"\n[INFO] Unique CustomerIDs : {df['CustomerID'].nunique():,}")
    print(f"[INFO] Unique InvoiceNos  : {df['InvoiceNo'].nunique():,}")
    print(f"[INFO] Unique StockCodes  : {df['StockCode'].nunique():,}")
    print(f"[INFO] Unique Countries   : {df['Country'].nunique():,}")
    print(f"[INFO] Date range         : {df['InvoiceDate'].min().date()}  →  {df['InvoiceDate'].max().date()}")
    print(f"[INFO] Total Revenue (£)  : £{df['TotalLineValue'].sum():,.2f}")
    print_line()



# ── Step 10: Save cleaned CSV ─────────────────────────────────────────────────
def save_csv(df: pd.DataFrame):
    print("STEP 10 — Saving cleaned dataset")
    df.to_csv(CLEANED_CSV, index=False)
    size_mb = os.path.getsize(CLEANED_CSV) / (1024 * 1024)
    print(f"[OK]   Saved to '{CLEANED_CSV}'  ({size_mb:.1f} MB,  {len(df):,} rows)")
    print(f"\n{'='*55}")
    print(f"  Cleaned CSV ready at: {CLEANED_CSV}")
    print(f"  Next step → run: python 03_load.py")
    print(f"{'='*55}\n")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df = load_raw()
    baseline_report(df)
    df = remove_cancellations(df)
    df = drop_null_customers(df)
    df = filter_invalid_values(df)
    df = fix_dtypes(df)
    df = engineer_features(df)
    df = remove_duplicates(df)
    post_clean_report(df)
    save_csv(df)
