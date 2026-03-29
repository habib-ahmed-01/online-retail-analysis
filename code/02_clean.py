"""
02_clean.py
==============
Cleans the Online Retail Dataset

Steps:
- Load raw Excel file
- Identify missing values
- Remove missing values
- Convert date column
- Extract Year, Month, Day
- Save cleaned dataset

"""

import os
import pandas as pd

# ── Configuration ─────────────────────────────────────────────
DATA_DIR = "data"
RAW_FILE = os.path.join(DATA_DIR, "Online Retail.xlsx")
CLEAN_FILE = os.path.join(DATA_DIR, "cleaned_retail.xlsx")


# ── Load Data ────────────────────────────────────────────────
def load_data():
    print("\n[INFO] Loading dataset...")
    df = pd.read_excel(RAW_FILE)
    print(f"[OK] Loaded dataset with shape: {df.shape}")
    return df


# ── Check Missing Values ─────────────────────────────────────
def check_missing(df):
    print("\n[INFO] Checking missing values...")
    missing = df.isnull().sum()
    print(missing[missing > 0])  # show only columns with missing values


# ── Clean Data ───────────────────────────────────────────────
def clean_data(df):
    print("\n[INFO] Removing missing values...")
    df = df.dropna()
    print(f"[OK] After removing missing values: {df.shape}")

    return df


# ── Feature Engineering (Date Split) ─────────────────────────
def process_dates(df):
    print("\n[INFO] Processing date column...")

    # Convert to datetime
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

    # Create new columns
    df['Year'] = df['InvoiceDate'].dt.year
    df['Month'] = df['InvoiceDate'].dt.month
    df['Day'] = df['InvoiceDate'].dt.day

    print("[OK] Date columns created: Year, Month, Day")

    return df


# ── Save Clean Data ──────────────────────────────────────────
def save_data(df):
    df.to_excel(CLEAN_FILE, index=False)
    print(f"\n[COMPLETE] Cleaned data saved to: {CLEAN_FILE}")


# ── Main ─────────────────────────────────────────────────────
if __name__ == "__main__":
    df = load_data()
    check_missing(df)
    df = clean_data(df)
    df = process_dates(df)
    save_data(df)
