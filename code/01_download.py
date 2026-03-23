"""
01_download.py
==============
Downloads the UCI Online Retail Dataset programmatically.
Saves the raw file to the /data directory

Usage:
    python 01_download.py
"""

import os
import zipfile
import urllib.request
import shutil

# ── Configuration ────────────────────────────────────────────────────────────
DATA_DIR     = "data"
RAW_ZIP      = os.path.join(DATA_DIR, "online_retail.zip")
RAW_EXCEL    = os.path.join(DATA_DIR, "Online Retail.xlsx")
DATASET_URL  = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"

# --- Makes data directory if does not exist -------------
def make_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"\n{'='*50}")
    print(f"[INFO] Data directory ready: {DATA_DIR}/")
    print(f"\n{'='*50}")


# --- Download ZIP file --------------------------------------------------------
def download_zip():
    if os.path.exists(RAW_EXCEL):
        print(f"[SKIP] Dataset already present at '{RAW_EXCEL}'. Delete it to re-download.")
        return

    print(f"[INFO] Downloading dataset from: {DATASET_URL}")
    try:
        # Download the dataset zip
        urllib.request.urlretrieve(DATASET_URL, RAW_ZIP)
        print(f"[OK]   Downloaded zip to '{RAW_ZIP}'")
        print(f"\n{'='*50}")
    except Exception as e:
        print(f"\n[ERROR] Download failed: {e}")
        raise


# ---- Extract ZIP file ---------------------------------
def extract_zip():
    if os.path.exists(RAW_EXCEL):
        return  # already extracted in a previous run

    print(f"[INFO] Extracting '{RAW_ZIP}' ...")
    with zipfile.ZipFile(RAW_ZIP, "r") as z:
        members = z.namelist()
        print(f"[INFO] Archive contains: {members}")
        z.extractall(DATA_DIR)

    # Move any extracted .xlsx to expected path if name differs
    for fname in os.listdir(DATA_DIR):
        if fname.endswith(".xlsx") and fname != "Online Retail.xlsx":
            src = os.path.join(DATA_DIR, fname)
            shutil.move(src, RAW_EXCEL)
            print(f"[INFO] Renamed '{fname}' → 'Online Retail.xlsx'")

    # Delete ZIP after extraction
    os.remove(RAW_ZIP)
    print(f"[OK]   Extracted Excel file to '{RAW_EXCEL}'")
    print(f"\n{'='*50}")


# --- Verify if excel downloaded and extracted -----------------
def verify():
    if not os.path.exists(RAW_EXCEL):
        raise FileNotFoundError(f"[ERROR] Expected file not found: '{RAW_EXCEL}'")

    print(f"[COMPLETE]   Raw dataset ready at: {RAW_EXCEL}")


# ── Main ─────────────────
if __name__ == "__main__":
    make_data_dir()
    download_zip()
    extract_zip()
    verify()
