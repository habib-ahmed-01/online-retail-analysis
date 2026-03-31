"""
03_load.py
==========
Loads the cleaned CSV into a MySQL database using SQLAlchemy + Pandas.
Creates the target table, writes all rows, then creates indexes for
query performance.

Prerequisites:
  pip install sqlalchemy mysql-connector-python pandas

MySQL setup (run once in MySQL shell):
  CREATE DATABASE online_retail;
  CREATE USER 'retail_user'@'localhost' IDENTIFIED BY 'retail_pass';
  GRANT ALL PRIVILEGES ON online_retail.* TO 'retail_user'@'localhost';
  FLUSH PRIVILEGES;


"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

# ── Configuration — update these to match your MySQL setup ───────────────────
DB_HOST    = "localhost"
DB_PORT    = 3306
DB_NAME    = "online_retail"
DB_USER    = "root"
DB_PASS    = "secret"
TABLE_NAME = "online_retail_clean"

DATA_DIR   = "data"
CLEANED_CSV = os.path.join(DATA_DIR, "online_retail_clean.csv")

# Chunk size for to_sql() — reduces memory pressure for large tables
CHUNK_SIZE = 5000


# ── Helper ────────────────────────────────────────────────────────────────────
def banner(title):
    print(f"\n{'─'*55}")
    print(f"  {title}")
    print(f"{'─'*55}")


# ── Step 1: Load cleaned CSV ──────────────────────────────────────────────────
def load_csv() -> pd.DataFrame:
    banner("STEP 1 — Loading cleaned CSV")
    if not os.path.exists(CLEANED_CSV):
        raise FileNotFoundError(
            f"[ERROR] Cleaned CSV not found: '{CLEANED_CSV}'\n"
            f"        Please run 02_clean.py first."
        )

    # Specify dtypes so Pandas doesn't guess incorrectly on reload
    dtype_map = {
        "InvoiceNo"    : str,
        "StockCode"    : str,
        "Description"  : str,
        "Quantity"     : "int32",
        "UnitPrice"    : "float64",
        "CustomerID"   : str,
        "Country"      : str,
        "TotalLineValue": "float64",
        "Year"         : "int16",
        "Month"        : "int8",
        "MonthName"    : str,
        "DayOfWeek"    : str,
        "Hour"         : "int8",
        "WeekNo"       : "int8",
    }
    df = pd.read_csv(
        CLEANED_CSV,
        dtype=dtype_map,
        parse_dates=["InvoiceDate"]
    )
    print(f"[OK]   Loaded {len(df):,} rows  ×  {df.shape[1]} columns from CSV")
    return df


# ── Step 2: Create MySQL engine ───────────────────────────────────────────────
def create_mysql_engine():
    banner("STEP 2 — Connecting to MySQL")
    connection_string = (
        f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    engine = create_engine(connection_string, echo=False)

    # Test connection
    with engine.connect() as conn:
        result = conn.execute(text("SELECT VERSION()"))
        version = result.fetchone()[0]
    print(f"[OK]   Connected to MySQL {version}  →  database: '{DB_NAME}'")
    return engine


# ── Step 3: Write DataFrame to MySQL ─────────────────────────────────────────
def write_to_mysql(df: pd.DataFrame, engine):
    banner("STEP 3 — Writing data to MySQL")
    total_rows = len(df)
    print(f"[INFO] Writing {total_rows:,} rows to table '{TABLE_NAME}' in chunks of {CHUNK_SIZE:,} ...")

    df.to_sql(
        name      = TABLE_NAME,
        con       = engine,
        if_exists = "replace",    # drop & recreate table each run
        index     = False,        # don't write the Pandas index as a column
        chunksize = CHUNK_SIZE,
        method    = "multi",      # multi-row INSERT for speed
    )
    print(f"[OK]   {total_rows:,} rows written to '{TABLE_NAME}'")


# ── Step 4: Create indexes for query performance ──────────────────────────────
def create_indexes(engine):
    banner("STEP 4 — Creating MySQL indexes")

    indexes = {
        "idx_customer"   : "CustomerID",
        "idx_invoice"    : "InvoiceNo",
        "idx_invoice_date": "InvoiceDate",
        "idx_stock"      : "StockCode",
        "idx_country"    : "Country",
    }

    with engine.connect() as conn:
        for idx_name, col in indexes.items():
            try:
                conn.execute(text(
                    f"CREATE INDEX {idx_name} ON {TABLE_NAME} ({col})"
                ))
                conn.commit()
                print(f"[OK]   Index '{idx_name}'  on column '{col}'")
            except Exception as e:
                # Index may already exist on re-run
                print(f"[SKIP] Index '{idx_name}' — {e}")


# ── Step 5: Verify row count in MySQL ────────────────────────────────────────
def verify_load(engine):
    banner("STEP 5 — Verifying load")
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}"))
        count  = result.fetchone()[0]

        result2 = conn.execute(text(
            f"SELECT MIN(InvoiceDate), MAX(InvoiceDate), "
            f"COUNT(DISTINCT CustomerID), COUNT(DISTINCT StockCode) "
            f"FROM {TABLE_NAME}"
        ))
        row = result2.fetchone()

    print(f"[OK]   Row count in MySQL  : {count:,}")
    print(f"[OK]   Date range          : {row[0]}  →  {row[1]}")
    print(f"[OK]   Unique customers    : {row[2]:,}")
    print(f"[OK]   Unique products     : {row[3]:,}")

    print(f"\n{'='*55}")
    print(f"  Table '{TABLE_NAME}' loaded successfully.")
    print(f"  Next step → run SQL queries in analysis.sql")
    print(f"              then: python 04_visualise.py")
    print(f"{'='*55}\n")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df     = load_csv()
    engine = create_mysql_engine()
    write_to_mysql(df, engine)
    create_indexes(engine)
    verify_load(engine)
