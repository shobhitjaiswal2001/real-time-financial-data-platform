"""
postgres_loader.py

Loads the Gold-layer Parquet data into PostgreSQL — INCREMENTALLY.

Only rows newer than what's already stored (per symbol) are upserted.
This avoids rewriting the entire rolling window every run, since Gold
recomputes all rows for correctness but most of them are unchanged.

Usage:
    python postgres_loader.py                # auto-finds latest gold parquet file
    python postgres_loader.py <path/to.parquet>   # loads a specific file (used by run_pipeline.py)
"""

import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


# ---------------------------------------------------------
# Project paths / env
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

ENV_FILE = PROJECT_ROOT / ".env"
load_dotenv(ENV_FILE)

GOLD_DIR = PROJECT_ROOT / "data" / "gold" / "daily_prices"


# ---------------------------------------------------------
# Resolve which Parquet file to load
# ---------------------------------------------------------

if len(sys.argv) > 1:
    PARQUET_FILE = Path(sys.argv[1])
else:
    if GOLD_DIR.is_file():
        PARQUET_FILE = GOLD_DIR
    else:
        matches = list(GOLD_DIR.glob("*.parquet"))
        if not matches:
            raise FileNotFoundError(f"No Gold parquet file found in: {GOLD_DIR}")
        PARQUET_FILE = max(matches, key=lambda f: f.stat().st_mtime)

if not PARQUET_FILE.exists():
    raise FileNotFoundError(f"Parquet file does not exist: {PARQUET_FILE}")


# ---------------------------------------------------------
# Database configuration (from .env, not hardcoded)
# ---------------------------------------------------------

DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", 2510)),
    "database": os.getenv("PG_DATABASE", "financial_data"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD"),
}

if not DB_CONFIG["password"]:
    raise ValueError(
        f"PG_PASSWORD not found in .env. Expected .env at: {ENV_FILE}"
    )


# ---------------------------------------------------------
# Read Gold data
# ---------------------------------------------------------

print(f"Reading Gold Parquet: {PARQUET_FILE}")

df = pd.read_parquet(PARQUET_FILE)

print(f"Rows read from Gold: {len(df)}")
print(f"Columns: {len(df.columns)}")

df["date"] = pd.to_datetime(df["date"]).dt.date


# ---------------------------------------------------------
# Connect to PostgreSQL
# ---------------------------------------------------------

print("Connecting to PostgreSQL...")

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()


# ---------------------------------------------------------
# INCREMENTAL FILTER
# Get the latest stored date per symbol, keep only newer rows.
# A symbol with no existing rows gets max_date = None -> all its
# rows are treated as new (first-ever load for that symbol).
# ---------------------------------------------------------

cursor.execute(
    "SELECT symbol, MAX(date) FROM finance.daily_prices GROUP BY symbol;"
)
existing_max_dates = dict(cursor.fetchall())  # {symbol: max_date}

print(f"Existing symbols in DB: {list(existing_max_dates.keys()) or 'none yet'}")

def is_new_row(row):
    max_date = existing_max_dates.get(row["symbol"])
    return max_date is None or row["date"] > max_date

incremental_df = df[df.apply(is_new_row, axis=1)].copy()

skipped = len(df) - len(incremental_df)
print(f"Rows already up to date (skipped): {skipped}")
print(f"New rows to upsert: {len(incremental_df)}")

if incremental_df.empty:
    print("Nothing new to load. Exiting.")
    cursor.close()
    conn.close()
    sys.exit(0)


# ---------------------------------------------------------
# Replace NaN with None
# PostgreSQL understands None as NULL.
# ---------------------------------------------------------

incremental_df = incremental_df.astype(object).where(pd.notnull(incremental_df), None)


# ---------------------------------------------------------
# Insert only the incremental rows
# ---------------------------------------------------------

columns = [
    "symbol", "date", "open", "high", "low", "close", "volume",
    "previous_close", "price_change", "daily_return_pct",
    "moving_avg_7", "moving_avg_20", "rolling_volatility_20", "volume_avg_20",
]

values = [
    tuple(row[column] for column in columns)
    for _, row in incremental_df.iterrows()
]

insert_query = """
    INSERT INTO finance.daily_prices (
        symbol, date, open, high, low, close, volume,
        previous_close, price_change, daily_return_pct,
        moving_avg_7, moving_avg_20, rolling_volatility_20, volume_avg_20
    )
    VALUES %s
    ON CONFLICT (symbol, date) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        previous_close = EXCLUDED.previous_close,
        price_change = EXCLUDED.price_change,
        daily_return_pct = EXCLUDED.daily_return_pct,
        moving_avg_7 = EXCLUDED.moving_avg_7,
        moving_avg_20 = EXCLUDED.moving_avg_20,
        rolling_volatility_20 = EXCLUDED.rolling_volatility_20,
        volume_avg_20 = EXCLUDED.volume_avg_20;
"""

try:
    execute_values(cursor, insert_query, values)
    conn.commit()
except Exception:
    conn.rollback()
    cursor.close()
    conn.close()
    raise


# ---------------------------------------------------------
# Verify
# ---------------------------------------------------------

cursor.execute("SELECT COUNT(*) FROM finance.daily_prices;")
count = cursor.fetchone()[0]

print(f"Total rows now in PostgreSQL: {count}")


# ---------------------------------------------------------
# Close connection
# ---------------------------------------------------------

cursor.close()
conn.close()

print("Gold -> PostgreSQL incremental load completed successfully.")