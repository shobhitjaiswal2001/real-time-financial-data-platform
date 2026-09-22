import json
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "data" / "raw"
PARQUET_DIR = PROJECT_ROOT / "data" / "bronze"
PARQUET_FILE = PARQUET_DIR / "daily_prices.parquet"  # combined, multi-symbol

PARQUET_DIR.mkdir(parents=True, exist_ok=True)

all_rows = []

# Process every raw file found (one per symbol)
for raw_file in sorted(RAW_DIR.glob("*_daily_raw.json")):
    symbol = raw_file.stem.replace("_daily_raw", "").upper()

    with open(raw_file, "r", encoding="utf-8") as file:
        data = json.load(file)

    time_series = data["Time Series (Daily)"]

    for date, values in time_series.items():
        all_rows.append({
            "symbol": symbol,
            "date": date,
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"]),
        })

df = pd.DataFrame(all_rows)
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

df.to_parquet(
    PARQUET_FILE,
    index=False,
    engine="pyarrow",
    coerce_timestamps="us",
    allow_truncated_timestamps=True,
)

print(f"Parquet file created successfully:")
print(PARQUET_FILE)
print(f"Rows: {len(df)}")
print(f"Symbols: {sorted(df['symbol'].unique())}")
print("\nSchema:")
print(df.dtypes)