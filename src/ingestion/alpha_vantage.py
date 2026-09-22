import os
import json
import time
from pathlib import Path

import requests
from dotenv import load_dotenv


# --------------------------------------------------
# 1. Find project root
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]


# --------------------------------------------------
# 2. Load environment variables
# --------------------------------------------------

ENV_FILE = PROJECT_ROOT / ".env"
load_dotenv(ENV_FILE)

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

if not API_KEY:
    raise ValueError(
        f"API key not found. Expected .env at: {ENV_FILE}"
    )


# --------------------------------------------------
# 3. Symbols to ingest
#    (single source of truth — add tickers here later)
# --------------------------------------------------

SYMBOLS = ["IBM"]  # e.g. later: ["IBM", "AAPL", "MSFT"]


# --------------------------------------------------
# 4. Alpha Vantage API
# --------------------------------------------------

url = "https://www.alphavantage.co/query"

RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

for symbol in SYMBOLS:
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": API_KEY,
    }

    response = requests.get(url, params=params, timeout=30)

    print(f"[{symbol}] HTTP Status:", response.status_code)

    response.raise_for_status()

    data = response.json()

    if "Time Series (Daily)" not in data:
        raise ValueError(
            f"[{symbol}] Unexpected API response (rate limit or bad symbol?): {data}"
        )

    raw_file = RAW_DIR / f"{symbol.lower()}_daily_raw.json"

    with open(raw_file, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)

    print(f"[{symbol}] Raw data saved successfully to: {raw_file}")

    time.sleep(15)  # stay safely under 1 req/sec; also spaces out daily quota usage