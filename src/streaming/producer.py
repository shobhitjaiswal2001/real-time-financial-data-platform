"""
producer.py

Simulates a live stock price tick stream and publishes to Kafka.
Since Alpha Vantage's free tier only gives daily data, this generates
realistic random-walk price ticks for demonstration of the streaming pattern.
"""

import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer


KAFKA_BROKER = "localhost:9092"
TOPIC = "stock-ticks"

SYMBOLS = ["IBM", "AAPL", "MSFT"]

# Starting prices (roughly realistic, will random-walk from here)
last_prices = {
    "IBM": 230.0,
    "AAPL": 275.0,
    "MSFT": 410.0,
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"Producing simulated ticks to Kafka topic '{TOPIC}'... Ctrl+C to stop.")

try:
    while True:
        symbol = random.choice(SYMBOLS)

        # Random walk: small % change up or down
        change_pct = random.uniform(-0.5, 0.5)
        new_price = round(last_prices[symbol] * (1 + change_pct / 100), 2)
        last_prices[symbol] = new_price

        tick = {
            "symbol": symbol,
            "price": new_price,
            "timestamp": datetime.utcnow().isoformat(),
        }

        producer.send(TOPIC, value=tick)
        print(f"Sent: {tick}")

        time.sleep(random.uniform(1, 3))  # simulate irregular tick arrival

except KeyboardInterrupt:
    print("\nStopping producer.")
    producer.close()