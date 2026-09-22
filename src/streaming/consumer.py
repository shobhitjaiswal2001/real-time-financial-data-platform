"""
consumer.py

Reads live stock ticks from Kafka and computes a rolling average
per symbol in near-real-time, printing updates as they arrive.
"""

import json
from collections import defaultdict, deque

from kafka import KafkaConsumer


KAFKA_BROKER = "localhost:9092"
TOPIC = "stock-ticks"

WINDOW_SIZE = 10  # rolling average over the last 10 ticks per symbol

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
)

# Rolling windows per symbol
price_windows = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))

print(f"Listening to Kafka topic '{TOPIC}'... Ctrl+C to stop.")

try:
    for message in consumer:
        tick = message.value
        symbol = tick["symbol"]
        price = tick["price"]

        price_windows[symbol].append(price)
        rolling_avg = sum(price_windows[symbol]) / len(price_windows[symbol])

        print(
            f"[{tick['timestamp']}] {symbol}: price={price:.2f} "
            f"rolling_avg({len(price_windows[symbol])} ticks)={rolling_avg:.2f}"
        )

except KeyboardInterrupt:
    print("\nStopping consumer.")