"""
spark_stream_consumer.py

Reads live stock ticks from Kafka using Spark Structured Streaming,
computes a rolling average per symbol over a sliding window, and
writes results continuously to PostgreSQL.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CHECKPOINT_DIR = PROJECT_ROOT / "data" / "streaming_checkpoints"

KAFKA_BROKER = "localhost:9092"
TOPIC = "stock-ticks"

PG_HOST = "localhost"
PG_PORT = "2510"
PG_DB = "financial_data"
PG_USER = "postgres"
PG_PASSWORD = "2510"
PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("StockTickStreaming")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.3",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# DEFINE TICK SCHEMA (matches producer.py's JSON structure)
# ============================================================

tick_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])


# ============================================================
# READ STREAM FROM KAFKA
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Kafka gives us raw bytes in a "value" column; parse the JSON
parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), tick_schema).alias("data"))
    .select(
        col("data.symbol").alias("symbol"),
        col("data.price").alias("price"),
        col("data.timestamp").cast(TimestampType()).alias("tick_timestamp"),
    )
)


# ============================================================
# COMPUTE ROLLING AVERAGE (30-second sliding window, per symbol)
# ============================================================

windowed_avg = (
    parsed_stream
    .withWatermark("tick_timestamp", "1 minute")
    .groupBy(
        window(col("tick_timestamp"), "30 seconds", "10 seconds"),
        col("symbol"),
    )
    .agg(avg("price").alias("rolling_avg"))
    .select(
        col("symbol"),
        col("rolling_avg"),
        col("window.end").alias("tick_timestamp"),
    )
)


# ============================================================
# WRITE EACH MICRO-BATCH TO POSTGRES
# ============================================================

def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no new data.")
        return

    print(f"Batch {batch_id}: writing {batch_df.count()} rows to Postgres.")

    (
        batch_df.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "finance.live_ticks")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


query = (
    windowed_avg.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("update")
    .option("checkpointLocation", str(CHECKPOINT_DIR))
    .trigger(processingTime="10 seconds")
    .start()
)

print("Streaming query started. Writing rolling averages to Postgres every 10 seconds...")
print("Press Ctrl+C to stop.")

query.awaitTermination()