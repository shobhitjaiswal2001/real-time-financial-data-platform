from pathlib import Path
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, stddev, round as spark_round
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_DIR = PROJECT_ROOT / "data" / "silver" / "daily_prices"
OUTPUT_DIR = PROJECT_ROOT / "data" / "gold" / "daily_prices"

spark = (
    SparkSession.builder
    .appName("FinancialGold")
    .master("local[*]")
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Reading Silver data...")
df = spark.read.parquet(str(INPUT_DIR))
print("Silver rows:", df.count())

df = df.orderBy("symbol", "date")

# ============================================================
# WINDOWS — NOW PARTITIONED BY SYMBOL
# This is the fix: each stock's calculations stay isolated.
# ============================================================

window_spec = Window.partitionBy("symbol").orderBy("date")
window_7 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-6, 0)
window_20 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-19, 0)

df = df.withColumn("previous_close", lag("close").over(window_spec))
df = df.withColumn("price_change", col("close") - col("previous_close"))
df = df.withColumn("daily_return_pct", (col("price_change") / col("previous_close")) * 100)
df = df.withColumn("moving_avg_7", avg("close").over(window_7))
df = df.withColumn("moving_avg_20", avg("close").over(window_20))
df = df.withColumn("rolling_volatility_20", stddev("daily_return_pct").over(window_20))
df = df.withColumn("volume_avg_20", avg("volume").over(window_20))

df = (
    df
    .withColumn("price_change", spark_round("price_change", 4))
    .withColumn("daily_return_pct", spark_round("daily_return_pct", 4))
    .withColumn("moving_avg_7", spark_round("moving_avg_7", 4))
    .withColumn("moving_avg_20", spark_round("moving_avg_20", 4))
    .withColumn("rolling_volatility_20", spark_round("rolling_volatility_20", 4))
    .withColumn("volume_avg_20", spark_round("volume_avg_20", 2))
)

gold_df = df.select(
    "symbol", "date", "open", "high", "low", "close", "volume",
    "previous_close", "price_change", "daily_return_pct",
    "moving_avg_7", "moving_avg_20", "rolling_volatility_20", "volume_avg_20",
)

print("Gold rows:", gold_df.count())
gold_df.printSchema()
gold_df.show(10, truncate=False)

print("Writing Gold data...")

pandas_gold_df = gold_df.toPandas()

if OUTPUT_DIR.exists():
    if OUTPUT_DIR.is_dir():
        shutil.rmtree(OUTPUT_DIR)
    else:
        OUTPUT_DIR.unlink()

pandas_gold_df.to_parquet(
    OUTPUT_DIR,
    engine="pyarrow",
    index=False,
    coerce_timestamps="us",
    allow_truncated_timestamps=True,
)

print(f"Gold data written successfully to: {OUTPUT_DIR}")
print(f"Rows written: {len(pandas_gold_df)}")

spark.stop()
print("Spark stopped successfully.")