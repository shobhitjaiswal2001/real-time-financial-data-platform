from pathlib import Path
import os
import shutil

os.environ["HADOOP_HOME"] = str(Path.home() / "hadoop")
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "data" / "bronze" / "daily_prices.parquet"
OUTPUT_DIR = PROJECT_ROOT / "data" / "silver" / "daily_prices"

spark = (
    SparkSession.builder
    .appName("FinancialDataSilver")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet(str(INPUT_FILE))

df = (
    df
    .withColumn("symbol", col("symbol").cast("string"))
    .withColumn("open", col("open").cast("double"))
    .withColumn("high", col("high").cast("double"))
    .withColumn("low", col("low").cast("double"))
    .withColumn("close", col("close").cast("double"))
    .withColumn("volume", col("volume").cast("long"))
)

print("Rows:", df.count())
df.printSchema()
df.show(10, truncate=False)

pandas_df = df.toPandas()

if OUTPUT_DIR.exists():
    if OUTPUT_DIR.is_dir():
        shutil.rmtree(OUTPUT_DIR)
    else:
        OUTPUT_DIR.unlink()

pandas_df.to_parquet(
    OUTPUT_DIR,
    engine="pyarrow",
    index=False,
    coerce_timestamps="us",
    allow_truncated_timestamps=True,
)

print(f"Silver data written to: {OUTPUT_DIR}")
print(f"Rows written: {len(pandas_df)}")

spark.stop()