#!/usr/bin/env python3

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, current_date
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

# =========================
# Configuration
# =========================
CSV_PATH = os.getenv("AIRPORT_CSV_PATH", "/opt/spark/work-dir/airports.csv")
DELTA_TABLE_PATH = "/tmp/delta-airports"

airport_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("altitude", DoubleType(), True),
    StructField("timezone_offset", IntegerType(), True),
    StructField("dst", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("airport_type", StringType(), True),
    StructField("source", StringType(), True)
])

# =========================
# Spark Session
# =========================
def create_spark_session():
    return SparkSession.builder \
        .appName("AirportBatchToDelta") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

# =========================
# Main Logic
# =========================
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading airport data from CSV: {CSV_PATH}")

    df = spark.read \
        .option("header", "false") \
        .option("sep", ",") \
        .option("encoding", "UTF-8") \
        .schema(airport_schema) \
        .csv(CSV_PATH)

    print("Total rows read:", df.count())
    print("Schema:")
    df.printSchema()
    print("First 5 rows:")
    df.show(5, truncate=False)

    # Metadata columns
    final_df = df \
        .withColumn("last_update", current_timestamp()) \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("processing_date", current_date())

    print(f"Writing data to Delta Lake (overwrite mode): {DELTA_TABLE_PATH}")

    final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(DELTA_TABLE_PATH)

    print("Airport batch processing completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()