#!/usr/bin/env python3

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    current_date
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)

# =========================
# Configuration
# =========================

CSV_PATH = os.getenv("AIRPORT_CSV_PATH", "/data/airports.csv")
DELTA_TABLE_PATH = os.getenv("AIRPORT_DELTA_TABLE_PATH", "/opt/delta-lake/tables/airports")

# =========================
# Schema Definition
# =========================

airport_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("ident", StringType(), True),
    StructField("icao", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("gps_code", StringType(), True),
    StructField("local_code", StringType(), True),
    StructField("name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("elevation", DoubleType(), True),
    StructField("continent", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("municipality", StringType(), True),
    StructField("scheduled_service", StringType(), True),
    StructField("web_url", StringType(), True),
    StructField("wikipedia_url", StringType(), True),
    StructField("keywords", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("airport_type", StringType(), True)
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

    # Read CSV file
    df = spark.read \
        .option("header", "true") \
        .schema(airport_schema) \
        .csv(CSV_PATH)

    # Data validation & cleaning
    clean_df = df \
        .filter(col("latitude").isNotNull() & col("longitude").isNotNull()) \
        .filter(col("icao").isNotNull() | col("ident").isNotNull())

    # Metadata columns
    final_df = clean_df \
        .withColumn("last_update", current_timestamp()) \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("processing_date", current_date())

    print(f"Writing data to Delta Lake: {DELTA_TABLE_PATH}")

    final_df.write \
        .format("delta") \
        .mode("append") \
        .save(DELTA_TABLE_PATH)

    print("Airport batch processing completed successfully.")

    spark.stop()
 
