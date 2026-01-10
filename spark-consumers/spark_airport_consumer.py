#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, current_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

# Kafka & Delta Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_INTERNAL_PORT', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_AIRPORT', 'airport_stream')
CHECKPOINT_LOCATION = "/opt/delta-lake/checkpoints/airports"
TABLE_PATH = "/opt/delta-lake/tables/airports"

# Schema for Airport Data
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
    StructField("airport_type", StringType(), True),
    StructField("last_update", StringType(), True)
])

def create_spark_session():
    return SparkSession.builder \
        .appName("AirportStreamProcessor") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading from Kafka topic: {KAFKA_TOPIC}")

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Parse JSON and Add Metadata
    transformed_df = df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), airport_schema).alias("data")) \
        .select(
            col("data.*"),
            current_timestamp().alias("processed_at"),
            current_date().alias("processing_date")
        )

    print(f"Writing stream to Delta table: {TABLE_PATH}")

    # Write to Delta Lake
    query = transformed_df.writeStream \
        .outputMode("append") \
        .format("delta") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start(TABLE_PATH)

    query.awaitTermination()

if __name__ == "__main__":
    main()
