#!/usr/bin/env python3
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp, 
    current_date, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, ArrayType
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'earthquake_raw')

DELTA_TABLE_PATH = os.getenv('EARTHQUAKE_DELTA_TABLE_PATH', '/opt/delta-lake/tables/earthquakes')
CHECKPOINT_LOCATION = os.getenv('EARTHQUAKE_CHECKPOINT_PATH', '/opt/delta-lake/checkpoints/earthquakes')

# schema of the incoming data (earthquake_raw)
earthquake_schema = StructType([
    StructField("action", StringType(), True),
    StructField("data", StructType([
        StructField("type", StringType(), True),
        StructField("id", StringType(), True),
        StructField("geometry", StructType([
            StructField("type", StringType(), True),
            StructField("coordinates", ArrayType(DoubleType()), True)
        ]), True),
        StructField("properties", StructType([
            StructField("unid", StringType(), True),
            StructField("source_id", StringType(), True),
            StructField("source_catalog", StringType(), True),
            StructField("lastupdate", StringType(), True),
            StructField("time", StringType(), True),
            StructField("flynn_region", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("depth", DoubleType(), True),
            StructField("evtype", StringType(), True),
            StructField("auth", StringType(), True),
            StructField("mag", DoubleType(), True),
            StructField("magtype", StringType(), True)
        ]), True)
    ]), True)
])

"""
Example of Incoming JSON data 
{
  "action": "create",
  "data": {
    "type": "Feature",
    "id": "20251212_0000172",
    "geometry": {
      "type": "Point",
      "coordinates": [28.9592, 39.29, -8.0]
    },
    "properties": {
      "source_catalog": "EMSC-RTS",
      "source_id": "1913376",
      "time": "2025-12-12T08:30:52.000Z",
      "lastupdate": "2025-12-12T10:32:50.351241Z",
      "flynn_region": "WESTERN TURKEY",
      "lat": 39.29,
      "lon": 28.9592,
      "depth": 8.0,
      "mag": 0.8,
      "magtype": "ml",
      "evtype": "ke",
      "auth": "AFAD",
      "unid": "20251212_0000172"
    }
  }
}
"""

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("EarthquakeStreamProcessor") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.ui.prometheus.enabled", "true") \
        .config("spark.executor.processTreeMetrics.enabled", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def read_from_kafka(spark):
    """Read streaming data from Kafka"""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()


def parse_and_transform(kafka_df):
    """
    Parse JSON from Kafka and flatten the structure with quality checks.
    """
    
    # Step 1: Parse JSON from Kafka value column
    # We keep raw_value for debugging or dead-letter if needed
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
        .withColumn("data", from_json(col("json_string"), earthquake_schema))
    
    # Step 2: Filter out corrupt records (where parsing failed)
    valid_json_df = parsed_df.filter(col("data").isNotNull())
    
    # Step 3 & 4: Extract and flatten all fields
    flattened_df = valid_json_df.select(
        col("data.action").alias("action"),
        col("data.data.id").alias("event_id"),
        col("data.data.properties.unid").alias("unid"),
        to_timestamp(col("data.data.properties.time")).alias("event_time"),
        to_timestamp(col("data.data.properties.lastupdate")).alias("last_update"),
        col("data.data.properties.lat").alias("latitude"),
        col("data.data.properties.lon").alias("longitude"),
        col("data.data.properties.depth").alias("depth_km"),
        col("data.data.geometry.coordinates")[0].alias("coord_longitude"),
        col("data.data.geometry.coordinates")[1].alias("coord_latitude"),
        col("data.data.geometry.coordinates")[2].alias("coord_depth"),
        col("data.data.properties.mag").alias("magnitude"),
        col("data.data.properties.magtype").alias("magnitude_type"),
        col("data.data.properties.flynn_region").alias("region"),
        col("data.data.properties.auth").alias("authority"),
        col("data.data.properties.evtype").alias("event_type"),
        col("data.data.properties.source_id").alias("source_id"),
        col("data.data.properties.source_catalog").alias("source_catalog"),
        current_timestamp().alias("processed_at"),
        current_date().alias("processing_date")
    )
    
    # Step 5: Data Quality Range Checks
    cleaned_df = flattened_df.filter(
        (col("latitude") >= -90) & (col("latitude") <= 90) &
        (col("longitude") >= -180) & (col("longitude") <= 180) &
        (col("unid").isNotNull())
    )
    
    # Step 6: Deduplication
    # Note: dropDuplicates in streaming requires watermarking if not using append mode on all fields,
    # but for simple deduplication on a unique key in append mode it works if configured with state.
    # However, a simpler way for exactly-once in Delta is to use MERGE (foreachBatch),
    # but for "basic basics", dropDuplicates on the unid is a good start.
    deduplicated_df = cleaned_df.dropDuplicates(["unid"])
    
    return deduplicated_df


def main():
    """
        Main execution function for now
        This is going to be in the terminal until 
        we add the data to delta lake 
            (we will use delta lake to store the data)
    """
    
    print("=" * 80)
    print("Starting Earthquake Stream Processor")
    print("=" * 80)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✓ Spark session created")
    print(f"✓ Reading from Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"✓ Topic: {KAFKA_TOPIC}")
    
    # Read from Kafka
    kafka_df = read_from_kafka(spark)
    print(f"✓ Kafka stream connected")
    
    # Parse and transform
    transformed_df = parse_and_transform(kafka_df)
    print(f"✓ Transformation pipeline configured")
    
    print("\n" + "=" * 80)
    print("Starting stream output to Delta Lake...")
    print("=" * 80 + "\n")
    
    query = transformed_df.writeStream \
        .outputMode("append") \
        .format("delta") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start(DELTA_TABLE_PATH)
    
    print("✓ Stream started successfully!")
    print("✓ Processing events every 10 seconds...")
    print("\nPress Ctrl+C to stop\n")
    
    # Wait for termination
    query.awaitTermination()


if __name__ == "__main__":
    main()
