#!/usr/bin/env python3
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp, 
    current_date, get_json_object
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
    Parse JSON from Kafka and flatten the structure
    
    WHAT I ADDED:
    1. Added get_json_object function import - more reliable than complex schema
    2. Added raw_json column to debug parsing issues
    3. Added debug filter to check for null data
    4. Using get_json_object approach which is more forgiving with schema mismatches
    """
    from pyspark.sql.functions import get_json_object
    
    # Step 1: Parse JSON from Kafka value column - KEEPING ORIGINAL APPROACH
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(
            from_json(col("json_string"), earthquake_schema).alias("data"),
            # ADDED: Keep raw JSON for debugging
            col("json_string").alias("raw_json")  
        )
    
    # ADDED: Debug filter to check for parsing failures
    # This helps identify if the schema doesn't match the actual data
    debug_failed_parsing = parsed_df.filter(col("data").isNull()).select("raw_json")
    
    # Step 2 & 3: Extract and flatten all fields
    # CHANGED: Using get_json_object for more reliable extraction
    # This ensures we get data even if the main schema parsing fails
    flattened_df = parsed_df.select(
        # Action field - using both methods for reliability
        col("data.action").alias("action"),
        get_json_object(col("raw_json"), "$.action").alias("action_v2"),
        
        # Event identifiers
        col("data.data.id").alias("event_id"),
        get_json_object(col("raw_json"), "$.data.id").alias("event_id_v2"),
        
        col("data.data.properties.unid").alias("unid"),
        get_json_object(col("raw_json"), "$.data.properties.unid").alias("unid_v2"),
        
        # Timestamps as strings first, then convert
        col("data.data.properties.time").alias("event_time_str"),
        get_json_object(col("raw_json"), "$.data.properties.time").alias("event_time_str_v2"),
        
        col("data.data.properties.lastupdate").alias("last_update_str"),
        get_json_object(col("raw_json"), "$.data.properties.lastupdate").alias("last_update_str_v2"),
        
        # Location from properties
        col("data.data.properties.lat").alias("latitude"),
        get_json_object(col("raw_json"), "$.data.properties.lat").cast("double").alias("latitude_v2"),
        
        col("data.data.properties.lon").alias("longitude"),
        get_json_object(col("raw_json"), "$.data.properties.lon").cast("double").alias("longitude_v2"),
        
        col("data.data.properties.depth").alias("depth_km"),
        get_json_object(col("raw_json"), "$.data.properties.depth").cast("double").alias("depth_km_v2"),
        
        # Coordinates array
        col("data.data.geometry.coordinates")[0].alias("coord_longitude"),
        get_json_object(col("raw_json"), "$.data.geometry.coordinates[0]").cast("double").alias("coord_longitude_v2"),
        
        col("data.data.geometry.coordinates")[1].alias("coord_latitude"),
        get_json_object(col("raw_json"), "$.data.geometry.coordinates[1]").cast("double").alias("coord_latitude_v2"),
        
        col("data.data.geometry.coordinates")[2].alias("coord_depth"),
        get_json_object(col("raw_json"), "$.data.geometry.coordinates[2]").cast("double").alias("coord_depth_v2"),
        
        # Magnitude
        col("data.data.properties.mag").alias("magnitude"),
        get_json_object(col("raw_json"), "$.data.properties.mag").cast("double").alias("magnitude_v2"),
        
        col("data.data.properties.magtype").alias("magnitude_type"),
        get_json_object(col("raw_json"), "$.data.properties.magtype").alias("magnitude_type_v2"),
        
        # Region and metadata
        col("data.data.properties.flynn_region").alias("region"),
        get_json_object(col("raw_json"), "$.data.properties.flynn_region").alias("region_v2"),
        
        col("data.data.properties.auth").alias("authority"),
        get_json_object(col("raw_json"), "$.data.properties.auth").alias("authority_v2"),
        
        col("data.data.properties.evtype").alias("event_type"),
        get_json_object(col("raw_json"), "$.data.properties.evtype").alias("event_type_v2"),
        
        col("data.data.properties.source_id").alias("source_id"),
        get_json_object(col("raw_json"), "$.data.properties.source_id").alias("source_id_v2"),
        
        col("data.data.properties.source_catalog").alias("source_catalog"),
        get_json_object(col("raw_json"), "$.data.properties.source_catalog").alias("source_catalog_v2"),
        
        # Raw JSON for debugging
        col("raw_json")
    )
    
    # This ensures we get data if either method succeeds
    from pyspark.sql.functions import coalesce
    
    final_df = flattened_df.select(
        coalesce(col("action"), col("action_v2")).alias("action"),
        coalesce(col("event_id"), col("event_id_v2")).alias("event_id"),
        coalesce(col("unid"), col("unid_v2")).alias("unid"),
        to_timestamp(coalesce(col("event_time_str"), col("event_time_str_v2"))).alias("event_time"),
        to_timestamp(coalesce(col("last_update_str"), col("last_update_str_v2"))).alias("last_update"),
        coalesce(col("latitude"), col("latitude_v2")).alias("latitude"),
        coalesce(col("longitude"), col("longitude_v2")).alias("longitude"),
        coalesce(col("depth_km"), col("depth_km_v2")).alias("depth_km"),
        coalesce(col("coord_longitude"), col("coord_longitude_v2")).alias("coord_longitude"),
        coalesce(col("coord_latitude"), col("coord_latitude_v2")).alias("coord_latitude"),
        coalesce(col("coord_depth"), col("coord_depth_v2")).alias("coord_depth"),
        coalesce(col("magnitude"), col("magnitude_v2")).alias("magnitude"),
        coalesce(col("magnitude_type"), col("magnitude_type_v2")).alias("magnitude_type"),
        coalesce(col("region"), col("region_v2")).alias("region"),
        coalesce(col("authority"), col("authority_v2")).alias("authority"),
        coalesce(col("event_type"), col("event_type_v2")).alias("event_type"),
        coalesce(col("source_id"), col("source_id_v2")).alias("source_id"),
        coalesce(col("source_catalog"), col("source_catalog_v2")).alias("source_catalog"),
        current_timestamp().alias("processed_at"),
        current_date().alias("processing_date")
    )
    
    return final_df


def main():
    """
        Main execution function
        ADDED: Console output to verify data is flowing
    """
    
    print("=" * 80)
    print("Starting Earthquake Stream Processor")
    print("=" * 80)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Spark session created")
    print(f"Reading from Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    
    # Read from Kafka
    kafka_df = read_from_kafka(spark)
    print(f"Kafka stream connected")
    
    # Parse and transform
    transformed_df = parse_and_transform(kafka_df)
    print(f"Transformation pipeline configured")
    
    print("\n" + "=" * 80)
    print("Starting stream output...")
    print("=" * 80 + "\n")
    
   
    console_query = transformed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("Console stream started - check output above")
    
    # Main stream to Delta Lake
    delta_query = transformed_df.writeStream \
        .outputMode("append") \
        .format("delta") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime="10 seconds") \
        .start(DELTA_TABLE_PATH)
    
    print("Delta Lake stream started successfully!")
    print(f"Writing to: {DELTA_TABLE_PATH}")
    print("\nPress Ctrl+C to stop\n")
    
    
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
