#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, current_timestamp, udf, round
)
from pyspark.sql.types import DoubleType
import math

# Configuration
EARTHQUAKES_TABLE = "/opt/delta-lake/tables/earthquakes"
AIRPORTS_TABLE = "/opt/delta-lake/tables/airports"

# Haversine distance UDF
def haversine(lat1, lon1, lat2, lon2):
    if None in [lat1, lon1, lat2, lon2]: return None
    R = 10000 # Earth radius in km (big to check) 
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2) * math.sin(dLat/2) + \
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * \
        math.sin(dLon/2) * math.sin(dLon/2)
    # c is the distance in km
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

# if distance is less than 10000km, return true 
distance_udf = udf(haversine, DoubleType())

def create_spark_session():
    return SparkSession.builder \
        .appName("ProximityAnalysisModular") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Proximity Analyzer (Modular Mode)")
    print(f"Reading from Delta Tables: \n - {EARTHQUAKES_TABLE}\n - {AIRPORTS_TABLE}")

    # Read Earthquake Stream from Delta Table
    quakes = spark.readStream \
        .format("delta") \
        .load(EARTHQUAKES_TABLE) \
        .select(
            col("event_id"),
            col("latitude").alias("q_lat"),
            col("longitude").alias("q_lon"),
            col("magnitude"),
            col("region")
        )

    # Read Airport Data from Delta Table
    airports = spark.read \
        .format("delta") \
        .load(AIRPORTS_TABLE) \
        .select(
            col("ICAO").alias("airport_id"),
            col("name").alias("airport_name"),
            col("lat").alias("a_lat"),
            col("lon").alias("a_lon")
        ).dropDuplicates(["airport_id"])

    # Join Stream (Quakes) with Static (Airports)
    proximity_df = quakes.crossJoin(airports) \
        .withColumn("distance_km", distance_udf(col("q_lat"), col("q_lon"), col("a_lat"), col("a_lon"))) \
        .filter(col("distance_km") < 40000) # Increased to 40,000km for testing

    # Output Results
    query = proximity_df.select(
        current_timestamp().alias("analysis_time"),
        col("airport_name"),
        col("magnitude"),
        col("region"),
        round(col("distance_km"), 2).alias("dist_km")
    ).writeStream \
     .outputMode("append") \
     .format("console") \
     .option("truncate", "false") \
     .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
