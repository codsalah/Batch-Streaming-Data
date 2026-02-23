#!/usr/bin/env python3
import os
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, current_timestamp, broadcast, 
    when, lit, log, round, pow, greatest, least, exp
)
from pyspark.sql.types import DoubleType, StringType

# Configuration from Environment
EARTHQUAKE_DELTA_PATH = os.getenv('EARTHQUAKE_DELTA_PATH', '/opt/delta-lake/tables/earthquakes')
AIRPORTS_DELTA_PATH = os.getenv('AIRPORTS_DELTA_PATH', '/opt/delta-lake/tables/airports')
OUTPUT_DELTA_PATH = os.getenv('INTENSITY_OUTPUT_PATH', '/opt/delta-lake/tables/intensity_analysis')
CHECKPOINT_PATH = os.getenv('INTENSITY_CHECKPOINT_PATH', '/opt/delta-lake/checkpoints/intensity_analysis')

# PGV/PGD estimation parameters (configurable)
PGV_PARAMS = {
    'a': float(os.getenv('PGV_A', '-2.5')),
    'b': float(os.getenv('PGV_B', '0.8')),
    'c': float(os.getenv('PGV_C', '1.2')),
    'e': float(os.getenv('PGV_E', '0.005'))
}

PGD_PARAMS = {
    'a2': float(os.getenv('PGD_A2', '-3.0')),
    'b2': float(os.getenv('PGD_B2', '0.9')),
    'c2': float(os.getenv('PGD_C2', '1.3')),
    'e2': float(os.getenv('PGD_E2', '0.006'))
}

# Intensity thresholds (configurable)
PGV_LOW_THRESHOLD = float(os.getenv('PGV_LOW_THRESHOLD', '0.1'))
PGV_MODERATE_THRESHOLD = float(os.getenv('PGV_MODERATE_THRESHOLD', '1.0'))

def create_spark_session(app_name="IntensityAnalyzer"):
    """Create optimized Spark session with Delta Lake support"""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_earthquake_data(spark):
    """Load earthquake data from Delta Lake"""
    return (
        spark.read.format("delta")
        .load(EARTHQUAKE_DELTA_PATH)
        .select(
            col("event_id"),
            col("event_time").alias("timestamp"),
            col("latitude").cast("double"),
            col("longitude").cast("double"),
            col("depth_km").alias("depth"),
            col("magnitude").cast("double")
        )
        .filter(col("latitude").isNotNull() & 
                col("longitude").isNotNull() & 
                col("magnitude").isNotNull())
    )

def load_airport_data(spark):
    """Load airport data from Delta Lake"""
    return (
        spark.read.format("delta")
        .load(AIRPORTS_DELTA_PATH)
        .select(
            col("id").alias("airport_id"),
            col("latitude").alias("airport_latitude"),
            col("longitude").alias("airport_longitude")
        )
        .filter(col("latitude").isNotNull() & col("longitude").isNotNull())
    )

def haversine_distance_expr(lat1_col, lon1_col, lat2_col, lon2_col):
    """
    Calculate Haversine distance using Spark SQL expressions.
    Returns distance in kilometers.
    """
    earth_radius_km = 6371.0
    
    return expr(f"""
        {earth_radius_km} * 
        acos(
            cos(radians({lat1_col})) * cos(radians({lat2_col})) * 
            cos(radians({lon2_col}) - radians({lon1_col})) + 
            sin(radians({lat1_col})) * sin(radians({lat2_col}))
        )
    """)

def estimate_ground_motion(df):
    """
    Estimate PGV and PGD using rule-based engineering models.
    Uses pure Spark SQL functions for optimal performance.
    """
    # PGV estimation: log(PGV) = a + b*magnitude - c*log(distance_km + 1) - e*depth
    df = df.withColumn(
        "log_pgv",
        lit(PGV_PARAMS['a']) + 
        lit(PGV_PARAMS['b']) * col("magnitude") - 
        lit(PGV_PARAMS['c']) * log(col("distance_km") + lit(1)) - 
        lit(PGV_PARAMS['e']) * col("depth")
    )
    
    # PGD estimation: log(PGD) = a2 + b2*magnitude - c2*log(distance_km + 1) - e2*depth
    df = df.withColumn(
        "log_pgd",
        lit(PGD_PARAMS['a2']) + 
        lit(PGD_PARAMS['b2']) * col("magnitude") - 
        lit(PGD_PARAMS['c2']) * log(col("distance_km") + lit(1)) - 
        lit(PGD_PARAMS['e2']) * col("depth")
    )
    
    # Convert from log scale to actual values
    df = df.withColumn("estimated_pgv", exp(col("log_pgv")))
    df = df.withColumn("estimated_pgd", exp(col("log_pgd")))
    
    # Clean up intermediate columns
    df = df.drop("log_pgv", "log_pgd")
    
    return df

def classify_intensity(df):
    """
    Classify intensity based on estimated PGV values.
    """
    return (
        df.withColumn(
            "intensity_label",
            when(col("estimated_pgv") < lit(PGV_LOW_THRESHOLD), "Low")
            .when(col("estimated_pgv") < lit(PGV_MODERATE_THRESHOLD), "Moderate")
            .otherwise("High")
        )
        .withColumn(
            "high_intensity_flag",
            when(col("intensity_label") == "High", lit(True))
            .otherwise(lit(False))
        )
    )

def find_nearest_airport(earthquake_df, airport_df):
    """
    Find nearest airport for each earthquake using cross join with distance calculation.
    Optimized using broadcast join for airports dataset.
    """
    # Cross join earthquakes with broadcasted airports
    cross_joined = earthquake_df.crossJoin(broadcast(airport_df))
    
    # Calculate distance using Haversine formula
    with_distance = cross_joined.withColumn(
        "distance_km",
        haversine_distance_expr(
            "latitude", "longitude", 
            "airport_latitude", "airport_longitude"
        )
    )
    
    # Find nearest airport for each earthquake using window function
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window_spec = Window.partitionBy("event_id").orderBy(col("distance_km"))
    
    nearest_airport = (
        with_distance
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn", "airport_latitude", "airport_longitude")
    )
    
    return nearest_airport

def main():
    """Main pipeline execution"""
    print("=" * 80)
    print("Starting Earthquake Intensity Analysis Pipeline")
    print("=" * 80)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load data
        print("Loading earthquake data...")
        earthquake_df = load_earthquake_data(spark)
        print(f"Loaded {earthquake_df.count()} earthquake records")
        
        print("Loading airport data...")
        airport_df = load_airport_data(spark)
        print(f"Loaded {airport_df.count()} airport records")
        
        # Find nearest airports
        print("Finding nearest airports for each earthquake...")
        with_airports = find_nearest_airport(earthquake_df, airport_df)
        
        # Estimate ground motion parameters
        print("Estimating PGV and PGD...")
        with_ground_motion = estimate_ground_motion(with_airports)
        
        # Classify intensity
        print("Classifying intensity levels...")
        final_df = classify_intensity(with_ground_motion)
        
        # Add processing timestamp
        final_df = final_df.withColumn("processed_at", current_timestamp())
        
        # Show sample results
        print("\nSample results:")
        final_df.select(
            "event_id", "magnitude", "distance_km", 
            "estimated_pgv", "estimated_pgd", 
            "intensity_label", "high_intensity_flag"
        ).show(10, truncate=False)
        
        # Save to Delta Lake
        print(f"\nSaving results to {OUTPUT_DELTA_PATH}")
        (
            final_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(OUTPUT_DELTA_PATH)
        )
        
        print("Pipeline completed successfully!")
        
        # Print statistics
        print("\nIntensity Classification Statistics:")
        final_df.groupBy("intensity_label").count().show()
        
        high_intensity_count = final_df.filter(col("high_intensity_flag") == True).count()
        print(f"High intensity events: {high_intensity_count}")
        
    except Exception as e:
        print(f"Pipeline failed with error: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
