#!/usr/bin/env python3
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr
from datetime import datetime

def main():
    spark = (
        SparkSession.builder
        .appName("PipelineValidator")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    tables = {
        "earthquakes": "/opt/delta-lake/tables/earthquakes",
        "wolf_seismic": "/opt/delta-lake/tables/wolf_seismic",
        "proximity_events": "/opt/delta-lake/tables/proximity_events"
    }

    success = True
    print("=" * 50)
    print(f"Validation started at {datetime.now()}")
    print("=" * 50)

    for name, path in tables.items():
        if not os.path.exists(path):
            print(f"FAILED: Table {name} does not exist at {path}")
            success = False
            continue

        try:
            df = spark.read.format("delta").load(path)
            count = df.count()

            time_col = "processed_at" if name != "proximity_events" else "analysis_time"

            if time_col in df.columns:
                recent_count = df.filter(
                    col(time_col) > current_timestamp() - expr("INTERVAL 10 MINUTES")
                ).count()

                print(f"Table {name}: Total rows = {count}, Recent rows (10m) = {recent_count}")

                if recent_count == 0 and count > 0:
                    print(f"WARNING: Table {name} has data but no updates in the last 10 minutes.")
                elif count == 0:
                    print(f"FAILED: Table {name} is empty.")
                    success = False
            else:
                print(f"Table {name}: Total rows = {count} (No timestamp column found)")
                if count == 0:
                    success = False

        except Exception as e:
            print(f"ERROR: Failed to read table {name}: {e}")
            success = False

    print("=" * 50)
    if success:
        print("RESULT: Pipeline validation PASSED")
        sys.exit(0)
    else:
        print("RESULT: Pipeline validation FAILED")
        sys.exit(1)

if __name__ == "__main__":
    main()