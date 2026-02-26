import os
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

"""To submit the job
docker exec -it spark-master bash -c "
mkdir -p /tmp/ivy &&
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages io.delta:delta-spark_2.12:3.1.0 \
--conf spark.jars.ivy=/tmp/ivy \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--driver-memory 512m \
--executor-memory 512m \
--executor-cores 1 \
"opt/spark/scripts/read_delta_tables.py
"""


# 1. Create Spark Session (Memory Optimized)
spark = (
    SparkSession.builder
    .appName("ReadDeltaTables")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # --- Memory & Resource Optimizations ---
    .config("spark.sql.shuffle.partitions", "4")           # Default 200 is too high for small clusters
    .config("spark.default.parallelism", "4")              # Match your total cores (2 workers x 2 cores)
    .config("spark.memory.fraction", "0.6")                # Fraction of JVM heap for execution/storage
    .config("spark.memory.storageFraction", "0.3")         # Less cache, more execution memory
    .config("spark.sql.files.maxPartitionBytes", "64mb")   # Smaller partition sizes
    .config("spark.driver.memory", "512m")                 # Limit driver memory
    .config("spark.executor.memory", "512m")               # Limit executor memory
    .config("spark.executor.memoryOverhead", "256m")       # Overhead buffer
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")  # Reduce log noise

# 2. Path to Delta Tables
BASE_PATH = "/opt/delta-lake/tables"

SAMPLE_ROWS = 5      # How many rows to preview
HISTORY_ROWS = 3     # How many history entries to show

# 3. Function to Read All Tables
def read_all_delta_tables(base_path):
    if not os.path.exists(base_path):
        print(f"Path does not exist: {base_path}")
        return

    tables = os.listdir(base_path)
    if not tables:
        print("No tables found.")
        return

    for table in tables:
        table_path = os.path.join(base_path, table)
        if not os.path.isdir(table_path):
            continue

        print("\n" + "=" * 50)
        print(f"TABLE: {table}")
        print("=" * 50)

        try:
            # --- Load only a small sample, avoid full scan ---
            df = (
                spark.read
                .format("delta")
                .load(table_path)
                .limit(SAMPLE_ROWS)     # Push limit down — reads minimal files
            )

            print("Schema:")
            df.printSchema()

            print(f"Sample Data (top {SAMPLE_ROWS} rows):")
            df.show(SAMPLE_ROWS, truncate=False)

            # --- Row count via Delta metadata (no full table scan) ---
            delta_table = DeltaTable.forPath(spark, table_path)
            detail = delta_table.detail().collect()[0]
            print(f"Total Files : {detail['numFiles']}")
            print(f"Size on Disk: {round(detail['sizeInBytes'] / 1024 / 1024, 2)} MB")

            # --- Show limited history ---
            print(f"Delta History (last {HISTORY_ROWS} operations):")
            delta_table.history(HISTORY_ROWS).show(truncate=False)

        except Exception as e:
            print(f"Error reading table '{table}': {e}")

        finally:
            # --- Release memory after each table ---
            spark.catalog.clearCache()

# 4. Run
if __name__ == "__main__":
    read_all_delta_tables(BASE_PATH)
    spark.stop()