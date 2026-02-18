from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("TableViewer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

table_path = "/opt/delta-lake/tables/proximity_events"

if os.path.exists(table_path):
    print(f"Viewing table: {table_path}")
    df = spark.read.format("delta").load(table_path)
    df.show(truncate=False)
else:
    print(f"Table {table_path} does not exist yet. Fuck it lets touch some grass.")

