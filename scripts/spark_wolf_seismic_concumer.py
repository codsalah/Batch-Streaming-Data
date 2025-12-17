from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType
)
import os

# ===============================
# Kafka Configuration
# ===============================
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_INTERNAL_PORT', 'kafka:9092')
KAFKA_TOPIC = "wolf_seismic_stream"

# ===============================
# Wolf Seismic Data Schema
# ===============================
wolf_schema = StructType([
    StructField("type", StringType(), True),
    StructField("region", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("version", StringType(), True),

    StructField("PGA", DoubleType(), True),
    StructField("PGV", DoubleType(), True),
    StructField("PGD", DoubleType(), True),

    StructField("PGA_EW", DoubleType(), True),
    StructField("PGV_EW", DoubleType(), True),
    StructField("PGD_EW", DoubleType(), True),

    StructField("PGA_NS", DoubleType(), True),
    StructField("PGV_NS", DoubleType(), True),
    StructField("PGD_NS", DoubleType(), True),

    StructField("PGA_UD", DoubleType(), True),
    StructField("PGV_UD", DoubleType(), True),
    StructField("PGD_UD", DoubleType(), True),

    StructField("Max_PGA", DoubleType(), True),
    StructField("Max_PGV", DoubleType(), True),
    StructField("Max_PGD", DoubleType(), True),

    StructField("Shindo", StringType(), True),
    StructField("Max_Shindo", StringType(), True),
    StructField("CalcShindo", DoubleType(), True),
    StructField("Max_CalcShindo", DoubleType(), True),

    StructField("Intensity", DoubleType(), True),
    StructField("Max_Intensity", DoubleType(), True),

    StructField("LPGM", DoubleType(), True),
    StructField("Max_LPGM", DoubleType(), True),

    StructField("Sva30", DoubleType(), True),
    StructField("Max_Sva30", DoubleType(), True),

    StructField("private", BooleanType(), True),
    StructField("High_Precision", BooleanType(), True),

    StructField("update_at", StringType(), True),
    StructField("create_at", StringType(), True)
])

# =====================================================
# Create and configure Spark Session
# =====================================================
def create_spark_session(app_name="WolfSeismicConsumer"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4") 
        .config("spark.ui.prometheus.enabled", "true") \
        .config("spark.executor.processTreeMetrics.enabled", "true") \

        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# =====================================================
# Read streaming data from Kafka
# =====================================================
def read_wolf_kafka(spark, bootstrap_servers, topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        # Read from the earliest offsets so existing topic data appears
        .option("startingOffsets", "earliest")
        .load()
    )

# =====================================================
# Extract Kafka value as raw string
# =====================================================
def parse_kafka_value(df):
    return df.selectExpr(
        "CAST(value AS STRING) as raw_value",
        "timestamp as kafka_timestamp"
    )

# =====================================================
# Apply Wolf schema and flatten structure
# =====================================================
def apply_wolf_schema(df, wolf_schema):
    return df.withColumn("wolf", from_json(col("raw_value"), wolf_schema)) \
             .select("wolf.*", "kafka_timestamp")

# =====================================================
# Convert timestamp fields to Spark TimestampType
# =====================================================
def cast_wolf_timestamps(df):
    return df.withColumn("create_at_ts", to_timestamp("create_at")) \
             .withColumn("update_at_ts", to_timestamp("update_at"))

# =====================================================
# Clean data
# =====================================================
def clean_wolf_data(df):
    return df.filter(col("latitude").isNotNull()) \
             .filter(col("longitude").isNotNull())

# =====================================================
# Main application entry point
# =====================================================
def main():
    print("=" * 80)
    print("Starting Wolf Seismic Spark Streaming Consumer")
    print("=" * 80)

    spark = create_spark_session()

    kafka_df = read_wolf_kafka(spark, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    parsed_df = parse_kafka_value(kafka_df)
    structured_df = apply_wolf_schema(parsed_df, wolf_schema)
    typed_df = cast_wolf_timestamps(structured_df)
    clean_df = clean_wolf_data(typed_df)

    query = (
        clean_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Wolf seismic stream is running...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
