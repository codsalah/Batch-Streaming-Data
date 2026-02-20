#!/bin/bash
# =====================================================
# Script to run Wolf Seismic Spark Streaming Consumer
# =====================================================

echo "=================================================="
echo "Starting Wolf Seismic Spark Streaming Consumer"
echo "=================================================="

# Resolve paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load environment variables from .env if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo "Loading environment variables from $PROJECT_ROOT/.env"
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
fi

# Use environment variables with defaults
SPARK_CONTAINER="${SPARK_MASTER_CONTAINER:-spark-master}"
SPARK_PORT="${SPARK_MASTER_PORT:-7077}"

# Path to Spark submit inside container
SPARK_SUBMIT="/opt/spark/bin/spark-submit"

# Spark application script path
APP_PATH="/opt/spark/consumers/spark_wolf_seismic_consumer.py"

# Required Kafka package version (match Spark version)
# Required Kafka and Delta Lake packages
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0"

# Execute Spark Streaming job with a writable Ivy cache
docker exec ${SPARK_CONTAINER} \
  ${SPARK_SUBMIT} \
  --packages ${KAFKA_PACKAGE} \
  --master "spark://${SPARK_CONTAINER}:${SPARK_PORT}" \
  --deploy-mode client \
  --conf spark.cores.max=2 \
  --conf spark.executor.memory=1g \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  ${APP_PATH}

echo "=================================================="
echo "Spark Streaming Consumer stopped"
echo "=================================================="
