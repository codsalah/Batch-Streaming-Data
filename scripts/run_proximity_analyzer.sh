#!/bin/bash
# Submit Proximity Analyzer to Spark
echo "Submitting Proximity Analyzer to Spark Master..."

# Resolve paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load environment variables from .env if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo "Loading environment variables from $PROJECT_ROOT/.env"
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
fi

# Use environment variables with defaults
SPARK_MASTER="${SPARK_MASTER_CONTAINER:-spark-master}"
SPARK_PORT="${SPARK_MASTER_PORT:-7077}"

docker exec -u root "${SPARK_MASTER}" /opt/spark/bin/spark-submit \
  --master "spark://${SPARK_MASTER}:${SPARK_PORT}" \
  --total-executor-cores 1 \
  --packages io.delta:delta-spark_2.12:3.0.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  /opt/spark/consumers/proximity_analyzer.py
