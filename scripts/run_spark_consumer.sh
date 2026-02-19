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

echo "=========================================="
echo "Starting Spark Streaming Consumer on ${SPARK_MASTER}"
echo "=========================================="

# Run spark-submit with required packages
docker exec "${SPARK_MASTER}" /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0 \
  --master "spark://${SPARK_MASTER}:${SPARK_PORT}" \
  --deploy-mode client \
  --conf spark.cores.max=2 \
  --conf spark.executor.memory=1g \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark/scripts/spark_consumer.py
