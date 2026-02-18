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
SPARK_WEBUI_PORT="${SPARK_MASTER_WEBUI_PORT:-8080}"

# Submit the Spark job
docker exec -u root "${SPARK_MASTER}" spark-submit \
    --master "spark://${SPARK_MASTER}:${SPARK_PORT}" \
    --deploy-mode client \
    --name WolfSeismicConsumer \
    --total-executor-cores 1 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    /opt/spark/consumers/spark_wolf_seismic_consumer.py &

SUBMIT_PID=$!
echo "Spark submit initiated with PID: $SUBMIT_PID"

# Wait a bit for submission
sleep 10

# Check if submission was successful by querying Spark Master
if curl -s "http://${SPARK_MASTER}:${SPARK_WEBUI_PORT}/json/" | grep -q "WolfSeismicConsumer"; then
    echo "✓ Wolf Seismic Consumer submitted successfully"
    exit 0
else
    echo "WARNING: Wolf Seismic Consumer may not have started yet"
    # Don't fail Please
    exit 0
fi
