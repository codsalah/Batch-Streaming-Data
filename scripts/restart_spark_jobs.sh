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

# Kill existing Spark applications if any
echo "Checking for existing Spark applications on ${SPARK_MASTER}..."
SPARK_JSON=$(curl -s "http://${SPARK_MASTER}:${SPARK_WEBUI_PORT}/json/")

# Extract application IDs and kill them
APP_IDS=$(echo "$SPARK_JSON" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)

if [ -n "$APP_IDS" ]; then
    echo "Found existing applications, killing them..."
    for APP_ID in $APP_IDS; do
        echo "Killing application: $APP_ID"
        docker exec "${SPARK_MASTER}" /opt/spark/bin/spark-class org.apache.spark.deploy.Client kill "spark://${SPARK_MASTER}:${SPARK_PORT}" $APP_ID || true
    done
    sleep 5
else
    echo "No existing applications found"
fi

# Resubmit Wolf Seismic Consumer
echo "Resubmitting Wolf Seismic Consumer..."
bash "$SCRIPT_DIR/submit_wolf_seismic_consumer.sh"

# Resubmit Earthquake Consumer
echo "Resubmitting Earthquake Consumer..."
bash "$SCRIPT_DIR/submit_earthquake_consumer.sh"

echo "Spark jobs restart complete"
exit 0
