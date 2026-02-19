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
SPARK_WEBUI_PORT="${SPARK_MASTER_WEBUI_PORT:-8080}"

# Query Spark Master for running applications
SPARK_JSON=$(curl -s "http://${SPARK_MASTER}:${SPARK_WEBUI_PORT}/json/")

# Check for Wolf Seismic Consumer
if echo "$SPARK_JSON" | grep -q "WolfSeismicConsumer"; then
    echo "✓ WolfSeismicConsumer is running on Spark"
    WOLF_RUNNING=1
else
    echo "✗ WolfSeismicConsumer is NOT running on Spark"
    WOLF_RUNNING=0
fi

# Check for Earthquake Stream Processor
if echo "$SPARK_JSON" | grep -q "EarthquakeStreamProcessor"; then
    echo "✓ EarthquakeStreamProcessor is running on Spark"
    EARTHQUAKE_RUNNING=1
else
    echo "✗ EarthquakeStreamProcessor is NOT running on Spark"
    EARTHQUAKE_RUNNING=0
fi

# Report status
if [ $WOLF_RUNNING -eq 1 ] && [ $EARTHQUAKE_RUNNING -eq 1 ]; then
    echo "All Spark jobs are running successfully"
    exit 0
else
    echo "WARNING: Not all Spark jobs are running yet"
    # Don't fail - jobs might still be starting
    exit 0
fi
