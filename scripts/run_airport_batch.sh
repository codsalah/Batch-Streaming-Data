#!/bin/bash
set -e    # Exit immediately if any command fails

echo "=== Airport Batch Job Start ==="
date

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
KAFKA_PORT="${KAFKA_INTERNAL_PORT:-9092}"

CSV_LOCAL="$PROJECT_ROOT/data/airports.csv"
DELTA_LOCAL="$PROJECT_ROOT/data/delta_airports"

echo "Project root: $PROJECT_ROOT"
echo "Spark master container: $SPARK_MASTER"
echo "CSV local: $CSV_LOCAL"
echo "Delta local: $DELTA_LOCAL"

# Validate CSV
if [ ! -f "$CSV_LOCAL" ]; then
    echo "ERROR: CSV file not found at $CSV_LOCAL"
    ls -la "$PROJECT_ROOT/data/" || true
    exit 1
fi

# Validate Spark container
echo "Checking $SPARK_MASTER container..."
if ! docker ps --format '{{.Names}}' | grep -q "^${SPARK_MASTER}$"; then
    echo "ERROR: $SPARK_MASTER container is not running"
    docker ps
    exit 1
fi

# Copy CSV to container
echo "Copying CSV to $SPARK_MASTER..."
docker cp "$CSV_LOCAL" "$SPARK_MASTER":/opt/spark/work-dir/airports.csv

# Run Spark job
echo "Submitting Spark job..."
docker exec "$SPARK_MASTER" bash -c "
set -e

if [ ! -f /opt/spark/work-dir/airports.csv ]; then
    echo 'ERROR: CSV not found inside container'
    exit 1
fi

/opt/spark/bin/spark-submit \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark/consumers/airport_batch_to_delta.py \
  --csv /opt/spark/work-dir/airports.csv \
  --delta /opt/delta-lake/delta_airports \
  --mode overwrite
"

SPARK_EXIT=$?
echo "Spark job completed with exit code: $SPARK_EXIT"

# Copy Delta table back if success
if [ $SPARK_EXIT -eq 0 ]; then
    echo "Copying Delta table from $SPARK_MASTER..."
    rm -rf "$DELTA_LOCAL"
    docker cp "$SPARK_MASTER":/opt/delta-lake/delta_airports "$DELTA_LOCAL"

    echo "Delta table copied successfully:"
    ls -la "$DELTA_LOCAL"
else
    echo "Spark job failed — Delta table not copied"
fi

echo "=== Airport Batch Job End ==="
date
exit $SPARK_EXIT
