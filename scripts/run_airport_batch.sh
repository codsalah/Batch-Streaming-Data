#!/bin/bash
set -e

echo "=== Airport Batch Job Start ==="
date
 
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

CSV_LOCAL="$PROJECT_ROOT/data/airports.csv"
PYTHON_CONSUMER="$PROJECT_ROOT/spark-consumers/airport_batch_to_delta.py"
DELTA_LOCAL="$PROJECT_ROOT/delta-lake/tables/airports"

SPARK_MASTER="${SPARK_MASTER_CONTAINER:-spark-master}"

echo "Project root: $PROJECT_ROOT"
echo "CSV local: $CSV_LOCAL"
echo "Delta local: $DELTA_LOCAL"
echo "Spark master container: $SPARK_MASTER"

if [ ! -f "$CSV_LOCAL" ]; then
    echo "ERROR: CSV file not found at $CSV_LOCAL"
    ls -la "$PROJECT_ROOT/data/" || true
    exit 1
fi

UTF8_CSV="$PROJECT_ROOT/data/airports_utf8.csv"
iconv -f UTF-8 -t UTF-8 "$CSV_LOCAL" -o "$UTF8_CSV" || cp "$CSV_LOCAL" "$UTF8_CSV"
echo "CSV encoding ensured (UTF-8)"

if ! docker ps --format '{{.Names}}' | grep -q "^${SPARK_MASTER}$"; then
    echo "ERROR: $SPARK_MASTER container is not running"
    docker ps
    exit 1
fi

# Create delta directory in container with proper permissions (as root)
echo "Creating delta directory in container with proper permissions..."
docker exec -u root "$SPARK_MASTER" bash -c "
    mkdir -p /opt/delta-lake/tables/airports && \
    chmod -R 777 /opt/delta-lake/tables/airports
"

# Copy files to container
docker cp "$UTF8_CSV" "$SPARK_MASTER":/opt/spark/work-dir/airports.csv
docker cp "$PYTHON_CONSUMER" "$SPARK_MASTER":/opt/spark/consumers/airport_batch_to_delta.py

 
docker exec "$SPARK_MASTER" bash -c "
set -e
echo 'Inside container: validating CSV...'
if [ ! -f /opt/spark/work-dir/airports.csv ]; then
    echo 'ERROR: CSV not found inside container'
    exit 1
fi

/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark/consumers/airport_batch_to_delta.py
"

SPARK_EXIT=$?
echo "Spark job completed with exit code: $SPARK_EXIT"

if [ $SPARK_EXIT -eq 0 ]; then
    echo "Copying Delta table from container's temp directory..."
    sudo rm -rf "$DELTA_LOCAL"
    sudo mkdir -p "$DELTA_LOCAL"
    #/tmp/delta-airports
    sudo docker cp "$SPARK_MASTER":/tmp/delta-airports/. "$DELTA_LOCAL/"
    sudo chown -R $(whoami):$(whoami) "$DELTA_LOCAL"
    echo "Delta table copied successfully:"
    ls -la "$DELTA_LOCAL"
    docker exec "$SPARK_MASTER" rm -rf /tmp/delta-airports
else
    echo "Spark job failed — Delta table not copied"
fi

echo "=== Airport Batch Job End ==="
date
exit $SPARK_EXIT