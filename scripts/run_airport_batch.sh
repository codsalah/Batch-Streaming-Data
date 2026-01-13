#!/bin/bash
set -e    # Exit immediately if any command fails

# Print start message and current date/time
echo "=== Airport Batch Job Start ==="
date

# Get the directory where this script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Get the project root directory (parent of script dir)
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Define paths for input CSV and output Delta table
CSV_LOCAL="$PROJECT_ROOT/data/airports.csv"
DELTA_LOCAL="$PROJECT_ROOT/data/delta_airports"

# Print paths for debugging
echo "Project root: $PROJECT_ROOT"
echo "CSV local: $CSV_LOCAL"
echo "Delta local: $DELTA_LOCAL"

# Check if the CSV file exists
if [ ! -f "$CSV_LOCAL" ]; then
    echo "ERROR: CSV file not found at: $CSV_LOCAL"
    echo "Available files in data directory:"
    ls -la "$PROJECT_ROOT/data/" || true  # List files if CSV missing
    exit 1  # Stop script if CSV not found
fi

# Check if the Docker container 'spark-master' is running
echo "Checking spark-master container..."
if ! docker ps --format '{{.Names}}' | grep -q 'spark-master'; then
    echo "ERROR: spark-master container is not running"
    echo "Running containers:"
    docker ps --format '{{.Names}}'  # Show running containers
    exit 1
fi

# Copy the CSV file to the spark-master container
echo "Copying CSV to spark-master container..."
docker cp "$CSV_LOCAL" spark-master:/data/airports.csv
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to copy CSV to spark-master"
    exit 1
fi

# Run Spark job inside the container
echo "Submitting Spark job..."
docker exec spark-master bash -c "
    echo 'Inside spark-master container...'
    echo 'Current directory: \$(pwd)'

    # Check CSV inside container
    if [ ! -f /data/airports.csv ]; then
        echo 'ERROR: /data/airports.csv not found in container'
        exit 1
    fi

    # Submit Spark job to convert CSV to Delta table
    /opt/spark/bin/spark-submit \
        --conf 'spark.jars.ivy=/tmp/.ivy2' \
        --packages io.delta:delta-spark_2.12:3.0.0 \
        --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' \
        --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' \
        /opt/spark/consumers/airport_batch_to_delta.py \
        --csv /data/airports.csv \
        --delta /opt/delta-lake/delta_airports \
        --mode overwrite

    SPARK_EXIT=\$?  # Capture Spark job exit code
    echo 'Spark job exited with code: \$SPARK_EXIT'

    # Check if Delta table folder was created
    if [ -d '/opt/delta-lake/delta_airports' ]; then
        echo 'Delta files created:'
        ls -la '/opt/delta-lake/delta_airports/'
    else
        echo 'WARNING: Delta directory not created'
    fi

    exit \$SPARK_EXIT  # Exit container script with Spark job code
"

SPARK_EXIT=$?  # Get Spark job exit code on host
echo "Spark job completed with exit code: $SPARK_EXIT"

# If Spark job succeeded, copy Delta table back to host
if [ $SPARK_EXIT -eq 0 ]; then
    echo "Copying Delta table from spark-master..."
    docker cp spark-master:/opt/delta-lake/delta_airports/ "$DELTA_LOCAL" 2>/dev/null || true

    if [ -d "$DELTA_LOCAL" ]; then
        echo "Delta table copied successfully"
        echo "Files in delta_airports:"
        ls -la "$DELTA_LOCAL/"
    else
        echo "WARNING: Could not copy Delta table from container"
    fi
fi

echo "=== Airport Batch Job End ==="
date
exit $SPARK_EXIT
