#!/bin/bash

echo "Submitting Airport Batch Job to Spark..."

# ============================
# Local paths
# ============================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CSV_LOCAL="$SCRIPT_DIR/../data/airports.csv"
DELTA_LOCAL="$SCRIPT_DIR/../data/delta_airports"
MODE="overwrite"

echo "CSV local path: $CSV_LOCAL"
echo "Delta table local path: $DELTA_LOCAL"

# ============================
# Docker paths (Linux inside container)
# ============================
CSV_DOCKER="/data/airports.csv"
DELTA_DOCKER="/opt/delta-lake/delta_airports"

echo "CSV path inside Docker: $CSV_DOCKER"
echo "Delta path inside Docker: $DELTA_DOCKER"

# ============================
# Check if CSV exists locally
# ============================
if [ ! -f "$CSV_LOCAL" ]; then
    echo "ERROR: CSV file does NOT exist at path: $CSV_LOCAL"
    exit 1
fi

# ============================
# Spark submit inside Docker
# ============================
docker exec -it spark-master bash -c "\
  mkdir -p /tmp/.ivy2 && chmod 777 /tmp/.ivy2 && \
  /opt/spark/bin/spark-submit \
  --conf 'spark.jars.ivy=/tmp/.ivy2' \
  --packages io.delta:delta-spark_2.12:3.0.0 \
  --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' \
  /opt/spark/consumers/airport_batch_to_delta.py \
  --csv $CSV_DOCKER \
  --delta $DELTA_DOCKER \
  --mode $MODE
"


echo "Spark submit command executed inside Docker"

 
