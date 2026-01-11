#!/usr/bin/env bash
set -e

echo "Submitting Wolf Seismic Consumer to Spark..."

SPARK_CONTAINER="spark-master"

echo "Detecting spark-submit inside container..."

SPARK_SUBMIT_PATH=$(docker exec "$SPARK_CONTAINER" bash -lc '
  for p in \
    /opt/spark/bin/spark-submit \
    /spark/bin/spark-submit \
    /usr/local/spark/bin/spark-submit \
    /usr/bin/spark-submit; do
      if [ -x "$p" ]; then
        echo "$p"
        exit 0
      fi
    done
  exit 1
')

if [ -z "$SPARK_SUBMIT_PATH" ]; then
  echo "ERROR: spark-submit not found inside container"
  exit 1
fi

echo "Found spark-submit at: $SPARK_SUBMIT_PATH"

docker exec -u root "$SPARK_CONTAINER" bash -lc "
  $SPARK_SUBMIT_PATH \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --name WolfSeismicConsumer \
    --total-executor-cores 1 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0 \
    --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' \
    --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' \
    /opt/spark/consumers/spark_wolf_seismic_consumer.py
"

echo "✓ Wolf Seismic Consumer Spark submit executed"

# Optional: brief wait and check
sleep 10
if curl -s http://localhost:8080 | grep -q "WolfSeismicConsumer"; then
  echo "Wolf Seismic Consumer is running"
else
  echo "WARNING: Wolf Seismic Consumer may not have started yet"
fi
