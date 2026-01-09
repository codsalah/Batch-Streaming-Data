#!/bin/bash
# Submit Wolf Seismic Consumer to Spark
echo "Submitting Wolf Seismic Consumer to Spark..."

# Submit the Spark job
docker exec -u root spark-master spark-submit \
    --master spark://spark-master:7077 \
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
if curl -s http://spark-master:8080/json/ | grep -q "WolfSeismicConsumer"; then
    echo "✓ Wolf Seismic Consumer submitted successfully"
    exit 0
else
    echo "WARNING: Wolf Seismic Consumer may not have started yet"
    # Don't fail Please
    exit 0
fi
