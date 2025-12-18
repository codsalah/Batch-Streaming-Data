#!/bin/bash
# Restart Spark Jobs (Self-Healing)
echo "Restarting Spark jobs..."

# Kill existing Spark applications if any
echo "Checking for existing Spark applications..."
SPARK_JSON=$(curl -s http://spark-master:8080/json/)

# Extract application IDs and kill them
APP_IDS=$(echo "$SPARK_JSON" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)

if [ -n "$APP_IDS" ]; then
    echo "Found existing applications, killing them..."
    for APP_ID in $APP_IDS; do
        echo "Killing application: $APP_ID"
        docker exec spark-master /opt/spark/bin/spark-class org.apache.spark.deploy.Client kill spark://spark-master:7077 $APP_ID || true
    done
    sleep 5
else
    echo "No existing applications found"
fi

# Resubmit Wolf Seismic Consumer
echo "Resubmitting Wolf Seismic Consumer..."
bash /opt/airflow/scripts/submit_wolf_seismic_consumer.sh

# Resubmit Earthquake Consumer
echo "Resubmitting Earthquake Consumer..."
bash /opt/airflow/scripts/submit_earthquake_consumer.sh

echo "Spark jobs restart complete"
exit 0
