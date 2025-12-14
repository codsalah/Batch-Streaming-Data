#!/bin/bash
# =====================================================
# Script to run Wolf Seismic Spark Streaming Consumer
# =====================================================

echo "=================================================="
echo "Starting Wolf Seismic Spark Streaming Consumer"
echo "=================================================="

# Docker container name (Spark Master)
SPARK_CONTAINER="spark-master"

# Path to Spark submit inside container
SPARK_SUBMIT="/opt/spark/bin/spark-submit"

# Spark application script path
APP_PATH="/opt/spark/scripts/spark_wolf_seismic_concumer.py"

# Required Kafka package version (match Spark version)
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

# Execute Spark Streaming job with a writable Ivy cache
docker exec -it ${SPARK_CONTAINER} \
  ${SPARK_SUBMIT} \
  --packages ${KAFKA_PACKAGE} \
  --master local[*] \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  ${APP_PATH}

echo "=================================================="
echo "Spark Streaming Consumer stopped"
echo "=================================================="
