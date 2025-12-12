#!/bin/bash
# Script to run Spark Streaming Consumer

echo "=========================================="
echo "Starting Spark Streaming Consumer"
echo "=========================================="

# Run spark-submit with required packages
docker exec spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master local[*] \
  /opt/spark/scripts/spark_consumer.py
