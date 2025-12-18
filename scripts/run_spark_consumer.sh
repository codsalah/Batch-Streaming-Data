#!/bin/bash
# Script to run Spark Streaming Consumer

echo "=========================================="
echo "Starting Spark Streaming Consumer"
echo "=========================================="

# Run spark-submit with required packages
# Run spark-submit with required packages
docker exec spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0 \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.cores.max=2 \
  --conf spark.executor.memory=1g \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark/scripts/spark_consumer.py
