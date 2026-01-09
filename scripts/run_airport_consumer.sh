#!/bin/bash
# Submit Airport Consumer to Spark
echo "Submitting Airport Stream Consumer to Spark Master..."

docker exec -u root spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --total-executor-cores 1 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0 \
  /opt/spark/consumers/spark_airport_consumer.py &
