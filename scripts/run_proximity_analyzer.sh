#!/bin/bash
# Submit Proximity Analyzer to Spark
echo "Submitting Proximity Analyzer to Spark Master..."

docker exec -u root spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --total-executor-cores 1 \
  --packages io.delta:delta-spark_2.12:3.0.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  /opt/spark/consumers/proximity_analyzer.py
