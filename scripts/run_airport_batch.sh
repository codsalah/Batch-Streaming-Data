#!/bin/bash
# Final working version for Git Bash

echo "Running Spark Batch Job..."

 
docker exec spark-master bash -c '
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    /opt/spark/work-dir/airport_batch_to_delta.py
'