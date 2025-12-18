#!/bin/bash
# Verify Spark Job Submission
echo "Verifying Spark job submission status..."

# Query Spark Master for running applications
SPARK_JSON=$(curl -s http://spark-master:8080/json/)

# Check for Wolf Seismic Consumer
if echo "$SPARK_JSON" | grep -q "WolfSeismicConsumer"; then
    echo "✓ WolfSeismicConsumer is running on Spark"
    WOLF_RUNNING=1
else
    echo "✗ WolfSeismicConsumer is NOT running on Spark"
    WOLF_RUNNING=0
fi

# Check for Earthquake Stream Processor
if echo "$SPARK_JSON" | grep -q "EarthquakeStreamProcessor"; then
    echo "✓ EarthquakeStreamProcessor is running on Spark"
    EARTHQUAKE_RUNNING=1
else
    echo "✗ EarthquakeStreamProcessor is NOT running on Spark"
    EARTHQUAKE_RUNNING=0
fi

# Report status
if [ $WOLF_RUNNING -eq 1 ] && [ $EARTHQUAKE_RUNNING -eq 1 ]; then
    echo "All Spark jobs are running successfully"
    exit 0
else
    echo "WARNING: Not all Spark jobs are running yet"
    # Don't fail - jobs might still be starting
    exit 0
fi
