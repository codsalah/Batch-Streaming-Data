#!/bin/bash
# Start Earthquake Producer
echo "Starting Earthquake Producer..."

# Determine log file location
LOG_FILE="/opt/airflow/logs/earthquake_producer.log"

# Run the producer in the background
PYTHONUNBUFFERED=1 nohup python3 /opt/airflow/Kafka/producer-earthquakes.py > "$LOG_FILE" 2>&1 &
PRODUCER_PID=$!

echo "Earthquake Producer started with PID: $PRODUCER_PID"
echo $PRODUCER_PID > /tmp/earthquake_producer.pid

# Wait a bit to ensure it started successfully
sleep 5

# Check if process is still running
if kill -0 $PRODUCER_PID 2>/dev/null; then
    echo "Earthquake Producer is running successfully"
    exit 0
else
    echo "ERROR: Earthquake Producer failed to start"
    exit 1
fi
