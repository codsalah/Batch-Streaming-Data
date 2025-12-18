#!/bin/bash
# Start Wolf Seismic Producer
echo "Starting Wolf Seismic Producer..."

# Run the producer in the background
nohup python3 /opt/airflow/Kafka/producer-wolf_seismic.py > /opt/airflow/logs/wolf_producer.log 2>&1 &
PRODUCER_PID=$!

echo "Wolf Seismic Producer started with PID: $PRODUCER_PID"
echo $PRODUCER_PID > /tmp/wolf_producer.pid

# Wait a bit to ensure it started successfully
sleep 5

# Check if process is still running
if kill -0 $PRODUCER_PID 2>/dev/null; then
    echo "Wolf Seismic Producer is running successfully"
    exit 0
else
    echo "ERROR: Wolf Seismic Producer failed to start"
    exit 1
fi
