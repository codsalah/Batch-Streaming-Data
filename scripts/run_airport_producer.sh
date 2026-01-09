#!/bin/bash
# Start Airport Producer
echo "Starting Airport Producer..."

# Determine log file location
LOG_FILE="local_logs/airport_producer.log"
mkdir -p local_logs

# Run the producer in the background
PYTHONUNBUFFERED=1 nohup python3 Kafka/producer-airport.py > "$LOG_FILE" 2>&1 &
PRODUCER_PID=$!

echo "Airport Producer started with PID: $PRODUCER_PID"
echo $PRODUCER_PID > /tmp/airport_producer.pid

# Wait a bit to ensure it started successfully
sleep 5

# Check if process is still running
if kill -0 $PRODUCER_PID 2>/dev/null; then
    echo "Airport Producer is running successfully"
    exit 0
else
    echo "ERROR: Airport Producer failed to start"
    echo "Check logs at $LOG_FILE"
    exit 1
fi
