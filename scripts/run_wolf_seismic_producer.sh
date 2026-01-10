#!/bin/bash
# Start Wolf Seismic Producer
echo "Starting Wolf Seismic Producer..."
echo "Platform: $(uname -s)"
echo "==========================================="

# Determine the correct paths based on OS and project structure
# Project root is one level up from scripts directory
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"
PID_FILE="$LOG_DIR/wolf_producer.pid"
LOG_FILE="$LOG_DIR/wolf_producer.log"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Stop any existing producer process
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE" 2>/dev/null)
    if [ -n "$OLD_PID" ]; then
        echo "Stopping existing producer (PID: $OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null || true
        sleep 2
    fi
    rm -f "$PID_FILE"
fi

# Try different possible locations for the producer script
PRODUCER_SCRIPT=""
if [ -f "$PROJECT_ROOT/src/producers/wolf_seismic_producer.py" ]; then
    PRODUCER_SCRIPT="$PROJECT_ROOT/src/producers/wolf_seismic_producer.py"
elif [ -f "$PROJECT_ROOT/Kafka/producer-wolf_seismic.py" ]; then
    PRODUCER_SCRIPT="$PROJECT_ROOT/Kafka/producer-wolf_seismic.py"
elif [ -f "/opt/airflow/Kafka/producer-wolf_seismic.py" ]; then
    PRODUCER_SCRIPT="/opt/airflow/Kafka/producer-wolf_seismic.py"
else
    echo "ERROR: Could not find producer script!"
    echo "Searched in:"
    echo "  - $PROJECT_ROOT/src/producers/wolf_seismic_producer.py"
    echo "  - $PROJECT_ROOT/Kafka/producer-wolf_seismic.py"
    echo "  - /opt/airflow/Kafka/producer-wolf_seismic.py"
    exit 1
fi

echo "Found producer script: $PRODUCER_SCRIPT"
echo "Log file: $LOG_FILE"

# Run the producer in the background
echo "Starting producer..."
if command -v python3 &> /dev/null; then
    python3 "$PRODUCER_SCRIPT" > "$LOG_FILE" 2>&1 &
else
    python "$PRODUCER_SCRIPT" > "$LOG_FILE" 2>&1 &
fi
PRODUCER_PID=$!

echo "Wolf Seismic Producer started with PID: $PRODUCER_PID"
echo $PRODUCER_PID > "$PID_FILE"

# Wait a bit to ensure it started successfully
sleep 5

# Check if process is still running
if ps -p $PRODUCER_PID > /dev/null 2>&1 || kill -0 $PRODUCER_PID 2>/dev/null; then
    echo "✓ Wolf Seismic Producer is running successfully"
    echo "To view logs: tail -f \"$LOG_FILE\""
    echo "To stop: kill $PRODUCER_PID or remove $PID_FILE"
    exit 0
else
    echo "✗ ERROR: Wolf Seismic Producer failed to start"
    echo "Check the log file for errors:"
    echo "  cat \"$LOG_FILE\""
    rm -f "$PID_FILE"
    exit 1
fi
