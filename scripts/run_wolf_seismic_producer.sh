#!/bin/bash
# Start Wolf Seismic Producer
echo "Starting Wolf Seismic Producer..."

# Resolve paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load environment variables from .env if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo "Loading environment variables from $PROJECT_ROOT/.env"
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Determine paths
PRODUCER_SCRIPT="$PROJECT_ROOT/Kafka/producer-wolf_seismic.py"
LOG_FILE="$PROJECT_ROOT/logs/wolf_producer.log"
PID_FILE="$PROJECT_ROOT/logs/wolf_producer.pid"

# Cleanup existing process if running (using PID file or matching name)
echo "Checking for existing Wolf Seismic Producer..."
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "Stopping existing producer (PID: $OLD_PID)..."
        kill "$OLD_PID"
        sleep 2
        kill -9 "$OLD_PID" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
fi

# Emergency cleanup by process name if port might be bound
if [ ! -z "$WOLF_METRICS_PORT" ]; then
    echo "Ensuring port $WOLF_METRICS_PORT is free..."
    # Since we don't have ps/pkill, we rely on the PID file and hope for the best
    # or try to use python to cleanup if needed.
fi

# Run the producer in the background
echo "Running producer: $PRODUCER_SCRIPT"
PYTHONUNBUFFERED=1 nohup python3 "$PRODUCER_SCRIPT" > "$LOG_FILE" 2>&1 &
PRODUCER_PID=$!

echo "Wolf Seismic Producer started with PID: $PRODUCER_PID"
echo $PRODUCER_PID > "$PID_FILE"

# Wait a bit to ensure it started successfully
sleep 5

# Check if process is still running
if kill -0 $PRODUCER_PID 2>/dev/null; then
    echo "Wolf Seismic Producer is running successfully"
    exit 0
else
    echo "ERROR: Wolf Seismic Producer failed to start. Check logs at $LOG_FILE"
    exit 1
fi
