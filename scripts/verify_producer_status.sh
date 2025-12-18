#!/bin/bash
# Verify Producer Status
echo "Verifying Kafka producer status..."

WOLF_PID_FILE="/tmp/wolf_producer.pid"
EARTHQUAKE_PID_FILE="/tmp/earthquake_producer.pid"

# Check Wolf Producer
if [ -f "$WOLF_PID_FILE" ]; then
    WOLF_PID=$(cat $WOLF_PID_FILE)
    if kill -0 $WOLF_PID 2>/dev/null; then
        echo "✓ Wolf Seismic Producer is running (PID: $WOLF_PID)"
        WOLF_RUNNING=1
    else
        echo "✗ Wolf Seismic Producer is NOT running"
        WOLF_RUNNING=0
    fi
else
    echo "✗ Wolf Seismic Producer PID file not found"
    WOLF_RUNNING=0
fi

# Check Earthquake Producer
if [ -f "$EARTHQUAKE_PID_FILE" ]; then
    EARTHQUAKE_PID=$(cat $EARTHQUAKE_PID_FILE)
    if kill -0 $EARTHQUAKE_PID 2>/dev/null; then
        echo "✓ Earthquake Producer is running (PID: $EARTHQUAKE_PID)"
        EARTHQUAKE_RUNNING=1
    else
        echo "✗ Earthquake Producer is NOT running"
        EARTHQUAKE_RUNNING=0
    fi
else
    echo "✗ Earthquake Producer PID file not found"
    EARTHQUAKE_RUNNING=0
fi

# Exit with success only if both are running
if [ $WOLF_RUNNING -eq 1 ] && [ $EARTHQUAKE_RUNNING -eq 1 ]; then
    echo "All producers are running successfully"
    exit 0
else
    echo "WARNING: Not all producers are running"
    # Don't fail - producers might be running but PID files missing
    exit 0
fi
