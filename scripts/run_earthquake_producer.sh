#!/bin/bash
# Cross-platform Earthquake Producer Starter
echo "==========================================="
echo "Earthquake Data Producer"
echo "Platform: $(uname -s)"
echo "==========================================="

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
LOG_DIR="$PROJECT_ROOT/local_logs"
PID_FILE="$LOG_DIR/earthquake_producer.pid"
LOG_FILE="$LOG_DIR/earthquake_producer.log"

# Create logs directory
mkdir -p "$LOG_DIR"

# Function to check if process exists (cross-platform)
check_process() {
    local pid=$1
    # Try kill -0 (works on Unix-like systems)
    if kill -0 "$pid" 2>/dev/null; then
        return 0
    fi
    
    # Try ps command
    if command -v ps > /dev/null 2>&1; then
        if ps -p "$pid" > /dev/null 2>&1; then
            return 0
        fi
    fi
    
    # Windows Git Bash alternative
    if [[ "$(uname -s)" == MINGW* ]] && command -v tasklist > /dev/null 2>&1; then
        if tasklist //fi "PID eq $pid" 2>/dev/null | grep -q "$pid"; then
            return 0
        fi
    fi
    
    return 1
}

echo ""
echo "=== Stopping Previous Instance ==="
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE" 2>/dev/null)
    if [ -n "$OLD_PID" ] && check_process "$OLD_PID"; then
        echo "Stopping existing producer (PID: $OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null
        sleep 2
        if check_process "$OLD_PID"; then
            kill -9 "$OLD_PID" 2>/dev/null
            echo "Force stopped"
        fi
    fi
    rm -f "$PID_FILE"
fi

echo ""
echo "=== Finding Producer Script ==="
# Try to find the producer script
PRODUCER_SCRIPT=""
if [ -f "Kafka/producer-earthquakes.py" ]; then
    PRODUCER_SCRIPT="Kafka/producer-earthquakes.py"
elif [ -f "$PROJECT_ROOT/Kafka/producer-earthquakes.py" ]; then
    PRODUCER_SCRIPT="$PROJECT_ROOT/Kafka/producer-earthquakes.py"
elif [ -f "producer-earthquakes.py" ]; then
    PRODUCER_SCRIPT="producer-earthquakes.py"
else
    echo "ERROR: Could not find producer-earthquakes.py!"
    echo "Searched in:"
    echo "  - Kafka/producer-earthquakes.py"
    echo "  - $PROJECT_ROOT/Kafka/producer-earthquakes.py"
    echo "  - producer-earthquakes.py"
    exit 1
fi

echo "Found script: $PRODUCER_SCRIPT"

echo ""
echo "=== Checking Python ==="
if command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
    echo "Using python3"
elif command -v python &> /dev/null; then
    PYTHON_CMD="python"
    echo "Using python"
else
    echo "ERROR: Python not found!"
    exit 1
fi

echo ""
echo "=== Starting Producer ==="
echo "Log file: $LOG_FILE"

# Clear old log if exists
> "$LOG_FILE"

# Start producer (without nohup - use PYTHONUNBUFFERED instead)
export PYTHONUNBUFFERED=1
cd "$PROJECT_ROOT" && $PYTHON_CMD "$PRODUCER_SCRIPT" > "$LOG_FILE" 2>&1 &
PRODUCER_PID=$!

# Save PID to local directory (not /tmp)
echo "$PRODUCER_PID" > "$PID_FILE"
echo "Earthquake Producer started with PID: $PRODUCER_PID"

echo ""
echo "=== Verifying Startup ==="
echo "Waiting 5 seconds..."
sleep 5

if check_process "$PRODUCER_PID"; then
    echo "Earthquake Producer is running successfully"
    
    # Show log tail
    echo ""
    echo "=== Log Output (last 5 lines) ==="
    if [ -s "$LOG_FILE" ]; then
        tail -5 "$LOG_FILE"
    else
        echo "(Log file is empty)"
    fi
    
    echo ""
    echo "=== Monitoring ==="
    echo "To view logs: tail -f \"$LOG_FILE\""
    echo "To stop: kill $PRODUCER_PID"
    echo "PID saved in: $PID_FILE"
    
    exit 0
else
    echo "ERROR: Earthquake Producer failed to start"
    echo ""
    echo "=== Log Contents ==="
    if [ -f "$LOG_FILE" ]; then
        cat "$LOG_FILE"
    fi
    
    echo ""
    echo "=== Troubleshooting ==="
    echo "1. Check Python script: $PYTHON_CMD \"$PRODUCER_SCRIPT\""
    echo "2. Check dependencies: $PYTHON_CMD -m pip install kafka-python requests"
    
    rm -f "$PID_FILE"
    exit 1
fi
