#!/bin/bash

echo "Starting Seismic Data Pipeline Infrastructure..."
echo "Platform: $(uname -s)"
echo "==========================================="

check_port() {
    local host=$1
    local port=$2
    local service=$3
    
    echo "Checking $service ($host:$port)..."
    
    if [[ "$OSTYPE" == "linux-gnu"* || "$OSTYPE" == "darwin"* ]]; then
        # Linux/Mac
        if command -v nc &> /dev/null; then
            timeout 2 nc -z $host $port >/dev/null 2>&1
            if [ $? -eq 0 ]; then
                echo "✓ $service is reachable"
            else
                echo "✗ Warning: $service might not be reachable"
            fi
        elif command -v telnet &> /dev/null; then
            timeout 2 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null
            if [ $? -eq 0 ]; then
                echo "✓ $service is reachable"
            else
                echo "✗ Warning: $service might not be reachable"
            fi
        else
            echo "? Cannot check $service (install netcat or telnet)"
        fi
    elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then
        # Windows
        if command -v nc &> /dev/null; then
            timeout 2 nc -z $host $port >/dev/null 2>&1
            if [ $? -eq 0 ]; then
                echo "✓ $service is reachable"
            else
                echo "✗ Warning: $service might not be reachable"
            fi
        elif command -v curl &> /dev/null; then
            
            timeout 2 curl -s "http://$host:$port" >/dev/null 2>&1
            if [ $? -eq 0 ]; then
                echo "✓ $service is reachable"
            else
                echo "✗ Warning: $service might not be reachable (tried curl)"
            fi
        else
            
            if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
                powershell -Command "Test-NetConnection -ComputerName $host -Port $port -InformationLevel Quiet" >/dev/null 2>&1
                if [ $? -eq 0 ]; then
                    echo "✓ $service is reachable (PowerShell)"
                else
                    echo "✗ Warning: $service might not be reachable"
                fi
            else
                echo "? Install netcat or curl to check $service"
            fi
        fi
    fi
}

 
check_port "localhost" "9092" "Kafka"
check_port "localhost" "7077" "Spark Master"
check_port "localhost" "5432" "PostgreSQL"
check_port "localhost" "8080" "Airflow Web UI"
check_port "localhost" "8888" "Jupyter Notebook"

echo -e "\nChecking Docker services..."
if command -v docker &> /dev/null; then
    if docker ps --format "table {{.Names}}\t{{.Status}}" | grep -q "kafka\|postgres\|spark"; then
        echo "Docker services are running:"
        docker ps --format "{{.Names}}: {{.Status}}" | grep -E "kafka|postgres|spark|airflow"
    else
        echo "Starting Docker services..."
        docker-compose up -d 2>/dev/null || echo "docker-compose not found or failed"
    fi
else
    echo "Docker not installed or not in PATH"
fi

echo -e "\n==========================================="
echo "Pipeline startup initialized."
echo "Run './trigger_pipeline_dag.sh' to start the pipeline."
