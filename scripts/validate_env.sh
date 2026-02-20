#!/bin/bash

# Configuration
REQUIRED_MODULES=("websockets" "kafka-python" "python-dotenv" "prometheus-client" "requests")
AIRFLOW_CONTAINERS=("airflow-webserver" "airflow-scheduler")

echo "Starting dependency validation..."

check_container_deps() {
    local container=$1
    echo "Checking dependencies in $container..."
    
    # Check if container is running
    if ! docker ps --format '{{.Names}}' | grep -q "^$container$"; then
        echo "Error: Container $container is not running."
        return 1
    fi

    for module in "${REQUIRED_MODULES[@]}"; do
        if ! docker exec "$container" python3 -c "import $module" 2>/dev/null; then
            echo "Missing dependency in $container: $module"
            echo "Attempting to install $module..."
            docker exec -u root "$container" pip install "$module" || {
                echo "Failed to install $module in $container"
                return 1
            }
        else
            echo "OK: $module is installed in $container"
        fi
    done
}

# Run checks
for container in "${AIRFLOW_CONTAINERS[@]}"; do
    check_container_deps "$container"
done

echo "Dependency validation completed successfully!"
