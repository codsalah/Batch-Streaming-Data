# Resolve paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load environment variables from .env if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo "Loading environment variables from $PROJECT_ROOT/.env"
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
fi

# Use environment variables with defaults
KAFKA_PORT="${KAFKA_INTERNAL_PORT:-9092}"
SPARK_PORT="${SPARK_MASTER_PORT:-7077}"
POSTGRES_PORT_DEFAULT="${POSTGRES_PORT:-5432}"

services="kafka:${KAFKA_PORT} spark-master:${SPARK_PORT} postgres:${POSTGRES_PORT_DEFAULT}"

for service_port in $services; do
    host=${service_port%:*}
    port=${service_port#*:}
    
    if nc -z -w 2 "$host" "$port"; then
        echo "Service $host:$port is reachable."
    else
        echo "Warning: Service $host:$port might not be reachable from this container."
    fi
done
echo "Pipeline startup initialized."
