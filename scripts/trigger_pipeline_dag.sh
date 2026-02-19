# Resolve paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load environment variables from .env if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo "Loading environment variables from $PROJECT_ROOT/.env"
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
fi

# Use environment variables with defaults
AIRFLOW_CONTAINER="${AIRFLOW_WEBSERVER_CONTAINER:-airflow-webserver}"

echo "Installing Python dependencies for producers..."
pip install --user websockets kafka-python python-dotenv 2>/dev/null || true

echo "Triggering seismic infrastructure validation DAG..."
docker exec "${AIRFLOW_CONTAINER}" airflow dags trigger seismic_infra_validation_dag

echo "Triggering seismic pipeline lifecycle DAG..."
docker exec "${AIRFLOW_CONTAINER}" airflow dags trigger seismic_pipeline_lifecycle_dag

echo "Triggering airport batch DAG..."
docker exec "${AIRFLOW_CONTAINER}" airflow dags trigger airport_batch_dag

echo "All DAGs triggered successfully!"
exit 0
 
