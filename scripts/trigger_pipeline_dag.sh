#!/usr/bin/env bash
set -e

echo "==========================================="
echo "Triggering all Seismic Pipeline DAGs"
echo "Platform: $(uname -s)"
echo "==========================================="

# List of DAGs to trigger
DAGS=(
    "seismic_infra_validation_dag"
    "seismic_pipeline_lifecycle_dag"
    "airport_batch_dag"  
)

# Airflow container name
AIRFLOW_CONTAINER="airflow-webserver"

# Trigger each DAG
for DAG_ID in "${DAGS[@]}"; do
    echo "Checking DAG: $DAG_ID"
    docker exec -it $AIRFLOW_CONTAINER airflow dags list | grep -q "$DAG_ID" || {
        echo "ERROR: DAG '$DAG_ID' not found in Airflow"
        continue
    }
    echo "Triggering DAG: $DAG_ID"
    docker exec -it $AIRFLOW_CONTAINER airflow dags trigger "$DAG_ID"
    echo "-------------------------------------------"
done

echo
echo "All specified DAGs triggered"
echo "Monitor at: http://localhost:8080"
echo "==========================================="
