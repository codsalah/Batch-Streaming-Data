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

# Find Airflow CLI
if command -v airflow >/dev/null 2>&1; then
    AIRFLOW_CMD="airflow"
elif [[ -f "$HOME/.local/bin/airflow" ]]; then
    AIRFLOW_CMD="$HOME/.local/bin/airflow"
elif [[ -f "/usr/local/bin/airflow" ]]; then
    AIRFLOW_CMD="/usr/local/bin/airflow"
else
    echo "ERROR: Airflow CLI not found"
    exit 1
fi

echo "Airflow CLI found at: $AIRFLOW_CMD"
echo

# Trigger each DAG
for DAG_ID in "${DAGS[@]}"; do
    echo "Checking DAG: $DAG_ID"
    $AIRFLOW_CMD dags list | grep -q "$DAG_ID" || {
        echo "ERROR: DAG '$DAG_ID' not found in Airflow"
        continue
    }
    echo "Triggering DAG: $DAG_ID"
    $AIRFLOW_CMD dags trigger "$DAG_ID"
    echo "-------------------------------------------"
done

echo
echo "All specified DAGs triggered"
echo "Monitor at: http://localhost:8080"
echo "==========================================="
