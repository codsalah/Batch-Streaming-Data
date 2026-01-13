#!/bin/bash
echo "Installing Python dependencies for producers..."
pip install --user websockets kafka-python python-dotenv 2>/dev/null || true

echo "Triggering seismic infrastructure validation DAG..."
docker exec airflow-webserver airflow dags trigger seismic_infra_validation_dag

echo "Triggering seismic pipeline lifecycle DAG..."
docker exec airflow-webserver airflow dags trigger seismic_pipeline_lifecycle_dag

echo "Triggering airport batch DAG..."
docker exec airflow-webserver airflow dags trigger airport_batch_dag

echo "All DAGs triggered successfully!"
exit 0
 
