#!/bin/bash
# Trigger the infrastructure validation DAG
echo "Installing Python dependencies for producers..."
pip install --user websockets kafka-python python-dotenv

echo "Triggering seismic infrastructure validation DAG..."
airflow dags trigger seismic_infra_validation_dag

echo "Pipeline initialization complete."
exit 0
