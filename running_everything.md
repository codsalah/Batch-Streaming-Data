# Comprehensive Guide: Running and Monitoring the Seismic Data Pipeline

This document provides all necessary commands to run, verify, and troubleshoot the entire data pipeline codebase.

## 1. Infrastructure Setup
Ensure all services (Kafka, Spark, Airflow, Postgres, etc.) are up and healthy.

```bash
# Start all containers in detached mode
docker compose down && docker compose up -d

# Check status of all containers
docker ps
```

## 2. Automated Execution (Airflow DAGs)
The pipeline is orchestrated via several Airflow DAGs. You can trigger them from the Airflow UI (localhost:8087) or via CLI.

### A. Infrastructure Validation
Check if Zookeeper, Kafka, and Spark Master/Workers are ready.
```bash
docker exec airflow-scheduler airflow dags unpause seismic_infra_validation_dag
docker exec airflow-scheduler airflow dags trigger seismic_infra_validation_dag
```

### B. Airport Batch Ingestion
Load the initial airport data into Delta Lake (required for proximity analysis).
```bash
docker exec airflow-scheduler airflow dags unpause airport_batch_dag
docker exec airflow-scheduler airflow dags trigger airport_batch_dag
```

### C. Full Pipeline Verification (Recommended)
This is the new consolidated DAG that runs the entire end-to-end flow:
1. Starts Earthquake & Wolf Seismic Producers.
2. Submits Spark Consumers.
3. Starts Proximity Analyzer.
4. Runs automated Delta table validation.

```bash
docker exec airflow-scheduler airflow dags unpause pipeline_verification_dag
docker exec airflow-scheduler airflow dags trigger pipeline_verification_dag
```

### D. Pipeline Lifecycle Management
Manages the lifecycle and provides self-healing (restarts failed jobs).
```bash
docker exec airflow-scheduler airflow dags unpause seismic_pipeline_lifecycle_dag
docker exec airflow-scheduler airflow dags trigger seismic_pipeline_lifecycle_dag
```

---

## 3. Manual Step-by-Step Execution
If you need to run specific components manually for debugging.

### A. Start Data Producers (Host or Container)
Producers connect to WebSocket APIs and stream raw data to Kafka.
```bash
bash scripts/run_earthquake_producer.sh
bash scripts/run_wolf_seismic_producer.sh
```

### B. Start Spark Consumers
Ingest data from Kafka topics into Delta Lake tables.
```bash
bash scripts/submit_earthquake_consumer.sh
bash scripts/submit_wolf_seismic_consumer.sh
```

### C. Start Proximity Analyzer
Joins real-time earthquake streams with static airport data.
```bash
bash scripts/run_proximity_analyzer.sh
```

---

## 4. Monitoring and Log Verification

### Producer Logs (Host)
```bash
tail -f local_logs/earthquake_producer.log
tail -f local_logs/wolf_producer.log
```

### Spark Application Status
Check if applications are active on the Spark Master.
```bash
docker exec spark-master curl -s http://localhost:8080/ | grep "Running Applications" -A 20
```

### Kafka Data Stream
Verify messages are arriving in Kafka topics.
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic earthquake_raw --max-messages 5 --from-beginning
```

---

## 5. Data Integrity and Delta Table Checks

### Automated Integrity Check
Runs a Spark job to verify row counts and recent updates (last 10m).
```bash
docker exec -u root spark-master /opt/spark/bin/spark-submit 
    --packages io.delta:delta-spark_2.12:3.0.0 
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" 
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" 
    /opt/spark/scripts/validate_delta_tables.py
```

### Manually View Table Content
Use the helper script to see the latest records sorted by timestamp.
```bash
# View Earthquakes
docker exec -u root -e VIEW_TABLE_PATH=/opt/delta-lake/tables/earthquakes spark-master 
  /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.0.0 --master local[1] /opt/spark/consumers/view_table.py

# View Proximity Events
docker exec -u root -e VIEW_TABLE_PATH=/opt/delta-lake/tables/proximity_events spark-master 
  /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.0.0 --master local[1] /opt/spark/consumers/view_table.py
```

---

## 6. Troubleshooting

| Symptom | Probable Cause | Resolution |
| :--- | :--- | :--- |
| `FileAlreadyExistsException` | Stale Spark checkpoint locks | `docker exec spark-master rm -rf /opt/delta-lake/checkpoints/*` |
| `ModuleNotFoundError` | Missing Python libs in Airflow | Ensure `_PIP_ADDITIONAL_REQUIREMENTS` is set in `docker-compose.yml` |
| `Connection refused` (Kafka) | Broker not ready | Wait for Kafka health check or restart: `docker restart kafka` |
| `No Workers found` (Spark) | Workers disconnected from Master | `docker restart spark-master spark-worker-1 spark-worker-2` |
| Empty `proximity_events` | No overlap or missing data | Ensure `airport_batch_dag` has been run at least once |
| `Permission Denied` | Container user mismatch | Run manual commands with `-u root` or `chmod -R 777` local directories |
