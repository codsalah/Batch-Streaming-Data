
# Scripts Documentation

## Overview

The `scripts/` directory contains operational utilities for managing the full seismic data pipeline.

These scripts handle:

- System monitoring
- Kafka topic validation
- Spark job submission and restart
- Producer lifecycle management
- Airflow DAG triggering
- Environment validation
- Delta table validation
- Pipeline health checks

The scripts are designed to automate deployment, orchestration, and monitoring across Docker-based services.

---

## System Utilities

### check_system_resources.sh

Checks basic host system health:

- CPU usage
- Memory usage
- Disk usage

Used for infrastructure diagnostics.

---

## Kafka Utilities

### run_kafka_check.sh

Validates Kafka topic existence.

Used during pipeline startup to ensure required topics are available:

- `earthquake_raw`
- `wolf_seismic_stream`

---

## Producer Management

### run_earthquake_producer.sh

- Starts the Earthquake WebSocket producer
- Runs in background using `nohup`
- Uses PID file tracking
- Loads environment variables from `.env`
- Ensures only one instance runs

Logs stored in:

```

local_logs/earthquake_producer.log

```

---

### run_wolf_seismic_producer.sh

Same structure as the earthquake producer script.

Manages:

- Wolf seismic WebSocket producer
- Background execution
- PID-based lifecycle control
- Logging

Logs stored in:

```

local_logs/wolf_producer.log

```

---

### verify_producer_status.sh

Checks whether:

- Earthquake producer is running
- Wolf producer is running

Uses PID files for verification.

Used by Airflow validation workflows.

---

## Spark Job Management

### run_spark_consumer.sh

Submits Spark Structured Streaming jobs:

- Earthquake consumer
- Includes Kafka and Delta dependencies
- Runs inside Spark container

---

### run_wolf_seosmic_consumer.sh

Submits Wolf seismic streaming consumer.

Uses:

- Kafka connector
- Delta Lake integration
- Spark master configuration

---

### submit_earthquake_consumer.sh

Submits Earthquake streaming job to Spark.

- Named application: `EarthquakeStreamProcessor`
- Verifies submission via Spark Master Web UI

---

### submit_wolf_seismic_consumer.sh

Submits Wolf streaming job.

- Named application: `WolfSeismicConsumer`
- Validates running status through Spark API

---

### restart_spark_jobs.sh

Automates Spark recovery:

- Detects running Spark applications
- Stops existing jobs
- Resubmits:
  - Earthquake consumer
  - Wolf consumer

Used for quick recovery after failures.

---

### verify_job_submission.sh

Checks whether:

- Earthquake consumer is running
- Wolf consumer is running

Queries Spark Master Web UI.

---

## Batch Processing

### run_airport_batch.sh

Handles airport reference data ingestion:

- Validates CSV file
- Copies data into Spark container
- Runs Spark batch job
- Writes to Delta Lake
- Copies resulting table back to host

Uses overwrite mode.

---

### run_proximity_analyzer.sh

Submits proximity analysis job to Spark.

Performs:

- Stream-static join
- Distance computation
- Writes results to Delta Lake

---

## Pipeline Orchestration

### trigger_pipeline_dag.sh

Triggers Airflow DAGs:

- Infrastructure validation DAG
- Pipeline lifecycle DAG
- Airport batch DAG

Ensures automated pipeline execution.

---

## Environment Validation

### validate_env.sh

Checks required Python dependencies inside Airflow containers.

Automatically installs missing packages if needed.

Ensures DAG execution reliability.

---

## Delta Lake Validation

### validate_delta_tables.py

Validates Delta Lake tables:

- earthquakes
- wolf_seismic
- proximity_events

Checks:

- Table existence
- Row counts
- Recent updates (last 10 minutes)

Used for pipeline health monitoring.

---

## Pipeline Startup

### start_pipeline.sh

Verifies service connectivity:

- Kafka
- Spark
- Postgres

Confirms that core infrastructure is reachable.

---

## Purpose of This Directory

The `scripts/` folder acts as the operational control layer of the system.

It enables:

- Automated deployment
- Service orchestration
- Fault recovery
- Health validation
- Streaming job management
- Batch execution
- Pipeline monitoring

It connects infrastructure, ingestion, processing, and storage into a unified automated workflow.

