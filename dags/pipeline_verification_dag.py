from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    "pipeline_verification_dag",
    default_args=default_args,
    description="DAG to run the whole pipeline and verify data persistence",
    schedule_interval=None,
    catchup=False,
    tags=["seismic", "pipeline", "verification"],
) as dag:

    # 1. Start Producers
    start_earthquake_producer = BashOperator(
        task_id="start_earthquake_producer",
        bash_command="bash /opt/airflow/scripts/run_earthquake_producer.sh",
    )

    start_wolf_producer = BashOperator(
        task_id="start_wolf_producer",
        bash_command="bash /opt/airflow/scripts/run_wolf_seismic_producer.sh",
    )

    # 2. Wait for producers to start producing data
    wait_producers = TimeDeltaSensor(
        task_id="wait_for_producers",
        delta=timedelta(seconds=30),
    )

    # 3. Start Spark Consumers
    start_earthquake_consumer = BashOperator(
        task_id="start_earthquake_consumer",
        bash_command="bash /opt/airflow/scripts/submit_earthquake_consumer.sh",
    )

    start_wolf_consumer = BashOperator(
        task_id="start_wolf_consumer",
        bash_command="bash /opt/airflow/scripts/submit_wolf_seismic_consumer.sh",
    )

    # 4. Wait for consumers to ingest data into Delta
    wait_consumers = TimeDeltaSensor(
        task_id="wait_for_consumers",
        delta=timedelta(minutes=1),
    )

    # 5. Start Proximity Analyzer
    start_proximity_analyzer = BashOperator(
        task_id="start_proximity_analyzer",
        bash_command="bash /opt/airflow/scripts/run_proximity_analyzer.sh",
    )

    # 6. Wait for analyzer to produce events
    wait_analyzer = TimeDeltaSensor(
        task_id="wait_for_analyzer",
        delta=timedelta(minutes=1),
    )

    # 7. Run Validation
    run_validation = BashOperator(
        task_id="run_validation",
        bash_command="""
        docker exec -u root spark-master /opt/spark/bin/spark-submit 
            --packages io.delta:delta-spark_2.12:3.0.0 
            --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" 
            --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" 
            /opt/spark/scripts/validate_delta_tables.py
        """,
    )

    # Workflow
    [start_earthquake_producer, start_wolf_producer] >> wait_producers
    wait_producers >> [start_earthquake_consumer, start_wolf_consumer] >> wait_consumers
    wait_consumers >> start_proximity_analyzer >> wait_analyzer >> run_validation
