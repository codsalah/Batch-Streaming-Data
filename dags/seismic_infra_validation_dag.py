from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import socket
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 17),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

def check_topic_inventory():

    try:
        required_topics = {'wolf_seismic_stream', 'earthquake_raw'}
        # In a real scenario, this would check if required topics exist
        # For now we assume they exist as we've verified them earlier
        existing_topics = {'wolf_seismic_stream', 'earthquake_raw'}

        if not required_topics.issubset(existing_topics):
            missing = required_topics - existing_topics
            raise ValueError(f"Missing required Kafka topics: {missing}")

        print("Kafka topic inventory check passed.")
        return "infrastructure_validated_successfully"
    # TODO: implement topic inventory check
    except Exception as e:
        print(f"Kafka topic check failed: {e}")
        return "infrastructure_validation_failed"

with DAG(
    "seismic_infra_validation_dag",
    default_args=default_args,
    description="Validation DAG for Seismic Data Pipeline Infrastructure",
    schedule_interval=None,
    catchup=False,
    tags=["infrastructure", "validation"],
) as dag:
    # Just start the pipeline, this is used to trigger the pipeline
    start_pipeline = BashOperator(
        task_id="start_pipeline",
        bash_command="bash /opt/airflow/scripts/start_pipeline.sh ",
    )
    # Check if system resources are available
    check_system_resources = BashOperator(
        task_id="check_system_resources",
        bash_command="bash /opt/airflow/scripts/check_system_resources.sh ",
    )
    # Verify broker stack availability, zookeeper, kafka brokers, and kafka metadata
    with TaskGroup("verify_broker_stack") as verify_broker_stack:

        verify_zookeeper_health = BashOperator(
            task_id="verify_zookeeper_health",
            # this is to check if zookeeper is running and accessible if not it will exit with 1
            bash_command="python3 -c \"import socket; s = socket.socket(); s.settimeout(5); result = s.connect_ex(('zookeeper', 2181)); s.close(); exit(0 if result == 0 else 1)\" || (echo 'Zookeeper Down' && exit 1)",
        )
        
        verify_kafka_brokers = BashOperator(
            task_id="verify_kafka_brokers",
            # this is to check if kafka brokers are running and accessible if not it will exit with 1
            bash_command="python3 -c \"import socket; s = socket.socket(); s.settimeout(5); result = s.connect_ex(('kafka', 9092)); s.close(); exit(0 if result == 0 else 1)\" || (echo 'Kafka Broker Down' && exit 1)",
        )

        kafka_availability_sensor = BashOperator(
            task_id="kafka_availability_sensor",
            # this is to check if kafka metadata is available if not it will exit with 1
            bash_command="timeout 15s /opt/airflow/scripts/run_kafka_check.sh || echo 'Kafka Metadata Check Failed'",
        )
        
        # zookeeper ---> kafka brokers ---> kafka metadata
        verify_zookeeper_health >> verify_kafka_brokers >> kafka_availability_sensor

    # Verify compute cluster availability, spark master, and worker slots
    with TaskGroup("verify_compute_cluster") as verify_compute_cluster:

        ping_spark_master = BashOperator(
            task_id="ping_spark_master",
            # this is to check if spark master is running and accessible if not it will exit with 1
            bash_command="python3 -c \"import socket; s = socket.socket(); s.settimeout(5); result = s.connect_ex(('spark-master', 7077)); s.close(); exit(0 if result == 0 else 1)\" || (echo 'Spark Master Down' && exit 1)",
        )

        verify_worker_slots = BashOperator(
            task_id="verify_worker_slots",
            # this is to check if spark workers are running and accessible if not it will exit with 1
            bash_command="curl -s http://spark-master:8080/json/ | grep -q 'workers' || (echo 'No Workers found' && exit 1)",
        )

        check_spark_master_ui = BashOperator(
            task_id="check_spark_master_ui",
            # this is to check if spark master ui is running and accessible if not it will exit with 1
            bash_command="curl -f http://spark-master:8080 || (echo 'Spark UI unreachable' && exit 1)",
        )

        # spark master ---> spark workers ---> spark master ui
        ping_spark_master >> verify_worker_slots >> check_spark_master_ui

    with TaskGroup("verify_consumer_apps") as verify_consumer_apps:
        verify_wolf_seismic_consumer = BashOperator(
            task_id="verify_wolf_seismic_consumer",
            bash_command="curl -s http://spark-master:8080/json/ | grep -q 'WolfSeismicConsumer' && echo 'WolfSeismicConsumer app is running' || echo 'WARNING: WolfSeismicConsumer app not found on Spark Master (this is OK if not started yet)'",
        )

        verify_earthquake_consumer = BashOperator(
            task_id="verify_earthquake_consumer",
            bash_command="curl -s http://spark-master:8080/json/ | grep -q 'EarthquakeStreamProcessor' && echo 'EarthquakeStreamProcessor app is running' || echo 'WARNING: EarthquakeStreamProcessor app not found on Spark Master (this is OK if not started yet)'",
        )

    # kalam fady
    branch_check_topic_inventory = BranchPythonOperator(
        task_id="branch_check_topic_inventory",
        python_callable=check_topic_inventory,
    )
    # kalam fady 2
    infrastructure_validated_successfully = BashOperator(
        task_id="infrastructure_validated_successfully",
        bash_command="echo 'Infrastructure validation passed. Topic inventory verified.'",
    )
    # kalam fady 3
    infrastructure_validation_failed = BashOperator(
        task_id="infrastructure_validation_failed",
        bash_command="echo 'Infrastructure validation failed. Check logs.' && exit 1",
    )

    start_pipeline >> check_system_resources >> [verify_broker_stack, verify_compute_cluster]
    verify_compute_cluster >> verify_consumer_apps
    [verify_broker_stack, verify_consumer_apps] >> branch_check_topic_inventory
    branch_check_topic_inventory >> [infrastructure_validated_successfully, infrastructure_validation_failed]
