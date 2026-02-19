from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.time_delta import TimeDeltaSensor
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

def check_spark_app_health():
    """
    Check if Spark applications are active and healthy.
    Returns the task_id.
    """
    import subprocess
    import json
    
    # Use environment variables from Airflow environment
    spark_master = os.getenv('SPARK_MASTER_CONTAINER', 'spark-master')
    spark_webui_port = os.getenv('SPARK_MASTER_WEBUI_PORT', '8080')
    
    try:
        # Query Spark Master for running applications
        result = subprocess.run(
            ["curl", "-s", f"http://{spark_master}:{spark_webui_port}/json/"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0:
            print("Failed to query Spark Master")
            return "self_healing_monitor.trigger_spark_job_restart"
        
        data = json.loads(result.stdout)
        active_apps = data.get("activeapps", [])
        
        # Check for our specific apps
        app_names = ["WolfSeismicConsumer", "EarthquakeStreamProcessor"]
        found_apps = [app for app in active_apps if app.get("name") in app_names]
        
        # If both apps are active, return log_application_status else trigger_spark_job_restart
        if len(found_apps) >= 2:
            print(f"All Spark applications are active: {[app['name'] for app in found_apps]}")
            return "self_healing_monitor.log_application_status"
        else:
            print(f"Missing applications. Found: {[app['name'] for app in found_apps]}")
            return "self_healing_monitor.trigger_spark_job_restart"
            
    # Raise exception if any error occurs then trigger restart
    except Exception as e:
        print(f"Error checking Spark app health: {e}")
        return "self_healing_monitor.trigger_spark_job_restart"

with DAG(
    "seismic_pipeline_lifecycle_dag",
    default_args=default_args,
    description="DAG to manage seismic pipeline lifecycle",
    schedule_interval=None,
    catchup=False,
    tags=["seismic", "pipeline", "lifecycle"],
) as dag:
    
    # Pipeline Initialization
    trigger_pipeline_dag = BashOperator(
        task_id="trigger_pipeline_dag",
        bash_command="bash /opt/airflow/scripts/start_pipeline.sh ",
    )
    
    # Ingestion Producers Task Group
    with TaskGroup("ingestion_producers") as ingestion_producers:
        # Wolf Seismic Producer Task
        run_wolf_seismic_producer = BashOperator(
            task_id="run_wolf_seismic_producer",
            bash_command="bash /opt/airflow/scripts/run_wolf_seismic_producer.sh ",
        )
        # Earthquake Producer Task 
        run_earthquake_producer = BashOperator(
            task_id="run_earthquake_producer",
            bash_command="bash /opt/airflow/scripts/run_earthquake_producer.sh ",
        )
    
    # Verify Producer Status
    verify_producer_status = BashOperator(
        task_id="verify_producer_status",
        bash_command="bash /opt/airflow/scripts/verify_producer_status.sh ",
    )
    
    # Spark Processors Task Group
    with TaskGroup("spark_processors") as spark_processors:
        # Wolf Seismic Consumer Task
        submit_wolf_seismic_consumer = BashOperator(
            task_id="submit_wolf_seismic_consumer",
            bash_command="bash /opt/airflow/scripts/submit_wolf_seismic_consumer.sh ",
        )
        # Earthquake Consumer Task
        submit_earthquake_consumer = BashOperator(
            task_id="submit_earthquake_consumer",
            bash_command="bash /opt/airflow/scripts/submit_earthquake_consumer.sh ",
        )
    
    # Verify Job Submission
    verify_job_submission = BashOperator(
        task_id="verify_job_submission",
        bash_command="bash /opt/airflow/scripts/verify_job_submission.sh ",
    )
    
    # Self-Healing Monitor Task Group
    with TaskGroup("self_healing_monitor") as self_healing_monitor:
        # Wait 5 minutes before checking (pulse check interval) --> work as sensor operator
        pulse_check_spark_apps = TimeDeltaSensor(
            task_id="pulse_check_spark_apps",
            delta=timedelta(minutes=5),
        )
        
        # Branch based on Spark app health
        check_spark_app_active = BranchPythonOperator(
            task_id="check_spark_app_active",
            python_callable=check_spark_app_health,
        )
        
        # If apps are healthy, just log status
        log_application_status = BashOperator(
            task_id="log_application_status",
            bash_command="echo 'All Spark applications are healthy and running' ",
        )
        
        # If apps failed, trigger restart
        trigger_spark_job_restart = BashOperator(
            task_id="trigger_spark_job_restart",
            bash_command="bash /opt/airflow/scripts/restart_spark_jobs.sh ",
        )
        
        # Define flow within the task group
        pulse_check_spark_apps >> check_spark_app_active
        check_spark_app_active >> [log_application_status, trigger_spark_job_restart]
    
    # DAG Flow Definition
    trigger_pipeline_dag >> ingestion_producers >> verify_producer_status
    verify_producer_status >> spark_processors >> verify_job_submission
    verify_job_submission >> self_healing_monitor