from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 17),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# DAG definition
with DAG(
    "airport_batch_dag",
    default_args=default_args,
    description="Batch processing DAG for airport data to Delta",
    schedule_interval=None,   
    catchup=False,
    tags=["batch", "airport", "delta"],
) as dag:

    # 1. Trigger Airport Batch Job
    run_airport_batch = BashOperator(
        task_id="run_airport_batch",
        bash_command=(
            "bash "
            + os.path.join(
                os.path.dirname(__file__), "../scripts/run_airport_batch.sh"
            )
        ),
    )

 
    verify_delta_table = BashOperator(
        task_id="verify_delta_table",
        bash_command=(
            "if [ -d ../data/delta_airports ]; then "
            "echo 'Delta table exists. Batch successful.'; "
            "else echo 'Delta table missing. Batch failed.'; exit 1; fi"
        ),
    )

    # DAG flow
    run_airport_batch >> verify_delta_table
