from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import subprocess

# ---------------------------
# Default task arguments
# ---------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ---------------------------
# Optional: Check Spark container
# ---------------------------
def check_spark_container():
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=spark-master", "--quiet"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if not result.stdout.strip():
            raise AirflowException("spark-master container is NOT running")

        print(f"spark-master container running (ID: {result.stdout.strip()})")
        return True

    except subprocess.TimeoutExpired:
        raise AirflowException("Timeout while checking Docker containers")
    except Exception as e:
        raise AirflowException(f"Error checking spark-master: {str(e)}")

# ---------------------------
# Define DAG
# ---------------------------
with DAG(
    dag_id="airport_batch_dag",
    default_args=default_args,
    description="Batch processing DAG for airport data to Delta Lake",
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=["batch", "airport", "delta"],
) as dag:

    # Task 1: Check Spark container
    check_spark = PythonOperator(
        task_id="check_spark_master",
        python_callable=check_spark_container,
    )

    # Task 2: Run batch script (FIXED)
    run_airport_batch = BashOperator(
        task_id="run_airport_batch",
        bash_command="bash -c '/opt/airflow/scripts/run_airport_batch.sh'",
    )

    # Task 3: Verify Delta table
    verify_delta_table = BashOperator(
        task_id="verify_delta_table",
        bash_command="""
        if [ -d /opt/airflow/data/delta_airports ]; then
            echo "Delta table exists. Batch successful."
            exit 0
        else
            echo "Delta table missing. Batch failed."
            exit 1
        fi
        """,
    )

    # Task dependencies
    check_spark >> run_airport_batch >> verify_delta_table
