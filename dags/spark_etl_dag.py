from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
import os

# Base directories (local paths in Codespaces)
BASE_PATH = "/workspaces/spark-airflow-etl"
INPUT_PATH = f"{BASE_PATH}/data/input/events.parquet"
OUTPUT_PATH = f"{BASE_PATH}/data/output/"

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_daily_etl",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",  # Daily at midnight
    catchup=False,
) as dag:

    # Sensor waits for input data to appear
    wait_for_data = FileSensor(
        task_id="wait_for_data",
        filepath="data/input/events.parquet",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 60,
        mode="poke",
    )

    # Run Spark job
    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command=f"""
        python {BASE_PATH}/spark_job/app.py \
            --input-path {INPUT_PATH} \
            --output-path {OUTPUT_PATH} \
            --date {{ ds }}
        """,
    )

    wait_for_data >> run_spark_etl
