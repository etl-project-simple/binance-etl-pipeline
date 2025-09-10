from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.main import run_etl

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    "daily_binance_etl",
    default_args=default_args,
    schedule_interval="0 4 * * *",  # run once daily at 04:00
    start_date=datetime(2025, 8, 28),
    catchup=False
) as dag:

    etl_task = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_etl
    )
    etl_task