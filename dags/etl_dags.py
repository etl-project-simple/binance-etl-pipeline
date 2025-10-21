from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
# Import ETL functions
from etl.extract import run_extract
from etl.transform import run_transform 
from etl.load import run_load

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5),}

with DAG(
    "daily_binance_etl",
    default_args=default_args,
    schedule_interval="0 18 * * *",  # run once daily at 18:00
    start_date=datetime(2025, 8, 28),
    catchup=False,
    tags=["binance", "etl", "pyspark", "redshift"]
) as dag:
    # 1. Extract Task
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=run_extract,
    )
    
    # 2. Transform Task
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=run_transform,
    )
    
    # 3. Load Task
    load_task = PythonOperator(
        task_id="load_data",
        python_callable=run_load,
    )
    
    extract_task >> transform_task >> load_task