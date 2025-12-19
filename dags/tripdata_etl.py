import sys
import os

# Ensure scripts directory is on PYTHONPATH
sys.path.append('/opt/airflow/scripts')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import ETL functions
try:
    from extract_tripdata import main as extract_func
    from clean_tripdata import main as clean_func
    from enrich_with_weather_csv import main as enrich_func
    from load_to_postgres import main as load_func
except ImportError as e:
    raise ImportError(
        f"Could not find scripts in /opt/airflow/scripts. Error: {e}"
    )

default_args = {
    "owner": "joy",
    "start_date": datetime(2025, 12, 17),
    "retries": 1
}

with DAG(
    dag_id="tripdata_etl",
    default_args=default_args,
    description="ETL for NYC Tripdata with Weather",
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_tripdata",
        python_callable=extract_func,
    )

    clean_task = PythonOperator(
        task_id="clean_tripdata",
        python_callable=clean_func,
    )

    enrich_task = PythonOperator(
        task_id="enrich_tripdata",
        python_callable=enrich_func,
    )

    load_task = PythonOperator(
        task_id="load_tripdata_to_postgres",
        python_callable=load_func,
    )

    # Correct ETL flow
    extract_task >> clean_task >> enrich_task >> load_task
