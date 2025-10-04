import sys
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Add the parent directory to sys.path to make pipelines importable
dag_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, dag_dir)

from pipelines.youtube.extract import extract_youtube_data
from pipelines.youtube.transform import transform_youtube_data
from pipelines.youtube.load import process_youtube_data


with DAG(
    dag_id="youtube_extract_test",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["youtube", "test"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_youtube_data", python_callable=extract_youtube_data
    )

    transform_task = PythonOperator(
        task_id="transform_youtube_data", python_callable=transform_youtube_data
    )

    load_task = PythonOperator(
        task_id="load_youtube_data", python_callable=process_youtube_data
    )

    # Set task dependencies - create a sequential ETL pipeline
    extract_task >> transform_task >> load_task
