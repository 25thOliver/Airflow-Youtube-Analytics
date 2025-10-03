from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Dummy functions for now
def extract_youtube_data():
    print("Extracting data from YouTube API...")

def transform_youtube_data():
    print("Transforming raw data with PySpark...")

def load_to_minio():
    print("Loading processed data to MinIO...")
# Minimal DAG just to check UI visibility
with DAG(
    dag_id="youtube_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["youtube", "test"],
) as dag:

    extract = PythonOperator(
        task_id="extract_youtube_data",
        python_callable=extract_youtube_data
    )

    transform = PythonOperator(
        task_id="transform_youtube_data",
        python_callable=transform_youtube_data
    )

    load = PythonOperator(
        task_id="load_to_minio",
        python_callable=load_to_minio
    )

    extract >> transform >> load
