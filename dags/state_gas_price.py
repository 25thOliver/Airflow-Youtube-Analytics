from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
import os
import json
import io

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
}

# Environment variables config
API_URL= "https://api.collectapi.com/gasPrice/stateUsaPrice?state=WA"
API_KEY = os.environ.get("COLLECTAPI_TOKEN")

DB_URL = os.environ.get("POSTGRES_CONN_STRING")

# Define DAG
with DAG(
    dag_id='state_gas_price',
    default_args=default_args,
    description='ETL DAG using XComs, pandas, and SQLAlchemy to fetch gas prices',
    schedule='@hourly',
    start_date=datetime(2025, 10, 7),
    catchup=False,
    tags=['collectapi', 'etl', 'gas_price']
) as dag:

    # Fetch data from API
    @task()
    def fetch_data():
        headers = {
            "content-type": "application/json",
            "authorization": f"apikey {API_KEY}",
        }
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
        print("Fetched data keys:", list(data.keys()))
        print(json.dumps(data, indent=2))

        return data

    # Transform data
    @task()
    def transform_data(raw_data: dict):
        # Extract the result list
        records = raw_data.get("result", [])
        if not records:
            print("Raw API data sample:", json.dumps(raw_data, indent=2))
            raise ValueError("No records found in API response")

        
         # Normalize records safely even if inconsistent
        try:
            df = pd.json_normalize(records)
        except Exception as e:
            print("Error normalizing JSON:", e)
            print("Raw records sample:", json.dumps(records[:2], indent=2))
            raise

        # Add timestamp
        df['fetched_at'] = datetime.now()

        print("Transformed DataFrame shape:", df.shape)
        print("Columns:", list(df.columns))
        print(df.head())

        # Return for next task
        return df.to_json(orient="records")

    # Load to PostgreSQL
    @task()
    def load_to_postgres(transformed_json: str):
        # Read JSON safely
        df = pd.read_json(io.StringIO(transformed_json))

        # Convert non-scalar (dict/list) columns to JSON strings
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df[col] = df[col].apply(json.dumps)

        engine = create_engine(DB_URL)

        # Write to PostgreSQL
        df.to_sql('state_gas_prices', engine, if_exists='append', index=False)

        print(f"Inserted {len(df)} records into 'state_gas_prices' table")
        print("Columns:", list(df.columns))

    # Task pipeline
    raw = fetch_data()
    transformed = transform_data(raw)
    load_to_postgres(transformed)