from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
)
import pandas as pd
import numpy as np
import os
import json
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


def process_youtube_data():
    # MinIO Configuration
    storage_options = {
        "key": os.environ.get("MINIO_ACCESS_KEY"),
        "secret": os.environ.get("MINIO_SECRET_KEY"),
        "client_kwargs": {
            "endpoint_url": "http://172.17.0.1:9000",
        },
    }

    # Define path variables
    bucket_name = "rayes-youtube"
    input_path = f"s3://{bucket_name}/transformed/channel_stats_transformed.parquet"
    output_path = f"s3://{bucket_name}/processed/channel_stats.parquet"

    print(f"Reading transformed data from {input_path}...")

    # Load data
    try:
        df = pd.read_parquet(input_path, storage_options=storage_options)
        print(f"Successfully loaded data with {len(df)} rows")
        print("DataFrame columns:", df.columns.tolist())
    except Exception as e:
        print(f"Error reading data: {e}")
        raise

   # Transform data
    print("Cleaning and aligning data schema...")
    

    try:
        transformed_df = pd.DataFrame()
        
        # Core channel info
        transformed_df["channel_id"] = df["channel_id"]
        transformed_df["channel_name"] = df["channel_title"]
        transformed_df["channel_description"] = df.get("channel_description", "")
        transformed_df["publish_date"] = pd.to_datetime(df["published_at"])
        
        # Optional fields
        transformed_df["country"] = df["country"].fillna("Unknown") if "country" in df.columns else "Unknown"
        transformed_df["etag"] = df.get("etag", "")

        # Stats and engagement metrics
        transformed_df["view_count"] = df["view_count"].astype(int)
        transformed_df["subscriber_count"] = df["subscriber_count"].astype(int)
        transformed_df["video_count"] = df["video_count"].astype(int)
        
        # Engagements additions from transform.py
        transformed_df["like_count"] = df.get("like_count", pd.Series([0]*len(df))).astype(int)
        transformed_df["comment_count"] = df.get("comment_count", pd.Series([0]*len(df))).astype(int)

        transformed_df["engagement_rate"] = (
            df["engagement_rate"].astype(float) if "engagement_rate" in df.columns else 0.0
        )

        # Fill nulls with appropriate values
        transformed_df = transformed_df.fillna(
            {
                "view_count": 0,
                "subscriber_count": 0,
                "video_count": 0,
                "like_count": 0,
                "comment_count": 0,
                "engagement_rate": 0.0,
                "country": "Unknown"
            }
        )

        # Output sample data
        print("Transformed data sample:\n", transformed_df.head())

        # Save processed data back to MinIO
        print(f"Saving processed data to {output_path}")
        transformed_df.to_parquet(output_path, storage_options=storage_options)

        # Load same data into PostgreSQL
        postgre_conn_string = os.environ.get("POSTGRES_CONN_STRING")
        if not postgre_conn_string:
            raise ValueError("POSTGRES_CONN_STRING not found in .env file")

        print("Connecting to PostgreSQL...")
        engine = create_engine(postgre_conn_string)

        # Load into PostgreSQL table
        table_name = "Raye_youtube_channel_stats"
        transformed_df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"Successfully loaded {len(transformed_df)} records into PostgreSQL table '{table_name}'")

    except Exception as e:
        print(f"Error during transformation: {e}")
        raise
if __name__ == "__main__":
    process_youtube_data()