from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


def process_youtube_data():
    # MinIO Configuration
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    storage_options = {
        "key": os.environ.get("MINIO_ACCESS_KEY"),
        "secret": os.environ.get("MINIO_SECRET_KEY"),
        "client_kwargs": {
            "endpoint_url": minio_endpoint,
        },
    }

    bucket_name = "rayes-youtube"
    input_path = f"s3://{bucket_name}/transformed/channel_stats_transformed.parquet"
    output_path = f"s3://{bucket_name}/processed/channel_stats.parquet"

    print(f"Reading transformed data from {input_path}...")

    # Load Transformed Data
    try:
        df = pd.read_parquet(input_path, storage_options=storage_options)
        print(f"Loaded transformed data with {len(df)} rows and {len(df.columns)} columns")
        print("Columns:", df.columns.tolist())
    except Exception as e:
        print(f"Error reading transformed data: {e}")
        raise

    # Data Cleaning & Schema Alignment
    print("Cleaning and aligning schema...")

    try:
        processed_df = pd.DataFrame()
        processed_df["channel_id"] = df["channel_id"]
        processed_df["channel_name"] = df["channel_title"]
        processed_df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

        # Handle categorical country values safely
        if "country" in df.columns:
            if pd.api.types.is_categorical_dtype(df["country"]):
                df["country"] = df["country"].astype(str)
        processed_df["country"] = df.get("country", "Unknown").fillna("Unknown")

        # Core channel metrics
        processed_df["view_count"] = df["view_count"].astype(int)
        processed_df["subscriber_count"] = df["subscriber_count"].astype(int)
        processed_df["video_count"] = df["video_count"].astype(int)

        # Engagement metrics
        processed_df["like_count"] = df["like_count"].fillna(0).astype(int)
        processed_df["comment_count"] = df["comment_count"].fillna(0).astype(int)
        processed_df["like_ratio"] = df["like_ratio"].astype(float)
        processed_df["comment_ratio"] = df["comment_ratio"].astype(float)
        processed_df["engagement_rate"] = df["engagement_rate"].astype(float)

        # Derived metrics
        processed_df["views_per_video"] = df["views_per_video"].astype(float)
        processed_df["subs_per_video"] = df["subs_per_video"].astype(float)
        processed_df["channel_age_days"] = df["channel_age_days"].astype(int)
        processed_df["daily_view_growth"] = df["daily_view_growth"].astype(float)
        processed_df["daily_sub_growth"] = df["daily_sub_growth"].astype(float)

        print("Schema aligned and cleaned successfully")
        print(processed_df.head())

    except Exception as e:
        print(f"Error during transformation: {e}")
        raise

    # Save Processed Data Back to MinIO
    print(f"Saving processed data to {output_path}")
    processed_df.to_parquet(output_path, storage_options=storage_options)
    print(f"Saved processed data to {output_path}")

    # Load Data into PostgreSQL
    postgre_conn_string = os.environ.get("POSTGRES_CONN_STRING", "postgresql://postgres:postgres@postgres-analytics:5432/youtube_analytics")
    if not postgre_conn_string:
        raise ValueError("POSTGRES_CONN_STRING not found in environment variables")

    try:
        print("Connecting to PostgreSQL...")
        engine = create_engine(postgre_conn_string)
        table_name = "raye_youtube_channel_stats"

        processed_df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"Successfully loaded {len(processed_df)} record(s) into PostgreSQL table '{table_name}'")

    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")
        raise


if __name__ == "__main__":
    process_youtube_data()
