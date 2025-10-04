import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta


def transform_youtube_data():
    """
    Transform YouTube channel data using Pandas.
    This function:
    1. Reads raw data from MinIO
    2. Applies transformations and feature engineering
    3. Writes processed data back to MinIO
    """
    print("Starting YouTube data transformation with Pandas")

    # Storage options for MinIO (using s3fs)
    storage_options = {
        "key": os.environ.get("MINIO_ACCESS_KEY"),
        "secret": os.environ.get("MINIO_SECRET_KEY"),
        "client_kwargs": {
            # Force the endpoint URL to use the Docker host IP instead of the service name
            "endpoint_url": "http://172.17.0.1:9000",
        },
    }

    # Define paths
    bucket_name = "rayes-youtube"
    raw_path = f"s3://{bucket_name}/raw/channel_stats.json"
    transformed_path = (
        f"s3://{bucket_name}/transformed/channel_stats_transformed.parquet"
    )

    # Read the raw JSON data
    try:
        print(f"Reading raw data from {raw_path}")
        raw_df = pd.read_json(raw_path, storage_options=storage_options)
        print(f"Successfully loaded raw data with {len(raw_df)} rows")
        print("DataFrame columns:")
        print(raw_df.columns)
    except Exception as e:
        print(f"Error reading raw data: {e}")
        raise

    # Apply transformations
    try:
        print("Applying transformations...")

        # Create a new DataFrame for the transformed data
        transformed_df = pd.DataFrame()

        # Extract and rename columns
        transformed_df["channel_id"] = raw_df["id"]
        transformed_df["channel_title"] = raw_df["snippet.title"]
        transformed_df["published_at"] = pd.to_datetime(raw_df["snippet.publishedAt"])
        transformed_df["view_count"] = raw_df["statistics.viewCount"].astype(int)
        transformed_df["subscriber_count"] = raw_df[
            "statistics.subscriberCount"
        ].astype(int)
        transformed_df["video_count"] = raw_df["statistics.videoCount"].astype(int)
        transformed_df["hidden_subscribers"] = raw_df[
            "statistics.hiddenSubscriberCount"
        ]

        # Convert published_at to date
        transformed_df["published_date"] = transformed_df["published_at"].dt.date

        # Add day of week (0=Monday, 6=Sunday in pandas)
        transformed_df["publish_day"] = transformed_df["published_at"].dt.dayofweek

        # Calculate engagement metrics
        transformed_df["subscribers_per_video"] = transformed_df.apply(
            lambda row: round(row["subscriber_count"] / max(row["video_count"], 1), 2),
            axis=1,
        )

        transformed_df["views_per_video"] = transformed_df.apply(
            lambda row: round(row["view_count"] / max(row["video_count"], 1), 2), axis=1
        )

        # Add channel age in days
        today = pd.to_datetime(datetime.now().date())
        transformed_df["channel_age_days"] = transformed_df["published_date"].apply(
            lambda date: (today - pd.to_datetime(date)).days
        )

        # Calculate growth metrics
        transformed_df["daily_subscriber_growth"] = transformed_df.apply(
            lambda row: round(
                row["subscriber_count"] / max(row["channel_age_days"], 1), 2
            ),
            axis=1,
        )

        transformed_df["daily_view_growth"] = transformed_df.apply(
            lambda row: round(row["view_count"] / max(row["channel_age_days"], 1), 2),
            axis=1,
        )

        # Show sample of transformed data
        print("Transformed data sample:")
        print(transformed_df.head())

        # Write transformed data
        print(f"Writing transformed data to {transformed_path}")
        transformed_df.to_parquet(transformed_path, storage_options=storage_options)
        print("✅ Successfully saved transformed data")

    except Exception as e:
        print(f"Error during transformation: {e}")
        raise

    return "YouTube data transformation completed successfully"


if __name__ == "__main__":
    transform_youtube_data()
