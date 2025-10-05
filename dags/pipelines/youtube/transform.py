import pandas as pd
import numpy as np 
import os
from datetime import datetime, timedelta


def transform_youtube_data():
    # MinIO Storage Configuration
    storage_options = {
        "key": os.environ.get("MINIO_ACCESS_KEY"),
        "secret": os.environ.get("MINIO_SECRET_KEY"),
        "client_kwargs": {
            "endpoint_url": "http://172.17.0.1:9000",
        },
    }

    # Paths definition
    bucket_name = "rayes-youtube"
    raw_path = f"s3://{bucket_name}/raw/channel_stats.json"
    transformed_path = f"s3://{bucket_name}/transformed/channel_stats_transformed.parquet"

    print(f"Reading raw data from {raw_path}")

    # Read the raw JSON data
    try:
        raw_df = pd.read_json(raw_path, storage_options=storage_options)
        print(f"Successfully loaded raw data with {len(raw_df)} rows")
        print("DataFrame columns:\n", raw_df.columns)
    except Exception as e:
        print(f"Error reading raw data: {e}")
        raise

    # Apply transformations
    try:
        print("Applying transformations...")

        # Create a new DataFrame for the transformed data
        transformed_df = pd.DataFrame()

        # Core identifiers
        transformed_df["channel_id"] = raw_df["id"]
        transformed_df["channel_title"] = raw_df["snippet.title"]
        transformed_df["published_at"] = pd.to_datetime(raw_df["snippet.publishedAt"])
        transformed_df["country"] = raw_df.get("snippet.country", "Unknown")

        # Channel statistics
        transformed_df["view_count"] = raw_df["statistics.viewCount"].astype(int)
        transformed_df["subscriber_count"] = raw_df["statistics.subscriberCount"].astype(int)
        transformed_df["video_count"] = raw_df["statistics.videoCount"].astype(int)
        transformed_df["hidden_subscribers"] = raw_df["statistics.hiddenSubscriberCount"].fillna(0)

        # Engagement metrics
        if "statistics.likeCount" in raw_df.columns:
            raw_df.rename(columns={"statistics.likeCount": "like_count"}, inplace=True)
        else:
            raw_df["like_count"] = 0

        if "statistics.commentCount" in raw_df.columns:
            raw_df.rename(columns={"statistics.commentCount": "comment_count"}, inplace=True)
        else:
            raw_df["comment_count"] = 0

        output_path = f"s3://{bucket_name}/transformed/channel_stats_transformed.parquet"
        transformed_df.to_parquet(output_path, storage_options=storage_options)


        # Derive insights
        transformed_df["views_per_video"] = transformed_df["view_count"] / transformed_df["video_count"].replace(0, 1)
        transformed_df["subs_per_video"] = transformed_df["subscriber_count"] / transformed_df["video_count"].replace(0, 1)

        transformed_df["like_ratio"] = (transformed_df["like_count"] / transformed_df["view_count"].replace(0, 1)).round(4)
        transformed_df["comment_ratio"] = (transformed_df["comment_count"] / transformed_df["view_count"].replace(0, 1)).round(4)
        transformed_df["engagement_rate"] = ((transformed_df["like_count"] + transformed_df["comment_count"]) / transformed_df["view_count"].replace(0, 1)).round(4)

        # Time-based metrics 
        today = pd.to_datetime(datetime.now().date())
        transformed_df["channel_age_days"] = (today - transformed_df["published_at"].dt.date.astype("datetime64[ns]")).dt.days

        transformed_df["daily_view_growth"] = (transformed_df["view_count"] / transformed_df["channel_age_days"].replace(0, 1)).round(2)
        transformed_df["daily_sub_growth"] = (transformed_df["subscriber_count"] / transformed_df["channel_age_days"].replace(0, 1)).round(2)

        # Clean NaN
        transformed_df = transformed_df.fillna({
            "country": "Unknown",
            "like_count": 0,
            "comment_count": 0,
        })

        # Show sample of transformed data
        print("Transformed data sample:\n", transformed_df.head())
    
        # Write transformed data
        print(f"Writing transformed data to {transformed_path}")
        transformed_df.to_parquet(transformed_path, storage_options=storage_options)
        print("Successfully saved transformed data")

    except Exception as e:
        print(f"Error during transformation: {e}")
        raise

    return "YouTube data transformation completed successfully!"

if __name__ == "__main__":
    transform_youtube_data()