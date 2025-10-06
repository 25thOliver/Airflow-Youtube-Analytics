import pandas as pd
import numpy as np
import os
from datetime import datetime


def transform_youtube_data():
    # MinIO configuration
    storage_options = {
        "key": os.environ.get("MINIO_ACCESS_KEY"),
        "secret": os.environ.get("MINIO_SECRET_KEY"),
        "client_kwargs": {
            "endpoint_url": "http://172.17.0.1:9000",
        },
    }

    bucket_name = "rayes-youtube"
    channel_path = f"s3://{bucket_name}/raw/channel_stats.json"
    videos_path = f"s3://{bucket_name}/raw/video_stats.json"
    transformed_path = f"s3://{bucket_name}/transformed/channel_stats_transformed.parquet"

    print(f"Reading channel data from {channel_path}")
    channel_df = pd.read_json(channel_path, storage_options=storage_options)
    print(f"Loaded channel data with {len(channel_df)} rows")

    print(f"Reading video data from {videos_path}")
    videos_df = pd.read_json(videos_path, storage_options=storage_options)
    print(f"Loaded video data with {len(videos_df)} rows")

    # 🔹 Aggregate video-level engagement metrics
    print("Aggregating video-level engagement metrics...")

    # Clean and convert numeric fields safely
    videos_df["statistics.viewCount"] = videos_df["statistics.viewCount"].astype(int)
    videos_df["statistics.likeCount"] = videos_df["statistics.likeCount"].astype(int)
    videos_df["statistics.commentCount"] = videos_df["statistics.commentCount"].astype(int)

    total_views = videos_df["statistics.viewCount"].sum()
    total_likes = videos_df["statistics.likeCount"].sum()
    total_comments = videos_df["statistics.commentCount"].sum()

    avg_likes = videos_df["statistics.likeCount"].mean()
    avg_comments = videos_df["statistics.commentCount"].mean()

    print(f"Aggregated totals: Views={total_views}, Likes={total_likes}, Comments={total_comments}")

    # 🔹 Transform channel-level data
    transformed_df = pd.DataFrame()
    transformed_df["channel_id"] = channel_df["id"]
    transformed_df["channel_title"] = channel_df["snippet.title"]
    transformed_df["published_at"] = pd.to_datetime(channel_df["snippet.publishedAt"])
    transformed_df["country"] = channel_df.get("snippet.country", "Unknown")

    transformed_df["view_count"] = channel_df["statistics.viewCount"].astype(int)
    transformed_df["subscriber_count"] = channel_df["statistics.subscriberCount"].astype(int)
    transformed_df["video_count"] = channel_df["statistics.videoCount"].astype(int)
    transformed_df["hidden_subscribers"] = channel_df["statistics.hiddenSubscriberCount"].fillna(0)

    # 🔹 Add engagement metrics
    transformed_df["like_count"] = total_likes
    transformed_df["comment_count"] = total_comments
    transformed_df["views_per_video"] = total_views / transformed_df["video_count"].replace(0, 1)
    transformed_df["subs_per_video"] = transformed_df["subscriber_count"] / transformed_df["video_count"].replace(0, 1)

    transformed_df["like_ratio"] = round(total_likes / total_views, 4) if total_views else 0
    transformed_df["comment_ratio"] = round(total_comments / total_views, 4) if total_views else 0
    transformed_df["engagement_rate"] = round((total_likes + total_comments) / total_views, 4) if total_views else 0

    # Time-based metrics
    today = pd.to_datetime(datetime.now().date())
    transformed_df["channel_age_days"] = (
        today - transformed_df["published_at"].dt.date.astype("datetime64[ns]")
    ).dt.days

    transformed_df["daily_view_growth"] = (
        transformed_df["view_count"] / transformed_df["channel_age_days"].replace(0, 1)
    ).round(2)
    transformed_df["daily_sub_growth"] = (
        transformed_df["subscriber_count"] / transformed_df["channel_age_days"].replace(0, 1)
    ).round(2)

    # Save transformed data
    print("Saving transformed dataset to MinIO...")
    transformed_df.to_parquet(transformed_path, storage_options=storage_options)
    print(f"Saved transformed data to {transformed_path}")

    print("\nTransformation complete. Summary:")
    print(transformed_df[["channel_title", "view_count", "like_count", "comment_count", "engagement_rate"]])

    return "YouTube data transformation completed successfully!"


if __name__ == "__main__":
    transform_youtube_data()
