import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, round, current_date, datediff, when, lit, sum as _sum
from pyspark.sql import functions as F


def transform_youtube_data():
    # 🔹 MinIO / S3 Configuration
    minio_endpoint = "http://172.17.0.1:9000"  # internal Docker host address for MinIO
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    # 🔹 Initialize SparkSession with Hadoop S3A support
    spark = (
        SparkSession.builder
        .appName("YouTube Data Transformation")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    bucket_name = "rayes-youtube"
    channel_path = f"s3a://{bucket_name}/raw/channel_stats.json"
    videos_path = f"s3a://{bucket_name}/raw/video_stats.json"
    transformed_path = f"s3a://{bucket_name}/transformed/channel_stats_transformed.parquet"

    print(f"Reading channel data from {channel_path}")
    channel_df = spark.read.json(channel_path)
    print(f"Loaded channel data with {channel_df.count()} rows")

    print(f"Reading video data from {videos_path}")
    videos_df = spark.read.json(videos_path)
    print(f"Loaded video data with {videos_df.count()} rows")

    # 🔹 Aggregate video-level engagement metrics
    print("Aggregating video-level engagement metrics...")

    # Clean and cast numeric fields safely
    videos_df = (
        videos_df
        .withColumn("`statistics.viewCount`", col("`statistics.viewCount`").cast("int"))
        .withColumn("`statistics.likeCount`", col("`statistics.likeCount`").cast("int"))
        .withColumn("`statistics.commentCount`", col("`statistics.commentCount`").cast("int"))
    )

    # Aggregate metrics
    total_views = videos_df.agg(_sum("`statistics.viewCount`")).first()[0] or 0
    total_likes = videos_df.agg(_sum("`statistics.likeCount`")).first()[0] or 0
    total_comments = videos_df.agg(_sum("`statistics.commentCount`")).first()[0] or 0

    print(f"Aggregated totals: Views={total_views}, Likes={total_likes}, Comments={total_comments}")

    # 🔹 Transform channel-level data
    transformed_df = channel_df.select(
        col("id").alias("channel_id"),
        col("`snippet.title`").alias("channel_title"),
        to_date(col("`snippet.publishedAt`")).alias("published_at"),
        when(col("`snippet.country`").isNull(), "Unknown").otherwise(col("`snippet.country`")).alias("country"),
        col("`statistics.viewCount`").cast("int").alias("view_count"),
        col("`statistics.subscriberCount`").cast("int").alias("subscriber_count"),
        col("`statistics.videoCount`").cast("int").alias("video_count"),
        when(col("`statistics.hiddenSubscriberCount`").isNull(), lit(0))
            .otherwise(col("`statistics.hiddenSubscriberCount`").cast("int"))
            .alias("hidden_subscribers")
    )


    transformed_df = (
        transformed_df
            .withColumn("like_count", lit(total_likes))
            .withColumn("comment_count", lit(total_comments))
            .withColumn(
                "views_per_video",
                (lit(total_views) / when(col("video_count") == 0, 1).otherwise(col("video_count")))
            )
            .withColumn(
                "subs_per_video",
                (col("subscriber_count") / when(col("video_count") == 0, 1).otherwise(col("video_count")))
            )
            .withColumn(
                "like_ratio",
                when(lit(total_views) > 0, round(lit(total_likes) / lit(total_views), 4)).otherwise(lit(0))
            )
            .withColumn(
                "comment_ratio",
                when(lit(total_views) > 0, round(lit(total_comments) / lit(total_views), 4)).otherwise(lit(0))
            )
            .withColumn(
                "engagement_rate",
                when(lit(total_views) > 0, round((lit(total_likes) + lit(total_comments)) / lit(total_views), 4)).otherwise(lit(0))
            )
    )


    today = current_date()
    transformed_df = (
        transformed_df
        .withColumn("channel_age_days", datediff(today, col("published_at")))
        .withColumn(
            "daily_view_growth",
            col("view_count") / when(col("channel_age_days") == 0, lit(1)).otherwise(col("channel_age_days"))
        )
        .withColumn(
            "daily_sub_growth",
            col("subscriber_count") / when(col("channel_age_days") == 0, lit(1)).otherwise(col("channel_age_days"))
        )
    )

    print("Schema after transformation:")
    transformed_df.printSchema()

    print("Saving transformed dataset to MinIO...")
    transformed_df.write.mode("overwrite").partitionBy("country").parquet(transformed_path)
    print(f"✅ Saved transformed data to {transformed_path}")

    print("\nTransformation complete. Summary:")
    transformed_df.select("channel_title", "view_count", "like_count", "comment_count", "engagement_rate").show()

    spark.stop()
    return "YouTube data transformation completed successfully!"


if __name__ == "__main__":
    transform_youtube_data()
