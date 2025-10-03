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
import os
import json


def process_youtube_data():
    """
    Process YouTube channel data from MinIO using PySpark
    """
    print("Starting PySpark processing job...")

    # Initialize Spark Session with S3/MinIO configuration
    spark = (
        SparkSession.builder.appName("YouTube Data Processing")
        .config("spark.hadoop.fs.s3a.endpoint", "http://172.17.0.1:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    print(f"Created Spark session: {spark.version}")

    # Define path variables
    bucket_name = "rayes-youtube"
    input_path = f"s3a://{bucket_name}/raw/channel_stats.json"
    output_path = f"s3a://{bucket_name}/processed/channel_stats.parquet"

    print(f"Reading data from {input_path}")

    # Load JSON data
    try:
        df = spark.read.json(input_path)
        print(f"Successfully loaded data with {df.count()} rows")
        print("Schema:")
        df.printSchema()
    except Exception as e:
        print(f"Error reading JSON data: {e}")
        spark.stop()
        raise

    # Transform data
    print("Transforming data...")
    try:
        # Extract key metrics and convert types appropriately
        transformed_df = df.select(
            col("id").alias("channel_id"),
            col("snippet.title").alias("channel_name"),
            col("snippet.description").alias("channel_description"),
            col("snippet.publishedAt").cast("timestamp").alias("publish_date"),
            col("snippet.country").alias("country"),
            col("statistics.viewCount").cast("long").alias("view_count"),
            col("statistics.subscriberCount").cast("long").alias("subscriber_count"),
            col("statistics.videoCount").cast("long").alias("video_count"),
            col("etag"),
        )

        # Fill nulls with appropriate values
        transformed_df = transformed_df.na.fill(
            {
                "view_count": 0,
                "subscriber_count": 0,
                "video_count": 0,
                "country": "Unknown",
            }
        )

        # Show sample data
        print("Transformed data sample:")
        transformed_df.show(5, truncate=False)

        # Save processed data
        print(f"Saving processed data to {output_path}")
        transformed_df.write.mode("overwrite").parquet(output_path)
        print("✅ Successfully saved processed data")

    except Exception as e:
        print(f"Error during transformation: {e}")
        spark.stop()
        raise
    finally:
        # Stop Spark session
        print("Stopping Spark session")
        spark.stop()

    return "YouTube data processing completed successfully"


if __name__ == "__main__":
    process_youtube_data()
