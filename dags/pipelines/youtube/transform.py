from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, dayofweek, hour, lit, round, when
from pyspark.sql.types import DoubleType
import os


def transform_youtube_data():
    """
    Transform YouTube channel data using PySpark.
    This function:
    1. Reads raw data from MinIO
    2. Applies transformations and feature engineering
    3. Writes transformed data back("[TRANSFORM] Would process raw JSON with PySpark")
    # TODO: implement PySpark job for cleaning + features

if __name__ == "__main__":
    main()
