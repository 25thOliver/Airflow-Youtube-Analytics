import os
import json
import requests
import pandas as pd


# API key
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")

# Storage options for MinIO (using s3fs)
storage_options = {
    "key": os.environ.get("MINIO_ACCESS_KEY"),
    "secret": os.environ.get("MINIO_SECRET_KEY"),
    "client_kwargs": {
        # Force the endpoint URL to use the Docker host IP instead of the service name
        "endpoint_url": "http://172.17.0.1:9000",
    },
}


def extract_youtube_data():
    # Get channel stats for a given channel (Raye)
    channel_id = "UCw5z_dopYnvEL6Rc8KNKsnw"
    url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&id={channel_id}&key={YOUTUBE_API_KEY}"

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Flatten relevant fields into DataFrame
    if "items" in data and len(data["items"]) > 0:
        item = data["items"][0]
        df = pd.json_normalize(item)
    else:
        raise ValueError("No data returned from YouTube API")

    # S3 URI for MinIO (JSON format)
    bucket_name = "rayes-youtube"
    s3_uri = f"s3://{bucket_name}/raw/channel_stats.json"

    print(f"Connecting to MinIO at {storage_options['client_kwargs']['endpoint_url']}")
    print(f"Using access key: {storage_options['key']}")
    print(
        f"Environment MINIO_ENDPOINT was: {os.environ.get('MINIO_ENDPOINT', 'not set')}"
    )

    # Save the DataFrame directly into MinIO
    df.to_json(s3_uri, orient="records", lines=False, storage_options=storage_options)

    print(f"✅ Uploaded Raye's YouTube channel stats to {s3_uri}")


if __name__ == "__main__":
    extract_youtube_data()
