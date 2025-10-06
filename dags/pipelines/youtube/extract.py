import os
import json
import requests
import pandas as pd

# API key
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")

# Storage options for MinIO
storage_options = {
    "key": os.environ.get("MINIO_ACCESS_KEY"),
    "secret": os.environ.get("MINIO_SECRET_KEY"),
    "client_kwargs": {
        "endpoint_url": "http://172.17.0.1:9000",
    },
}

BUCKET_NAME = "rayes-youtube"
CHANNEL_ID = "UCw5z_dopYnvEL6Rc8KNKsnw"  # Raye's channel

def fetch_channel_data():
    """Fetch core channel statistics."""
    print(f"Fetching channel statistics for {CHANNEL_ID}...")
    url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&id={CHANNEL_ID}&key={YOUTUBE_API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    if "items" not in data or len(data["items"]) == 0:
        raise ValueError("No data returned from YouTube API for channel")

    channel_df = pd.json_normalize(data["items"][0])
    print(f"Retrieved channel data with {len(channel_df.columns)} columns")

    s3_uri = f"s3://{BUCKET_NAME}/raw/channel_stats.json"
    channel_df.to_json(s3_uri, orient="records", lines=False, storage_options=storage_options)
    print(f"Saved channel data to {s3_uri}")

    return channel_df


def fetch_video_ids(channel_id, max_results=50):
    """Fetch all video IDs from the channel."""
    print("Fetching video IDs from channel uploads...")
    video_ids = []
    page_token = None

    while True:
        url = (
            f"https://www.googleapis.com/youtube/v3/search?"
            f"part=id&channelId={channel_id}&maxResults={max_results}"
            f"&order=date&type=video&key={YOUTUBE_API_KEY}"
        )
        if page_token:
            url += f"&pageToken={page_token}"

        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()

        ids = [item["id"]["videoId"] for item in data.get("items", []) if "id" in item]
        video_ids.extend(ids)

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    print(f"Retrieved {len(video_ids)} video IDs")
    return video_ids


def fetch_video_stats(video_ids):
    """Fetch statistics for each video."""
    print("Fetching video statistics...")
    all_videos = []
    for i in range(0, len(video_ids), 50):  # 50 IDs per API call limit
        ids_chunk = ",".join(video_ids[i:i+50])
        url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={ids_chunk}&key={YOUTUBE_API_KEY}"
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        all_videos.extend(data.get("items", []))

    if not all_videos:
        raise ValueError("No video data returned from API")

    videos_df = pd.json_normalize(all_videos)
    print(f"Retrieved stats for {len(videos_df)} videos")

    s3_uri = f"s3://{BUCKET_NAME}/raw/video_stats.json"
    videos_df.to_json(s3_uri, orient="records", lines=False, storage_options=storage_options)
    print(f"Saved video data to {s3_uri}")

    return videos_df


def extract_youtube_data():
    """Run full YouTube data extraction: channel + videos."""
    try:
        channel_df = fetch_channel_data()
        video_ids = fetch_video_ids(CHANNEL_ID)
        videos_df = fetch_video_stats(video_ids)

        print("\nExtraction Summary:")
        print(f"- Channel: {channel_df['snippet.title'][0]}")
        print(f"- Total videos fetched: {len(videos_df)}")

    except Exception as e:
        print(f"Extraction failed: {e}")
        raise


if __name__ == "__main__":
    extract_youtube_data()
