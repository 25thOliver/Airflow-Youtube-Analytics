import os

def main():
    api_key = os.getenv("YOUTUBE_API_KEY")
    channel_id = os.getenv("YOUTUBE_CHANNEL_ID")
    print(f"[EXTRACT] Would fetch videos for channel {channel_id} using API key {api_key}")
    # TODO: implement API calls + save raw JSON to MinIO

if __name__ == "__main__":
    main()
