# YouTube Analytics Pipeline (Airflow Project)

This repo is a **pluggable Airflow project** designed to run on my reusable engine in `~/airflow-docker/`.

## Run instructions

1. Copy `.env.example` to `.env` and set your values (API key, MinIO creds).
2. Run the sync script to copy environment variables to the Airflow engine:
   ```bash
   chmod +x sync_env.sh
   ./sync_env.sh
   ```
3. Point the Airflow engine to this project and start the containers:
   ```bash
   cd ~/airflow-docker
   docker compose down
   docker compose up -d
   ```

## Environment Variables

The project requires the following environment variables:
- `YOUTUBE_API_KEY`: Your YouTube Data API v3 key
- `MINIO_ACCESS_KEY`: Access key for MinIO/S3 storage
- `MINIO_SECRET_KEY`: Secret key for MinIO/S3 storage
- `MINIO_ENDPOINT`: MinIO endpoint URL (default: http://172.17.0.1:9000)

## Troubleshooting

- If you encounter connectivity issues with MinIO, make sure the endpoint URL is using the Docker host IP (172.17.0.1) instead of the service name (minio)
- You can manually test the extraction script with:
  ```bash
  docker exec -i airflow-docker-airflow-scheduler-1 python /opt/airflow/dags/pipelines/youtube/extract.py
  ```
