# YouTube Analytics Pipeline (Airflow Project)

This repo is a **pluggable Airflow project** designed to run on my reusable engine in `~/airflow-docker/`.

## Run instructions

1. Copy `.env.example` to `.env` and set your values (API key, MinIO creds).
2. Point the Airflow engine to this project:
   ```bash
   export AIRFLOW_PROJ_DIR=~/airflow-youtube-analytics
   cd ~/airflow-docker
   docker compose up -d
