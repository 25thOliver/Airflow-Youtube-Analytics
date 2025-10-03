#!/bin/bash

# sync_env.sh - Syncs environment variables from project to Airflow engine
# Usage: ./sync_env.sh

# Get absolute path of this script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
ENGINE_DIR="$HOME/airflow-docker"

# Check if .env exists in project directory
if [ ! -f "$PROJECT_DIR/.env" ]; then
    echo "❌ Error: .env file not found in $PROJECT_DIR"
    echo "Please create a .env file based on .env.example first."
    exit 1
fi

# Check if Airflow engine directory exists
if [ ! -d "$ENGINE_DIR" ]; then
    echo "❌ Error: Airflow engine directory not found at $ENGINE_DIR"
    exit 1
fi

# Copy the .env file from project to engine
echo "Copying .env from $PROJECT_DIR to $ENGINE_DIR"
cp "$PROJECT_DIR/.env" "$ENGINE_DIR/.env"

# Add JWT secret for Airflow API if it doesn't exist
if ! grep -q "AIRFLOW__API_FASTAPI__AUTH__TOKENS__API_AUTH__JWT_SECRET" "$ENGINE_DIR/.env"; then
    echo "Adding JWT secret to $ENGINE_DIR/.env"
    echo "AIRFLOW__API_FASTAPI__AUTH__TOKENS__API_AUTH__JWT_SECRET=$(openssl rand -hex 32)" >> "$ENGINE_DIR/.env"
fi

# Add AIRFLOW_PROJ_DIR to the .env file if not present
if ! grep -q "AIRFLOW_PROJ_DIR" "$ENGINE_DIR/.env"; then
    echo "Adding AIRFLOW_PROJ_DIR to $ENGINE_DIR/.env"
    echo "AIRFLOW_PROJ_DIR=$PROJECT_DIR" >> "$ENGINE_DIR/.env"
fi

# Add AIRFLOW_UID to the .env file if not present
if ! grep -q "AIRFLOW_UID" "$ENGINE_DIR/.env"; then
    echo "Adding AIRFLOW_UID to $ENGINE_DIR/.env"
    echo "AIRFLOW_UID=$(id -u)" >> "$ENGINE_DIR/.env"
fi

# Ensure the MINIO_ENDPOINT uses the Docker host IP instead of the service name
if grep -q "MINIO_ENDPOINT=http://minio:9000" "$ENGINE_DIR/.env"; then
    echo "Updating MINIO_ENDPOINT to use Docker host IP (172.17.0.1) in $ENGINE_DIR/.env"
    sed -i 's|MINIO_ENDPOINT=http://minio:9000|MINIO_ENDPOINT=http://172.17.0.1:9000|g' "$ENGINE_DIR/.env"
fi

echo "Environment variables synced successfully!"
echo ""
echo "⚠️ IMPORTANT NOTE: The MinIO endpoint must use the Docker host IP (172.17.0.1)"
echo "   instead of the container name (minio) for connectivity from Airflow containers."
echo ""
echo "Next steps:"
echo "1. cd $ENGINE_DIR"
echo "2. docker compose down"
echo "3. docker compose up -d"
echo ""
echo "Your environment variables should now be available to Airflow containers."

exit 0
