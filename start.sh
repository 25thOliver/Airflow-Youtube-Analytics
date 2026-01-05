#!/bin/bash

# Startup script for Airflow YouTube Analytics project
# This script sets up and starts all Docker services

set -e

echo "🚀 Starting Airflow YouTube Analytics Pipeline..."
echo ""

# Check if .env exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found!"
    echo ""
    echo "Please create a .env file with the following required variables:"
    echo "  - YOUTUBE_API_KEY (required)"
    echo "  - AIRFLOW_UID (optional, will be auto-set if not provided)"
    echo "  - MINIO_ACCESS_KEY (optional, defaults to minioadmin)"
    echo "  - MINIO_SECRET_KEY (optional, defaults to minioadmin)"
    echo "  - POSTGRES_CONN_STRING (optional, has default)"
    echo ""
    echo "See README.md for a complete list of environment variables."
    echo ""
    exit 1
fi

# Set AIRFLOW_UID if not set (Linux/Mac)
if ! grep -q "AIRFLOW_UID" .env; then
    echo "🔧 Setting AIRFLOW_UID..."
    echo "AIRFLOW_UID=$(id -u)" >> .env
fi

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p logs plugins

# Start services
echo "🐳 Starting Docker services..."
docker compose up -d

echo ""
echo "⏳ Waiting for services to initialize (this may take 1-2 minutes)..."
sleep 10

# Wait for Airflow to be ready
echo "🔍 Checking Airflow status..."
until docker compose ps airflow-webserver | grep -q "healthy\|Up"; do
    echo "   Waiting for Airflow..."
    sleep 5
done

echo ""
echo "✅ Services are starting up!"
echo ""
echo "📍 Access your services:"
echo "   • Airflow Web UI:    http://localhost:8080 (airflow/airflow)"
echo "   • Grafana:           http://localhost:3000 (admin/admin)"
echo "   • MinIO Console:     http://localhost:9001 (minioadmin/minioadmin)"
echo ""
echo "📊 View logs: docker compose logs -f"
echo "🛑 Stop services: docker compose down"
echo ""

