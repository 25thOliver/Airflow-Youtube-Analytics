#!/bin/bash
set -e

# Create additional database for YouTube analytics
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE youtube_analytics;
    GRANT ALL PRIVILEGES ON DATABASE youtube_analytics TO $POSTGRES_USER;
EOSQL

echo "Database 'youtube_analytics' created successfully!"

