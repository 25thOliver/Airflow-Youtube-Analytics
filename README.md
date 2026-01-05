# Building an Automated YouTube Analytics Dashboard with Airflow, PySpark, MinIO, PostgreSQL & Grafana

**Author:** *Oliver Samuel*

**Date:** *October 2025*

---

## Introduction

This project explores the digital footprint of **Raye**, the UK chart-topping artist known for her soulful pop sound and breakout hits like Escapism. Using a custom-built **YouTube Analytics Pipeline** powered by **Apache Airflow**, **PySpark**, **MinIO**, **PostgreSQL** and **Grafana**, we analyzed Raye’s channel performance — from engagement trends to audience distribution.

The goal was to design a scalable data workflow capable of extracting, transforming, and visualizing YouTube channel insights in real time. Beyond technical architecture, this analysis reveals how content release patterns, audience geography, and engagement rates evolve alongside Raye’s career milestones.

## Overview

This project demonstrates how to design, containerize, and automate an **end-to-end data engineering pipeline** for YouTube analytics using **Apache Airflow**, **PySpark**, **MinIO**, **PostgreSQL**, and **Grafana**.

It automatically fetches YouTube channel data, performs transformations in Spark, loads the results into a PostgreSQL warehouse, and visualizes insights in Grafana — all orchestrated by Airflow.

By the end, you’ll have a live dashboard showing:

* Total videos, views, and subscribers
* Average engagement rates
* Country-level view distribution
* Growth trends and publishing cadence

![Final Grafana dashboard overview](images/Grafana_dashboard.png)
![Final Grafana dashboard overview](images/grafana_2.png)
*Final Grafana dashboard overview.*

---

## Architecture Overview

Here’s the end-to-end data flow:

```
YouTube API → Raw JSON → MinIO (Data Lake)
         ↓
     PySpark Transform
         ↓
 PostgreSQL Warehouse
         ↓
 Grafana Dashboard (Visualization)
         ↓
 Airflow DAG (Automation & Scheduling)
```
![Architecture Diagram](images/architecture_diagram.png)

---

## Step 1: Automated Extraction with Airflow

The first DAG task — `extract_youtube_data` — uses the **YouTube Data API v3** to fetch metadata and statistics for each target channel.

The extracted JSON files are stored in **MinIO**, a local S3-compatible data lake.

Sample record:

```json
{
  "channel_id": "UC123456...",
  "channel_title": "Raye",
  "statistics": {
    "viewCount": "10402000",
    "subscriberCount": "251000",
    "videoCount": "159",
    "likeCount": "359000",
    "commentCount": "50382"
  },
  "country": "GB",
  "publishedAt": "2014-06-22T10:05:00Z"
}
```
![Raw data in MinIO browser](images/raw_minio.png)

---

## Step 2: Data Transformation with PySpark

Next, Airflow triggers the **transform task**, which runs `transform_youtube_data()` inside the same containerized environment.

It loads the raw files from MinIO using the S3A connector, casts numeric types, fills missing values, and computes engagement metrics like `views_per_video`, `like_ratio`, and `engagement_rate`.

### Key Transformations

```python
 transformed_df = (
        transformed_df
            .withColumn("like_count", lit(total_likes))
            .withColumn("comment_count", lit(total_comments))
            .withColumn(
                "views_per_video",
                (lit(total_views) / when(col("video_count") == 0, 1).otherwise(col("video_count")))
            )
            .withColumn(
                "subs_per_video",
                (col("subscriber_count") / when(col("video_count") == 0, 1).otherwise(col("video_count")))
            )
            .withColumn(
                "like_ratio",
                when(lit(total_views) > 0, round(lit(total_likes) / lit(total_views), 4)).otherwise(lit(0))
            )
            .withColumn(
                "comment_ratio",
                when(lit(total_views) > 0, round(lit(total_comments) / lit(total_views), 4)).otherwise(lit(0))
            )
            .withColumn(
                "engagement_rate",
                when(lit(total_views) > 0, round((lit(total_likes) + lit(total_comments)) / lit(total_views), 4)).otherwise(lit(0))
            )
    )
```

### Output Format

The cleaned dataset is stored back to MinIO as Parquet for optimized reads:

```python
transformed_df.write.mode("overwrite").parquet(
    "s3a://rayes-youtube/transformed/channel_stats_transformed.parquet"
)
```
![Spark job logs showing transformation success in Airflow](images/transformation_logs.png)

---

## Step 3: Load into PostgreSQL

Airflow’s final task — `load_to_postgres` — transfers the transformed Parquet data into PostgreSQL using a JDBC connector or pandas-based loader.

### Schema Alignment

| PySpark Column    | PostgreSQL Column | Type             |
| ----------------- | ----------------- | ---------------- |
| channel_id        | channel_id        | text             |
| channel_title     | channel_name      | text             |
| published_at      | published_at      | timestamp        |
| view_count        | view_count        | bigint           |
| subscriber_count  | subscriber_count  | bigint           |
| video_count       | video_count       | bigint           |
| like_count        | like_count        | bigint           |
| comment_count     | comment_count     | bigint           |
| like_ratio        | like_ratio        | double precision |
| comment_ratio     | comment_ratio     | double precision |
| engagement_rate   | engagement_rate   | double precision |
| views_per_video   | views_per_video   | double precision |
| channel_age_days  | channel_age_days  | bigint           |
| daily_view_growth | daily_view_growth | double precision |
| daily_sub_growth  | daily_sub_growth  | double precision |
| country           | country           | text             |

Note: `channel_title` in Spark maps to `channel_name` in PostgreSQL — the only column renamed during loading.
![Sample query results from PostgreSQL](images/SQL_query.png)
*Sample query results from PostgreSQL.*

---

## Step 4: Standalone Docker Setup

This project is now **fully independent** and self-contained. All services (Airflow, MinIO, PostgreSQL, and Grafana) are orchestrated via Docker Compose.

### Prerequisites

- Docker and Docker Compose installed
- YouTube Data API v3 key ([Get one here](https://developers.google.com/youtube/v3/getting-started))

### Quick Start

**Option 1: Using the startup script (recommended):**
```bash
cd airflow-youtube-analytics
chmod +x start.sh
./start.sh
```

**Option 2: Manual setup:**
1. **Clone and navigate to the project directory:**
   ```bash
   cd airflow-youtube-analytics
   ```

2. **Create `.env` file with required variables:**
   ```bash
   # Create .env file (it's gitignored, so create it manually)
   cat > .env << EOF
   # Required
   YOUTUBE_API_KEY=your_youtube_api_key_here
   
   # Airflow Configuration
   AIRFLOW_UID=$(id -u)
   AIRFLOW_PROJ_DIR=$(pwd)
   _AIRFLOW_WWW_USER_USERNAME=airflow
   _AIRFLOW_WWW_USER_PASSWORD=airflow
   
   # MinIO Configuration
   MINIO_ENDPOINT=http://minio:9000
   MINIO_ACCESS_KEY=minioadmin
   MINIO_SECRET_KEY=minioadmin
   
   # PostgreSQL Configuration
   POSTGRES_CONN_STRING=postgresql://postgres:postgres@postgres-analytics:5432/youtube_analytics
   
   # Grafana Configuration
   GRAFANA_ADMIN_USER=admin
   GRAFANA_ADMIN_PASSWORD=admin
   EOF
   ```
   
   Or manually create `.env` and add the variables. See the "Environment Variables" section below for details.

3. **Start all services:**
   ```bash
   docker compose up -d
   ```

5. **Wait for services to initialize** (about 1-2 minutes), then access:
   - **Airflow Web UI**: http://localhost:8080 (username: `airflow`, password: `airflow`)
   - **Grafana**: http://localhost:3000 (username: `admin`, password: `admin`)
   - **MinIO Console**: http://localhost:9001 (username: `minioadmin`, password: `minioadmin`)

### Service Details

| Service         | Port | Description                          |
| --------------- | ---- | ------------------------------------ |
| Airflow Web UI  | 8080 | DAG orchestration and monitoring     |
| Grafana         | 3000 | Data visualization dashboard          |
| MinIO API       | 9000 | S3-compatible object storage         |
| MinIO Console   | 9001 | MinIO web interface                  |
| PostgreSQL      | 5434 | Airflow metadata database (host port) |
| PostgreSQL      | 5433 | YouTube analytics database (host port) |

**Note:** The PostgreSQL services use different host ports (5434 and 5433) to avoid conflicts with local PostgreSQL installations. Inside Docker, they both use port 5432.

### Testing the Pipeline

To manually test a DAG task:

```bash
docker exec -it airflow-youtube-analytics-airflow-scheduler-1 \
  python /opt/airflow/dags/pipelines/youtube/extract.py
```

### Environment Variables

Create a `.env` file in the project root with the following variables:

| Variable              | Description                                        | Required | Default                    |
| --------------------- | -------------------------------------------------- | -------- | -------------------------- |
| `YOUTUBE_API_KEY`     | YouTube Data API v3 key                            | ✅ Yes   | -                          |
| `AIRFLOW_UID`         | User ID for Airflow (Linux/Mac: run `id -u`)       | No       | `50000`                    |
| `AIRFLOW_PROJ_DIR`    | Project directory path                              | No       | Current directory          |
| `_AIRFLOW_WWW_USER_USERNAME` | Airflow web UI username                        | No       | `airflow`                  |
| `_AIRFLOW_WWW_USER_PASSWORD` | Airflow web UI password                        | No       | `airflow`                  |
| `MINIO_ENDPOINT`      | MinIO endpoint (use `minio:9000` for Docker)        | No       | `http://minio:9000`        |
| `MINIO_ACCESS_KEY`    | MinIO access key                                   | No       | `minioadmin`               |
| `MINIO_SECRET_KEY`    | MinIO secret key                                   | No       | `minioadmin`               |
| `POSTGRES_CONN_STRING`| PostgreSQL connection string for analytics        | No       | Auto-configured            |
| `GRAFANA_ADMIN_USER`  | Grafana admin username                             | No       | `admin`                    |
| `GRAFANA_ADMIN_PASSWORD` | Grafana admin password                          | No       | `admin`                    |

**Note:** The `.env` file is gitignored, so you'll need to create it manually. Only `YOUTUBE_API_KEY` is required; all other variables have sensible defaults.

### Managing Services

**Stop all services:**
```bash
docker compose down
```

**Stop and remove volumes (⚠️ deletes all data):**
```bash
docker compose down -v
```

**View logs:**
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f airflow-scheduler
docker compose logs -f postgres-analytics
```

**Restart a specific service:**
```bash
docker compose restart airflow-scheduler
```

### Troubleshooting

**Issue: Airflow DAGs not appearing**
- Wait 1-2 minutes for Airflow to initialize
- Check logs: `docker compose logs airflow-scheduler`
- Ensure DAGs are in the `dags/` directory

**Issue: MinIO connection errors**
- Verify MinIO is running: `docker compose ps minio`
- Check MinIO logs: `docker compose logs minio`
- Ensure `MINIO_ENDPOINT` uses service name `minio:9000` (not localhost)

**Issue: PostgreSQL connection errors**
- Wait for database to be healthy: `docker compose ps postgres-analytics`
- Check connection string uses `postgres-analytics:5432` (service name)

**Issue: PySpark errors**
- Verify Java is installed in Airflow container: `docker exec airflow-scheduler java -version`
- Check Spark logs in Airflow scheduler logs

![Airflow DAG graph showing extract → transform → load tasks](images/DAG_graph.png)
*Airflow DAG graph showing extract → transform → load tasks.*

---

## Step 5: Visualization with Grafana

Grafana connects directly to PostgreSQL to visualize key metrics.

### Example Queries

#### 1. Overview Metrics

```sql
SELECT 
  SUM(view_count) AS total_views,
  SUM(subscriber_count) AS total_subscribers,
  COUNT(DISTINCT channel_id) AS total_channels
FROM "raye_youtube_channel_stats";
```

#### 2. Engagement Rate

```sql
SELECT 
  ROUND(AVG((like_count + comment_count)::numeric / NULLIF(view_count, 0)), 4) AS avg_engagement_rate,
  SUM(like_count) AS total_likes,
  SUM(comment_count) AS total_comments
FROM "raye_youtube_channel_stats";
```

#### 3. Country Breakdown

```sql
SELECT 
    published_at AS time,      
    country, 
    SUM(view_count) AS total_views
FROM "raye_youtube_channel_stats"
GROUP BY published_at, country
ORDER BY time DESC, total_views DESC;

```
![Country Metrics](images/country_insights.png)
*Grafana panels showing engagement and country metrics.*

---

## Insights

* **Engagement Peaks:** Engagement rates spike around high-visibility video releases.
* **View Concentration:** Most traffic originates from English-speaking regions.
* **Content Rhythm:** Publishing trends show periodic releases tied to album cycles.

![More on engagements](images/panels_2.png)
*Chart highlighting peak engagement days.*

---

## Tech Stack Summary

| Layer            | Tool                  |
| ---------------- | --------------------- |
| Orchestration    | Apache Airflow        |
| Data Storage     | MinIO (S3-compatible) |
| Transformation   | PySpark               |
| Warehouse        | PostgreSQL            |
| Visualization    | Grafana               |
| Containerization | Docker Compose        |

---

## Automation Summary

Each Airflow DAG run performs the full cycle:

1. **Extract:** Fetch YouTube channel data
2. **Transform:** Clean and compute new metrics via PySpark
3. **Load:** Write clean results into PostgreSQL
4. **Visualize:** Grafana auto-refreshes metrics in near real time

---

## Key Takeaways

- PySpark and MinIO enable scalable, cloud-like ETL locally.
- Airflow provides robust scheduling and retry mechanisms.
- Grafana and PostgreSQL make analytics exploration seamless.
- Modular design allows reuse across multiple data sources or APIs.

---

## Conclusion

This project went beyond dashboards and data pipelines — it told a story about how an artist’s digital rhythm mirrors their creative journey. By building a robust analytics workflow for Raye’s YouTube channel, we connected raw engagement metrics to real-world momentum — from viral singles to album releases.

The pipeline’s architecture, powered by **Apache Airflow**, **PySpark**, **MinIO**, **PostgreSQL**, and **Grafana**, proved not just scalable but insightful — offering a live pulse on fan interactions, audience geography, and engagement surges tied to content drops.

As a next step, the same framework can be extended to analyze cross-platform trends (Spotify, Instagram, TikTok) and measure how each channel amplifies an artist’s reach in the streaming era.

![Dashboard shot](images/final_shot.png)
*Final dashboard hero shot.*

*Data meets artistry — and every like, view, and comment becomes a note in the bigger symphony of audience connection.*

---