# Building an Automated YouTube Analytics Dashboard with Airflow, PySpark, MinIO, PostgreSQL & Grafana

**Author:** *Oliver Samuel*
**Date:** *October 2025*

---

## Introduction

This project explores the digital footprint of Raye, the UK chart-topping artist known for her soulful pop sound and breakout hits like Escapism. Using a custom-built **YouTube Analytics Pipeline** powered by **Apache Airflow**, **PySpark**, **MinIO**, **PostgreSQL** and **Grafana**, we analyzed Raye’s channel performance — from engagement trends to audience distribution.

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

## Step 4: Airflow Integration and Reusability

This pipeline is designed as a **modular Airflow project** that plugs into a reusable local engine (`~/airflow-docker`).

### Run Instructions

```bash
# Copy and configure environment variables
cp .env
chmod +x sync_env.sh
./sync_env.sh

# Start the Airflow engine
cd ~/airflow-docker
docker compose down
docker compose up -d
```

To manually test a DAG task inside Airflow:

```bash
docker exec -it airflow-docker-airflow-scheduler-1 \
  python /opt/airflow/dags/pipelines/youtube/extract.py
```

### Environment Variables

| Variable           | Description                                        |
| ------------------ | -------------------------------------------------- |
| `YOUTUBE_API_KEY`  | YouTube Data API v3 key                            |
| `MINIO_ACCESS_KEY` | MinIO access key                                   |
| `MINIO_SECRET_KEY` | MinIO secret key                                   |
| `MINIO_ENDPOINT`   | MinIO endpoint (default: `http://172.17.0.1:9000`) |

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

![More one engagements](images/panels_2.png)
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

This project shows how open-source data tools can build a **modern analytics stack** that’s automated, modular, and production-ready — no external cloud costs required.

![Dashboard shot](images/final_shot.png)
*Final dashboard hero shot.*

---