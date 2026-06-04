# Market Pipeline

An end-to-end ELT pipeline for BSE (Bombay Stock Exchange) stock data, built with Python, Snowflake, dbt, Apache Airflow, and AWS S3. The pipeline runs on a schedule, writes to both a cloud data warehouse and a data lake, validates its own data quality on every run, and runs in a reproducible containerized environment.

## What This Pipeline Does

Every weekday morning before market open, this pipeline pulls the latest closing prices for five large-cap BSE stocks from the Alpha Vantage API. It loads them into Snowflake and, in parallel, writes the same raw data to an AWS S3 data lake. dbt then transforms the raw data into clean staging views and analytical mart tables covering daily performance metrics and moving averages. The result is a queryable dataset that tracks price movements, volume trends, and short-term momentum signals for RELIANCE, TCS, HDFCBANK, INFY, and WIPRO.

## Architecture

```
Alpha Vantage API → Python (Extract) → Snowflake RAW → dbt (Transform) → Mart tables
│                                   ↑
└──────→ AWS S3 (Data Lake)   Orchestrated by Airflow
```

![Architecture](Indian%20Market%20ELT%20Pipeline%20Architecture.png)

## Stack

| Layer | Tool |
|-------|------|
| Extraction | Python + Alpha Vantage API |
| Warehouse | Snowflake |
| Data Lake | AWS S3 (ap-south-1) |
| Transformation | dbt |
| Orchestration | Apache Airflow (Dockerized) |

## Data Flow

Every pipeline run writes to two destinations — a dual-write pattern used by real production data teams:

- **Snowflake** — structured warehouse layer that dbt transforms and analysts query
- **AWS S3** — immutable raw data lake backup (`raw/stocks_YYYYMMDD.json`), enabling reprocessing and serving as the landing zone for future streaming workloads

## Project Structure

```
market_pipeline/
├── src/
│   ├── extract/stock_extractor.py   # Alpha Vantage API client
│   ├── load/snowflake_loader.py     # Snowflake loader with dedup logic
│   ├── utils/s3_helper.py           # boto3 S3 upload/download/list
│   └── pipeline.py                  # Entry point: extract → Snowflake → S3
├── market_dbt/
│   └── models/
│       ├── staging/                 # stg_stock_prices (view)
│       └── marts/                   # mart_daily_performance, mart_moving_averages (tables)
├── airflow_dags/
│   └── market_pipeline_dag.py       # Daily DAG: Mon–Fri at 06:00 IST
├── tests/                           # Python unit tests for the extractor
└── .env.example                     # Required env vars (copy to .env)
```

## Tracked Stocks

RELIANCE.BSE, TCS.BSE, HDFCBANK.BSE, INFY.BSE, WIPRO.BSE

## Setup

1. Copy `.env.example` to `.env` and fill in your credentials
2. Install dependencies: `pip install -r requirements.txt`
3. Run pipeline manually: `python src/pipeline.py`
4. dbt: `cd market_dbt && dbt run && dbt test`

## Airflow Schedule

`0 6 * * 1-5` — 06:00 IST, Monday–Friday. See Key Engineering Decisions below for the reasoning behind the schedule and secrets approach.

## Key Engineering Decisions

**Incremental loading / idempotency**
Before inserting a record, the loader checks whether a row already exists for that `(symbol, trading_day)` pair. Re-running the pipeline on the same day is safe — no duplicate rows accumulate in Snowflake.

**Dual-write to warehouse and data lake**
Each run writes raw data to both Snowflake (for transformation and querying) and AWS S3 (as an immutable backup). This decouples the warehouse from the source-of-truth raw layer, so data can always be reprocessed from S3 without re-hitting the rate-limited API.

**Medallion architecture (RAW → STAGING → MARTS)**
Raw API responses land in Snowflake's `RAW` schema exactly as received. dbt staging models clean and type-cast the data into views. Mart models materialise as tables containing the analytical columns — daily performance and moving averages — that a consumer would actually query.

**Secrets management via Airflow Variables**
No credentials appear in DAG code or in any committed file. The Airflow task injects `ALPHA_VANTAGE_KEY`, `SNOWFLAKE_PASSWORD`, AWS keys, and all other secrets at runtime from Airflow's Variable store.

**Least-privilege cloud access**
The pipeline's AWS IAM user is scoped to a single S3 bucket with only the actions it needs (`ListBucket`, `GetObject`, `PutObject`, `DeleteObject`) — no broad `S3FullAccess`. If the credentials leaked, the blast radius is one bucket.

**Reproducible containerized environment**
Airflow runs via a custom Docker image with dbt and all dependencies baked in, so the orchestration environment rebuilds identically on any machine and survives container restarts without manual reinstalls.

**dbt data quality tests**
`schema.yml` files define `not_null` tests on key columns, plus `accepted_values` tests on `day_direction` (`GAIN` / `LOSS`) and `momentum` (`BULLISH` / `BEARISH` / `NEUTRAL`). The DAG runs `dbt test` as a downstream task so the pipeline fails loudly if data quality drops. 11 tests run on every execution.

**Market-aware scheduling**
The DAG cron `0 6 * * 1-5` runs at 06:00 IST, Monday–Friday — before BSE opens — with `catchup=False`. Previous day's closing data is ready for analysis at the start of each trading day, and weekends are skipped automatically.