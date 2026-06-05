# Indian Market Batch ELT Pipeline

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8?logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Transform-FF694B?logo=dbt&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-Orchestration-017CEE?logo=apache-airflow&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS_S3-Data_Lake-FF9900?logo=amazons3&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker&logoColor=white)

## What This Pipeline Does

Every weekday morning before market open, this pipeline pulls the previous day's closing prices for five large-cap BSE stocks from the Alpha Vantage API and dual-writes them — structured rows go into Snowflake's RAW schema for transformation, while the raw JSON lands in an AWS S3 data lake as an immutable source of truth. dbt then builds a clean staging view and two analytical mart tables covering daily performance metrics and moving averages. Eleven data-quality tests run automatically on every execution; the DAG fails loudly if any assertion breaks. The result is a queryable, validated dataset tracking price movements, volume trends, and short-term momentum signals for RELIANCE, TCS, HDFCBANK, INFY, and WIPRO — rebuilt fresh each trading day, idempotently, inside a reproducible Docker environment.

This is **Part 1 of a two-project Data Engineering portfolio**. Part 2 ([streaming-pipeline](https://github.com/Samik7hos0/streaming-pipeline)) adds real-time event streaming on the same BSE domain. Together they demonstrate batch ELT and streaming patterns side-by-side.

---

## Architecture

![Architecture](Indian%20Market%20ELT%20Pipeline%20Architecture.png)

```
┌──────────────────────────┐
│     Alpha Vantage API    │
│  BSE daily closing data  │
└────────────┬─────────────┘
             │  HTTP (Mon–Fri, 06:00 IST)
             │
      ┌──────▼───────┐
      │  pipeline.py  │  ◄─ Airflow task 1: extract_and_load
      └──────┬────────┘
             │  dual-write
     ┌───────┴──────────────────┐
     │                          │
     ▼                          ▼
┌────────────────┐    ┌──────────────────────────────┐
│   Snowflake    │    │          AWS S3               │
│  RAW schema    │    │  de-grind-market-data-samik   │
│  (structured)  │    │  raw/stocks_YYYYMMDD.json     │
└───────┬────────┘    │  (immutable · versioned · SSE)│
        │              └──────────────────────────────┘
        │  Airflow task 2: dbt_run
        ▼
┌───────────────────────────────────┐
│         dbt Transformation        │
│  staging/  stg_stock_prices       │  ← view, cleaned & typed
│  marts/    mart_daily_performance │  ← table, daily P&L metrics
│            mart_moving_averages   │  ← table, 5-/10-day MAs
└───────────────────────────────────┘
        │  Airflow task 3: dbt_test
        ▼
  ✓  11 schema + value tests — pipeline halts on failure
```

---

## Stack

| Layer | Tool | Role |
|---|---|---|
| Extraction | Python + Alpha Vantage API | Fetches daily OHLCV for 5 BSE tickers |
| Warehouse | Snowflake (RAW schema) | Structured storage; source for dbt |
| Data Lake | AWS S3 (`ap-south-1`) | Immutable raw backup; future streaming landing zone |
| Transformation | dbt Core | Staging views + analytical mart tables |
| Orchestration | Apache Airflow (Dockerized) | Schedules, injects secrets, enforces task order |
| Infrastructure | Docker + custom image | Airflow + dbt + boto3 baked in; reproducible on any machine |

---

## Data Flow

Each pipeline run performs a **dual-write** — the same pattern used by production data teams to decouple the warehouse from the raw source of truth:

- **Snowflake (RAW schema)** — structured rows that dbt can immediately transform and analysts can query via SQL
- **AWS S3** (`raw/stocks_YYYYMMDD.json`) — immutable JSON backup enabling full reprocessing without re-hitting the rate-limited API; also the natural landing zone for the companion streaming pipeline

The two writes happen inside a single Airflow task (`extract_and_load`), so either both succeed or the task fails and Airflow retries — no partial state reaches the warehouse without a corresponding lake backup.

---

## Project Structure

```
market_pipeline/
│
├── src/
│   ├── extract/
│   │   └── stock_extractor.py    # Alpha Vantage API client
│   │                             #   Fetches daily OHLCV for each BSE ticker; returns normalised dicts
│   │
│   ├── load/
│   │   └── snowflake_loader.py   # Snowflake writer with dedup guard
│   │                             #   Checks (symbol, trading_day) before INSERT; safe to re-run same day
│   │
│   ├── utils/
│   │   └── s3_helper.py          # boto3 S3 helper (upload / download / list)
│   │                             #   Wraps PutObject/GetObject; credentials injected at runtime
│   │
│   └── pipeline.py               # Orchestration entry point
│                                 #   Calls extractor → Snowflake loader → S3 helper in sequence
│
├── market_dbt/
│   └── models/
│       ├── staging/
│       │   ├── stg_stock_prices.sql   # Cleans & type-casts RAW rows into a view
│       │   ├── sources.yml            # Declares RAW schema as dbt source
│       │   └── schema.yml             # not_null tests on staging columns
│       │
│       └── marts/
│           ├── mart_daily_performance.sql   # Daily P&L, % change, volume delta (table)
│           ├── mart_moving_averages.sql     # 5-day and 10-day moving averages (table)
│           └── schema.yml                   # accepted_values tests: GAIN/LOSS, BULLISH/BEARISH/NEUTRAL
│
├── airflow_dags/
│   └── market_pipeline_dag.py    # Three-task DAG (extract_and_load → dbt_run → dbt_test)
│                                 #   Cron 0 6 * * 1-5; secrets pulled from Airflow Variables
│
├── tests/
│   └── test_extractor.py         # Unit tests for the API extraction layer
│
├── Dockerfile                    # Custom Airflow image with dbt + boto3 + certifi baked in
├── docker-compose.yml            # Spins up Airflow webserver, scheduler, and worker
├── requirements.txt              # Python dependencies
└── .env.example                  # Template for required credentials (copy → .env, never commit .env)
```

---

## Layer Responsibilities

| Layer | File | Job |
|---|---|---|
| Extract | `src/extract/stock_extractor.py` | Calls Alpha Vantage API; returns normalised OHLCV dicts for 5 tickers |
| Load — Warehouse | `src/load/snowflake_loader.py` | Inserts rows into Snowflake RAW; skips duplicates on `(symbol, trading_day)` |
| Load — Data Lake | `src/utils/s3_helper.py` | Uploads raw JSON to S3 via boto3; credentials never touch the filesystem |
| Orchestrate | `src/pipeline.py` | Chains extract → warehouse → lake; single entry point for the Airflow task |
| Stage | `market_dbt/models/staging/` | Casts raw strings to proper types; exposed as a view (`stg_stock_prices`) |
| Mart | `market_dbt/models/marts/` | Materialises two tables consumed by analysts: performance metrics + MAs |
| Schedule | `airflow_dags/market_pipeline_dag.py` | Declares task graph, injects runtime secrets, enforces Mon–Fri schedule |

---

## Tracked Stocks

`RELIANCE.BSE` · `TCS.BSE` · `HDFCBANK.BSE` · `INFY.BSE` · `WIPRO.BSE`

---

## Setup

### Prerequisites

- Docker + Docker Compose
- Alpha Vantage API key (free tier)
- Snowflake account (free trial works)
- AWS account with S3 access

### Run locally

```bash
# 1. Clone and configure credentials
cp .env.example .env          # fill in all values

# 2. Start Airflow (includes dbt)
docker compose up --build -d

# 3. Import Airflow Variables (credentials for the DAG)
#    In the Airflow UI: Admin → Variables → Import
#    Variables needed: ALPHA_VANTAGE_KEY, SNOWFLAKE_* (user/password/account/db/schema/warehouse), AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME

# 4. Run the pipeline manually (outside Airflow)
python src/pipeline.py

# 5. Run dbt transforms + tests manually
cd market_dbt && dbt run && dbt test
```

---

## Key Engineering Decisions

### Dual-write: warehouse + data lake

Every extraction writes to Snowflake **and** S3 atomically within the same Airflow task. This decouples the source of truth (S3) from the transformation target (Snowflake). If a dbt model ever needs to be rebuilt from scratch, the raw data is already in S3 — no second API call required against a rate-limited free-tier key. The S3 key format (`raw/stocks_YYYYMMDD.json`) makes backfilling by date trivial.

### Least-privilege IAM

The pipeline's AWS IAM user is bound to a custom policy (`de-grind-s3-market-data`) that grants only what is needed:

- `s3:ListBucket` + `s3:GetBucketLocation` on the bucket itself
- `s3:GetObject` + `s3:PutObject` + `s3:DeleteObject` on bucket contents

No `AmazonS3FullAccess`. If the credentials leaked, the blast radius is exactly one bucket. AWS credentials are injected at runtime from Airflow Variables — they never appear in code or committed files.

### S3 bucket hardening

Bucket `de-grind-market-data-samik` (ap-south-1) has:
- **Versioning enabled** — every overwrite is preserved; accidental deletes are recoverable
- **Public access blocked** — no ACL or bucket policy can make objects public
- **SSE-S3 encryption** — data encrypted at rest by default

### dbt medallion layering (RAW → STAGING → MARTS)

Raw API responses land in Snowflake's `RAW` schema exactly as received — no transformation at load time. The `stg_stock_prices` view casts strings to proper types and renames columns to a consistent convention. Mart models (`mart_daily_performance`, `mart_moving_averages`) materialise as tables containing only the analytical columns a consumer would query. Each layer has one job; a change in the API response format touches only the staging model.

### 11 dbt data-quality tests

`schema.yml` files assert:
- `not_null` on every key column (symbol, trading_day, close_price, volume, …)
- `accepted_values` on `day_direction` (`GAIN` / `LOSS`) and `momentum` (`BULLISH` / `BEARISH` / `NEUTRAL`)

The DAG runs `dbt test` as a third task, downstream of `dbt_run`. If any assertion fails, Airflow marks the DAG run failed — bad data never silently reaches the mart tables.

### Airflow task dependency and scheduling

```
extract_and_load  →  dbt_run  →  dbt_test
```

The chain is strict: dbt transforms only run after raw data is confirmed loaded; quality tests only run after transforms complete. Cron `0 6 * * 1-5` fires at 06:00 IST Monday–Friday — before BSE opens — with `catchup=False` so missed weekend runs don't trigger a backfill storm on Monday.

### Idempotent loading

Before inserting a record, `snowflake_loader.py` checks for an existing row on `(symbol, trading_day)`. Re-triggering the DAG on the same day — whether from a failure retry or a manual run — produces no duplicate rows. The pipeline is safe to run multiple times.

### Custom Airflow Docker image

The `Dockerfile` builds from the official Airflow base and bakes in dbt-snowflake, boto3, and certifi. The orchestration environment rebuilds identically on any machine from `docker compose up --build`. There are no manual pip installs, no environment drift between runs, and no dependency on the host Python.

### Secrets via Airflow Variables

No credentials appear in DAG code or any committed file. The Airflow task reads `ALPHA_VANTAGE_KEY`, `SNOWFLAKE_PASSWORD`, AWS keys, and all other secrets from Airflow's Variable store at runtime. `.env.example` documents the required variable names; `.gitignore` ensures `.env` is never committed.

---

## Related Project — Part 2: Real-Time Streaming Pipeline

| | Part 1 (this repo) | Part 2 |
|---|---|---|
| **Repo** | [market-pipeline](https://github.com/Samik7hos0/market-pipeline) | [streaming-pipeline](https://github.com/Samik7hos0/streaming-pipeline) |
| **Pattern** | Batch ELT (daily) | Real-time event streaming |
| **Domain** | BSE stock prices | Same BSE domain |
| **Cadence** | Once per trading day | Continuous / low-latency |

Both projects target the same Indian market domain, making them a natural pair: **market-pipeline** builds the historical batch layer that analysts query at the start of each trading day; **streaming-pipeline** adds the real-time layer that processes intraday events as they arrive. Together they form a complete Lambda-style data architecture — batch accuracy paired with streaming freshness — across a production-realistic financial dataset.
