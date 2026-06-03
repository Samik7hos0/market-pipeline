# Market Pipeline

An end-to-end ELT pipeline for BSE (Bombay Stock Exchange) stock data, built with Python, Snowflake, dbt, and Apache Airflow.

## What This Pipeline Does

Every weekday morning before market open, this pipeline pulls the latest closing prices for five large-cap BSE stocks from the Alpha Vantage API and loads them into Snowflake. dbt then transforms the raw data into clean staging views and analytical mart tables covering daily performance metrics and moving averages. The result is a queryable dataset that tracks price movements, volume trends, and short-term momentum signals for RELIANCE, TCS, HDFCBANK, INFY, and WIPRO.

## Architecture

```
Alpha Vantage API → Python (Extract) → Snowflake RAW → dbt (Transform) → Mart tables
                                                                  ↑
                                                          Orchestrated by Airflow
```

## Stack

| Layer | Tool |
|-------|------|
| Extraction | Python + Alpha Vantage API |
| Storage | Snowflake |
| Transformation | dbt |
| Orchestration | Apache Airflow |

## Project Structure

```
market_pipeline/
├── src/
│   ├── extract/stock_extractor.py   # Alpha Vantage API client
│   ├── load/snowflake_loader.py     # Snowflake loader with dedup logic
│   └── pipeline.py                 # Entry point: extract → load
├── market_dbt/
│   └── models/
│       ├── staging/                 # stg_stock_prices (view)
│       └── marts/                  # mart_daily_performance, mart_moving_averages (table)
├── airflow_dags/
│   └── market_pipeline_dag.py      # Daily DAG: Mon–Fri at 06:00 IST
└── .env.example                    # Required env vars (copy to .env)
```

## Tracked Stocks

RELIANCE.BSE, TCS.BSE, HDFCBANK.BSE, INFY.BSE, WIPRO.BSE

## Setup

1. Copy `.env.example` to `.env` and fill in your credentials
2. Install dependencies: `pip install -r requirements.txt`
3. Run pipeline manually: `python src/pipeline.py`
4. dbt: `cd market_dbt && dbt run && dbt test`

## Airflow Schedule

Runs daily Monday–Friday at 06:00 IST (`0 6 * * 1-5`). Credentials are injected via Airflow Variables — no secrets in DAG code.

## Key Engineering Decisions

**Incremental loading / idempotency**
Before inserting a record, the loader checks whether a row already exists for that `(symbol, trading_day)` pair. Re-running the pipeline on the same day is safe — no duplicate rows accumulate in Snowflake.

**Medallion architecture (RAW → STAGING → MARTS)**
Raw API responses land in Snowflake's `RAW` schema exactly as received. dbt staging models clean and type-cast the data into views. Mart models materialise as tables containing the analytical columns — daily performance and moving averages — that a consumer would actually query.

**Secrets management via Airflow Variables**
No credentials appear in DAG code or in any committed file. The Airflow task injects `ALPHA_VANTAGE_KEY`, `SNOWFLAKE_PASSWORD`, and all other secrets at runtime from Airflow's encrypted Variable store.

**dbt data quality tests**
`schema.yml` defines `not_null` tests on `id`, `symbol`, `close`, and `trading_day`, plus an `accepted_values` test on `day_direction` (`GAIN` / `LOSS`). The DAG runs `dbt test` as a downstream task so the pipeline fails loudly if data quality drops.

**Market-aware scheduling**
The DAG cron `0 6 * * 1-5` runs at 06:00 IST, Monday–Friday — before BSE opens — with `catchup=False`. Previous day's closing data is ready for analysis at the start of each trading day, and weekends are skipped automatically.
