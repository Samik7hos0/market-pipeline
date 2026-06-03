# Market Pipeline

An end-to-end ELT pipeline for BSE (Bombay Stock Exchange) stock data, built with Python, Snowflake, dbt, and Apache Airflow.

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

1. Copy `.env.example` to `.env` and fill in credentials
2. Install dependencies: `pip install -r requirements.txt`
3. Run pipeline manually: `python src/pipeline.py`
4. dbt: `cd market_dbt && dbt run && dbt test`

## Airflow Schedule

Runs daily Monday–Friday at 06:00 IST (`0 6 * * 1-5`). Credentials are injected via Airflow Variables — no secrets in DAG code.
