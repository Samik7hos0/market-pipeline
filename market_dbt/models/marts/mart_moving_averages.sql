-- Mart model: mart_moving_averages
-- Materialised as a TABLE (full refresh each run).
-- Uses Snowflake window functions to compute rolling averages per symbol,
-- ordered by trading_day within each symbol partition.
-- All windows are bounded (ROWS BETWEEN N PRECEDING AND CURRENT ROW) so
-- early rows get a shorter window rather than returning NULL — a deliberate
-- trade-off that keeps the table useful from day one as data accumulates.

WITH stg AS (
    SELECT * FROM {{ ref('stg_stock_prices') }}
),

moving_avgs AS (
    SELECT
        trading_day,
        symbol,
        company_code,
        close,
        volume,

        -- 3-day simple moving average of closing price per stock
        -- Window: current row + 2 preceding trading days
        ROUND(AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY trading_day
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS ma_3day,

        -- 5-day simple moving average — a common short-term trend indicator
        -- Window: current row + 4 preceding trading days (one full trading week)
        ROUND(AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY trading_day
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ), 2) AS ma_5day,

        -- 3-day average daily volume — gives context for whether price moves had volume behind them
        ROUND(AVG(volume) OVER (
            PARTITION BY symbol
            ORDER BY trading_day
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 0) AS avg_volume_3day,

        -- Binary signal: is today's price above or below its own 3-day average?
        -- Recomputes the same window as ma_3day — consistent by definition
        CASE
            WHEN close > AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY trading_day
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) THEN 'ABOVE_MA'
            ELSE 'BELOW_MA'
        END AS price_vs_ma

    FROM stg
)

SELECT * FROM moving_avgs