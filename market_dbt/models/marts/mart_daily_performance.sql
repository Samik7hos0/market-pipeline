-- Mart model: mart_daily_performance
-- Materialised as a TABLE (full refresh each run).
-- Adds three analytical columns on top of the staging view:
--   return_percent  — percentage return vs previous close (what most finance dashboards show)
--   volume_millions — raw share volume rescaled to millions for readability
--   momentum        — whether the stock closed above or below its open (intraday direction)
-- This is the primary mart for day-level analysis of individual stock performance.

WITH stg AS (
    SELECT * FROM {{ ref('stg_stock_prices') }}
),

performance AS (
    SELECT
        trading_day,
        symbol,
        company_code,
        open,
        high,
        low,
        close,
        volume,
        change,
        change_percent,
        daily_range,
        day_direction,
        previous_close,

        -- Percentage return relative to previous close; NULLIF prevents divide-by-zero
        ROUND((close - previous_close) / NULLIF(previous_close, 0) * 100, 4)
            AS return_percent,

        -- Volume expressed in millions — easier to read at BSE scale (e.g. 2.4M vs 2400000)
        ROUND(volume / 1000000.0, 2)
            AS volume_millions,

        -- Intraday momentum: did the stock close higher or lower than it opened?
        CASE
            WHEN close > open THEN 'BULLISH'
            WHEN close < open THEN 'BEARISH'
            ELSE 'NEUTRAL'
        END AS momentum,

        loaded_at

    FROM stg
)

SELECT * FROM performance