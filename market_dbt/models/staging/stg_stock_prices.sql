-- Staging model: stg_stock_prices
-- Reads from raw.stock_prices (the table written by the Python loader).
-- Responsibilities:
--   1. Type-cast columns to their correct types (DATE, TIMESTAMP, FLOAT)
--   2. Normalise the symbol string (trim whitespace, uppercase)
--   3. Derive two business columns — daily_range and day_direction
--   4. Filter out test records and rows with a zero close price

WITH source AS (
    SELECT * FROM {{ source('raw', 'stock_prices') }}
),

-- Normalise symbol once so downstream CTEs don't repeat UPPER(TRIM(...))
normalised AS (
    SELECT
        *,
        UPPER(TRIM(symbol)) AS symbol_clean
    FROM source
    WHERE UPPER(TRIM(symbol)) != 'TEST.BSE'  -- exclude manual test inserts from the loader __main__ block
      AND close > 0                           -- guard against API returning zero prices
),

cleaned AS (
    SELECT
        id,
        symbol_clean                              AS symbol,
        open,
        high,
        low,
        close,
        volume,
        trading_day::DATE                         AS trading_day,
        previous_close,
        change,
        ROUND(change_percent::FLOAT, 4)           AS change_percent,
        extracted_at::TIMESTAMP                   AS extracted_at,
        loaded_at::TIMESTAMP                      AS loaded_at,

        -- High minus low gives the intraday price swing for the day
        ROUND(high - low, 2)                      AS daily_range,

        -- Label each day as a gain or loss based on the net change
        CASE
            WHEN change >= 0 THEN 'GAIN'
            ELSE 'LOSS'
        END                                       AS day_direction,

        -- Remove the exchange suffix so downstream models show e.g. "RELIANCE" not "RELIANCE.BSE"
        REPLACE(symbol_clean, '.BSE', '')         AS company_code

    FROM normalised
)

SELECT * FROM cleaned