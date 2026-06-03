WITH source AS (
    SELECT * FROM {{ source('raw', 'stock_prices') }}
),

cleaned AS (
    SELECT
        id,
        UPPER(TRIM(symbol))             AS symbol,
        open,
        high,
        low,
        close,
        volume,
        trading_day::DATE               AS trading_day,
        previous_close,
        change,
        ROUND(change_percent::FLOAT, 4) AS change_percent,
        extracted_at::TIMESTAMP         AS extracted_at,
        loaded_at::TIMESTAMP            AS loaded_at,

        -- Derived columns
        ROUND(high - low, 2)            AS daily_range,
        CASE
            WHEN change >= 0 THEN 'GAIN'
            ELSE 'LOSS'
        END                             AS day_direction,

        -- Strip .BSE suffix for cleaner display
        REPLACE(UPPER(TRIM(symbol)), '.BSE', '') AS company_code

    FROM source
    WHERE symbol != 'TEST.BSE'  -- exclude test data
      AND close > 0
)

SELECT * FROM cleaned