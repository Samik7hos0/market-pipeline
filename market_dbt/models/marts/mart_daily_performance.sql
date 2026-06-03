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

        -- Performance metrics
        ROUND((close - previous_close) / NULLIF(previous_close, 0) * 100, 4)
            AS return_percent,

        -- Volume in millions
        ROUND(volume / 1000000.0, 2)
            AS volume_millions,

        -- Price momentum — is today's close above open?
        CASE
            WHEN close > open THEN 'BULLISH'
            WHEN close < open THEN 'BEARISH'
            ELSE 'NEUTRAL'
        END AS momentum,

        loaded_at

    FROM stg
)

SELECT * FROM performance