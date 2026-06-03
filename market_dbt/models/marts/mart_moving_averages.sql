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

        -- 3-day moving average (we only have a few days of data now,
        -- will be meaningful as data accumulates daily)
        ROUND(AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY trading_day
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS ma_3day,

        -- 5-day moving average
        ROUND(AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY trading_day
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ), 2) AS ma_5day,

        -- Average volume 3 days
        ROUND(AVG(volume) OVER (
            PARTITION BY symbol
            ORDER BY trading_day
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 0) AS avg_volume_3day,

        -- Is current price above 3-day MA? (momentum signal)
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