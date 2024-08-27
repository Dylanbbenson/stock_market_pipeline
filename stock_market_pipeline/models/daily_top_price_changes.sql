{{ config(
    materialized='table'
) }}

SELECT
    ticker,
    cast(to_timestamp(processed_timestamp) as date) AS day,
    MAX(price_change) AS max_price_change,
    MIN(price_change) AS min_price_change,
    MAX(percentchange) AS max_percent_change,
    MIN(percentchange) AS min_percent_change
FROM {{ ref('stock_prices') }}
GROUP BY ticker, day
ORDER BY max_price_change DESC
