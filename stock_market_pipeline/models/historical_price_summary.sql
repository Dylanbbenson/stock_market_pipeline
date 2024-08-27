{{ config(
    materialized='table'
) }}

SELECT
    ticker,
    MIN(low_price) AS min_low_price,
    MAX(high_price) AS max_high_price,
    MIN(open_price) AS min_open_price,
    AVG(last_price) AS avg_last_price
FROM {{ ref('stock_prices') }}
GROUP BY ticker
