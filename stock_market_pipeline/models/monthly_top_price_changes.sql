{{ config(
    materialized='table'
) }}

SELECT
    ticker,
    to_char(to_timestamp(processed_timestamp), 'YYYY-MM')  AS month,
    MAX(price_change) AS max_price_change,
    MIN(price_change) AS min_price_change,
    MAX(percentchange) AS max_percent_change,
    MIN(percentchange) AS min_percent_change
FROM {{ ref('stock_prices') }}
GROUP BY ticker, month
ORDER BY max_price_change DESC
