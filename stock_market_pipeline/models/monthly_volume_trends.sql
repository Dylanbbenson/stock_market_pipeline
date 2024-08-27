{{ config(
    materialized='table'
) }}

SELECT
    ticker,
    to_char(to_timestamp(processed_timestamp), 'YYYY-MM')  AS month,
    SUM(volume) AS total_volume,
    AVG(volume) AS avg_volume
FROM {{ ref('stock_prices') }}
GROUP BY ticker, month
ORDER BY total_volume DESC
