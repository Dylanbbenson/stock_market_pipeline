{{ config(
    materialized='table'
) }}

WITH cte AS (
    SELECT
        ticker,
        processed_timestamp,
        last_price
    FROM {{ ref('stock_prices') }}
),

moving_avg AS (
    SELECT
        ticker,
        processed_timestamp,
        AVG(last_price) OVER (PARTITION BY ticker ORDER BY processed_timestamp ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS moving_avg_30d
    FROM cte
)

SELECT *
FROM moving_avg
