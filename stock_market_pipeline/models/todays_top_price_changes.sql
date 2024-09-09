{{ config(
    materialized='table'
) }}

SELECT
    ticker,
    cast(to_timestamp(processed_timestamp) as date) as day,
    avg(price_change) as average_price_change,
    avg(percentchange) as average_percent_change,
FROM {{ ref('stock_prices') }}
where cast(to_timestamp(processed_timestamp) as date) = current_date
GROUP BY ticker, day
ORDER BY 2 DESC