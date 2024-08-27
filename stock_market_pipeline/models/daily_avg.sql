{{ config(
    materialized='table'
) }}

SELECT
    ticker,
    cast(to_timestamp(processed_timestamp) as date) AS day,
    AVG(bid_price) AS avg_bid_price,
    AVG(last_price) AS avg_last_price,
    AVG(price_change) AS avg_price_change,
    AVG(percentchange) AS avg_percent_change,
    AVG(askprice) AS avg_ask_price,
    AVG(bidsize) AS avg_bid_size,
    AVG(asksize) AS avg_ask_size,
    AVG(last_price_ext) AS avg_last_price_ext,
    AVG(price_change_ext) AS avg_price_change_ext,
    AVG(low_price) AS avg_low_price,
    AVG(open_price) AS avg_open_price,
    AVG(high_price) AS avg_high_price,
    AVG(prev_price) AS avg_prev_price,
    AVG(volume) AS avg_volume,
    AVG(avg_volume) AS avg_avg_volume,
    AVG(weighted_alpha) AS avg_weighted_alpha,
    AVG(low_price_1y) AS avg_low_price_1y,
    AVG(high_price_1y) AS avg_high_price_1y
FROM {{ ref('stock_prices') }}
GROUP BY ticker, day
