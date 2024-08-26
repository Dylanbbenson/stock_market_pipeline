{{ config(
    materialized='incremental',
    unique_key=['ticker', 'processed_timestamp']
) }}

WITH max_timestamp AS (
    SELECT MAX(processed_timestamp) AS last_processed
    FROM {{ source('source', 'stocks') }}
),

stock_prices AS (
    SELECT
        symbol AS ticker,
        replace(bidprice, 'N/A') AS bid_price,
        lastprice AS last_price,
        replace(pricechange, '+') AS price_change,
        round(percentchange, 4) AS percentchange,
        replace(askprice, 'N/A') AS askprice,
        replace(bidsize, 'N/A') AS bidsize,
        replace(asksize, 'N/A') AS asksize,
        replace(lastpriceext, '+') AS last_price_ext,
        pricechangeext AS price_change_ext,
        lowprice AS low_price,
        openprice AS open_price,
        highprice AS high_price,
        previousprice AS prev_price,
        volume,
        averagevolume AS avg_volume,
        weightedalpha AS weighted_alpha,
        lowprice1y AS low_price_1y,
        highprice1y AS high_price_1y,
        processed_timestamp
    FROM {{ source('source', 'stocks') }}
)

{% if is_incremental() %}
    SELECT *
    FROM stock_prices
    WHERE processed_timestamp > (SELECT last_processed FROM max_timestamp)
{% else %}
    SELECT *
    FROM stock_prices
{% endif %}
