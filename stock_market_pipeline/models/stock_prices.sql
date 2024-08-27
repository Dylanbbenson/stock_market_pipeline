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
        REGEXP_REPLACE(bidprice, '[^0-9-]', '') AS bid_price,
        REGEXP_REPLACE(lastprice, '[^0-9-]', '') AS last_price,
        REGEXP_REPLACE(pricechange, '[^0-9-]', '') AS price_change,
        round(percentchange, 4) AS percentchange,
        REGEXP_REPLACE(askprice, '[^0-9-]', '') AS askprice,
        REGEXP_REPLACE(bidsize, '[^0-9-]', '')AS bidsize,
        REGEXP_REPLACE(asksize, '[^0-9-]', '') AS asksize,
        REGEXP_REPLACE(lastpriceext, '[^0-9-]', '') AS last_price_ext,
        REGEXP_REPLACE(pricechangeext, '[^0-9-]', '') AS price_change_ext,
        REGEXP_REPLACE(lowprice, '[^0-9-]', '') AS low_price,
        REGEXP_REPLACE(openprice, '[^0-9-]', '') AS open_price,
        REGEXP_REPLACE(highprice, '[^0-9-]', '') AS high_price,
        REGEXP_REPLACE(previousprice, '[^0-9-]', '') AS prev_price,
        REGEXP_REPLACE(volume, '[^0-9-]', '') as volume,
        REGEXP_REPLACE(averagevolume, '[^0-9-]', '')  AS avg_volume,
        REGEXP_REPLACE(weightedalpha, '[^0-9-]', '')  AS weighted_alpha,
        REGEXP_REPLACE(lowprice1y, '[^0-9-]', '')  AS low_price_1y,
        REGEXP_REPLACE(highprice1y, '[^0-9-]', '')  AS high_price_1y,
        processed_timestamp
    FROM {{ source('source', 'stocks') }}
)

{% if is_incremental() %}
    SELECT *
    FROM stock_prices
    WHERE processed_timestamp = (SELECT last_processed FROM max_timestamp)
{% else %}
    SELECT *
    FROM stock_prices
{% endif %}
