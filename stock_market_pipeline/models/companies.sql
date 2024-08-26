{{ config(
    materialized='incremental',
    unique_key='ticker'
) }}

WITH max_timestamp AS (
    SELECT MAX(processed_timestamp) AS last_processed
    FROM {{ source('source', 'stocks') }}
),

source_data AS (
    SELECT
        DISTINCT symbol AS ticker,
        TRIM(symbolname) AS name,
        TRIM(exchange) AS exchange,
        TRIM(sicindustry) AS industry,
        processed_timestamp
    FROM {{ source('source', 'stocks') }}
)

SELECT
    ticker,
    name,
    exchange,
    industry
FROM source_data
WHERE processed_timestamp = (SELECT last_processed FROM max_timestamp)

{% if not is_incremental() %}
    SELECT
        ticker,
        name,
        exchange,
        industry
    FROM source_data
{% endif %}
