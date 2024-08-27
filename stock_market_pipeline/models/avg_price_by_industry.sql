{{ config(
    materialized='table'
) }}

SELECT
    c.normalized_industry,
    AVG(a.last_price) AS avg_last_price,
    AVG(a.bid_price) AS avg_bid_price,
    AVG(a.price_change) AS avg_price_change
FROM {{ ref('stock_prices') }} a
inner join {{ ref('companies') }} b
on a.ticker = b.ticker
inner join {{ ref('industries') }} c
on b.industry = c.industry
GROUP BY normalized_industry
