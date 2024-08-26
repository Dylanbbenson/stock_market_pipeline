select
  symbol as ticker
, trim(symbolname) as name
, trim(exchange) as exchange
, trim(sicindustry) as industry
, replace(bidprice, 'N/A') as bid_price
, lastprice as last_price
, replace(pricechange, '+') as price_change
, round(percentchange, 4) as percentchange
, replace(askprice, 'N/A') as askprice
, replace(bidsize, 'N/A') as bidsize
, replace(asksize, 'N/A') as asksize
, replace(lastpriceext, '+') as last_price_ext
, pricechangeext as price_change_ext
, lowprice as low_price
, openprice as open_price
, highprice as high_price
, previousprice as prev_price
, volume
, averagevolume as avg_volume
, weightedalpha as weighted_alpha
, lowprice1y as low_price_1y
, highprice1y as high_price_1y
, processed_timestamp
from stocks
order by symbol, processed_timestamp desc