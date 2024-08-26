select
  distinct symbol as ticker
, trim(symbolname) as name
, trim(exchange) as exchange
, trim(sicindustry) as industry
from stocks
order by symbol