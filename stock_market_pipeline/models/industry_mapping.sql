SELECT
      sicindustry as industry
    , CASE
        when sicindustry ilike '%AEROSPACE%' then 'AEROSPACE'
        when sicindustry ilike '%AUTO%' then 'AUTO'
        when sicindustry ilike '%BANKS%' then 'BANKING'
        when sicindustry ilike '%BEVERAGES%' then 'BEVERAGES'
        when sicindustry ilike '%BROADCAST%' then 'BROADCAST'
        when sicindustry ilike '%COMPUTER%' then 'COMPUTER'
        when sicindustry ilike '%COSMETICS%' then 'COSMETICS'
        when sicindustry ilike '%ELECTRONICS%' then 'ELECTRONICS'
        when sicindustry ilike '%FINANCE%' then 'FINANCE'
        when sicindustry ilike '%INTERNET COMMERCE%' then 'E-COMMERCE'
        when sicindustry ilike '%INTERNET%' then 'INTERNET'
        when sicindustry ilike '%PHARMA%' then 'PHARMA'
        when sicindustry ilike '%MEDIA%' then 'MEDIA'
        when sicindustry ilike '%MEDICAL%' then 'MEDICAL'
        when sicindustry ilike '%MINING%' then 'MINING'
        when sicindustry ilike '%OIL%' then 'OIL'
        when sicindustry ilike '%ALT ENERGY%' then 'ALT ENERGY'
        when sicindustry ilike '%RETAIL%' then 'RETAIL'
        when sicindustry ilike '%SEMI GENERAL%' then 'SEMI GENERAL'
        when sicindustry ilike '%SEMI MEMORY%' then 'SEMI MEMORY'
        when sicindustry ilike '%APPAREL%' then 'APPAREL'
        when sicindustry ilike '%CLEANING PRODUCTS%' then 'CLEANING PRODUCTS'
        when sicindustry ilike '%TECHNOLOGY SERVICES%' then 'TECHNOLOGY SERVICES'
        when sicindustry ilike '%TRANSPORTATION%' then 'TRANSPORTATION'
        when sicindustry ilike '%WIRELESS EQUIPMENT%' then 'WIRELESS EQUIPMENT'
        when sicindustry ilike '%-%' then upper(split_part(sicindustry, '-',1))
        ELSE 'OTHER'
    END AS normalized_industry
FROM
    stocks