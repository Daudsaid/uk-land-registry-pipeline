select
    year,
    county,
    district,
    property_type,
    duration,
    old_new,
    count(*) as transactions,
    round(avg(price_paid)) as avg_price,
    round(median(price_paid)) as median_price,
    min(price_paid) as min_price,
    max(price_paid) as max_price
from {{ ref('stg_price_paid') }}
group by year, county, district, property_type, duration, old_new
