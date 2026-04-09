select
    year,
    property_type,
    old_new,
    duration,
    count(*) as transactions,
    round(avg(price_paid)) as avg_price
from {{ ref('stg_price_paid') }}
group by year, property_type, old_new, duration
order by year, transactions desc
