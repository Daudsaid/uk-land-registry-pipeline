select
    year,
    count(*) as total_transactions,
    round(avg(price_paid)) as avg_price,
    round(median(price_paid)) as median_price
from {{ ref('stg_price_paid') }}
group by year
order by year
