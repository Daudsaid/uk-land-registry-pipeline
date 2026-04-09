select
    year,
    district as borough,
    property_type,
    duration,
    transactions,
    avg_price,
    median_price
from {{ ref('int_transactions_by_area') }}
where county = 'GREATER LONDON'
order by year, avg_price desc
