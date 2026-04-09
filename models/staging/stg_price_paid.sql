select
    transaction_id,
    price_paid,
    transfer_date,
    postcode,
    property_type,
    old_new,
    duration,
    street,
    town,
    district,
    county,
    year(transfer_date) as year,
    month(transfer_date) as month
from {{ source('raw', 'price_paid') }}
where record_status = 'A'
