select
    column00 as transaction_id,
    CAST(column01 AS INTEGER) as price_paid,
    CAST(column02 AS DATE) as transfer_date,
    column03 as postcode,
    case column04
        when 'D' then 'Detached'
        when 'S' then 'Semi-Detached'
        when 'T' then 'Terraced'
        when 'F' then 'Flat'
        else 'Other'
    end as property_type,
    case column05
        when 'Y' then 'New Build'
        when 'N' then 'Established'
    end as old_new,
    case column06
        when 'F' then 'Freehold'
        when 'L' then 'Leasehold'
    end as duration,
    column09 as street,
    column11 as town,
    column12 as district,
    column13 as county,
    year(CAST(column02 AS DATE)) as year,
    month(CAST(column02 AS DATE)) as month
from read_csv_auto('/Users/daudsaid/pp-20*.csv', header=false)
where column15 = 'A'
