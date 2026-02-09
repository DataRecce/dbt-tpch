-- Average delivery time by shipping mode per month
with items as (

    select * from {{ ref('fct_orders_items') }}

)
select
    date_trunc('month', i.ship_date) as ship_month,
    i.ship_mode_name,
    count(*) as shipment_count,
    avg(i.receipt_date - i.ship_date) as avg_transit_days,
    avg(i.ship_date - i.order_date) as avg_processing_days,
    avg(i.receipt_date - i.order_date) as avg_total_days,
    sum(case when i.receipt_date > i.commit_date then 1 else 0 end) as late_count,
    round(sum(case when i.receipt_date > i.commit_date then 1 else 0 end)::decimal
        / nullif(count(*), 0) * 100, 2) as late_pct
from items i
where i.receipt_date is not null
group by 1, 2
