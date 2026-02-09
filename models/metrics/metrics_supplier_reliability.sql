-- On-time delivery rate by supplier per month
with items as (

    select * from {{ ref('fct_orders_items') }}

),
suppliers as (

    select * from {{ ref('dim_supplier') }}

)
select
    date_trunc('month', i.ship_date) as ship_month,
    s.supplier_key,
    s.supplier_name,
    s.supplier_nation_name,
    count(*) as total_shipments,
    sum(case when i.receipt_date <= i.commit_date then 1 else 0 end) as on_time_count,
    sum(case when i.receipt_date > i.commit_date then 1 else 0 end) as late_count,
    round(sum(case when i.receipt_date <= i.commit_date then 1 else 0 end)::decimal
        / nullif(count(*), 0) * 100, 2) as on_time_pct
from
    items i
    join suppliers s on i.supplier_key = s.supplier_key
where i.receipt_date is not null
group by 1, 2, 3, 4
