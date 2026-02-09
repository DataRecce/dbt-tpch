-- Delivery and revenue metrics per supplier
with items as (

    select * from {{ ref('fct_orders_items') }}

),
suppliers as (

    select * from {{ ref('dim_supplier') }}

)
select
    s.supplier_key,
    s.supplier_name,
    s.supplier_nation_name,
    s.supplier_region_name,
    count(*) as total_line_items,
    count(distinct i.order_key) as total_orders,
    sum(i.quantity) as total_quantity,
    sum(i.gross_item_sales_amount) as total_revenue,
    sum(i.net_item_sales_amount) as total_net_revenue,
    sum(case when i.receipt_date > i.commit_date then 1 else 0 end) as late_deliveries,
    sum(case when i.receipt_date <= i.commit_date then 1 else 0 end) as on_time_deliveries,
    round(sum(case when i.receipt_date <= i.commit_date then 1 else 0 end)::decimal
        / nullif(count(*), 0) * 100, 2) as on_time_pct,
    avg(i.receipt_date - i.ship_date) as avg_delivery_days,
    sum(case when i.return_status_code = 'R' then 1 else 0 end) as returned_items,
    round(sum(case when i.return_status_code = 'R' then 1 else 0 end)::decimal
        / nullif(count(*), 0) * 100, 2) as return_rate_pct
from
    items i
    join suppliers s on i.supplier_key = s.supplier_key
group by 1, 2, 3, 4
