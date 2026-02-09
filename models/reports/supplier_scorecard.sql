-- Comprehensive supplier evaluation scorecard
with delivery as (

    select
        supplier_key,
        count(*) as total_shipments,
        sum(case when receipt_date <= commit_date then 1 else 0 end) as on_time_shipments,
        sum(case when return_status_code = 'R' then 1 else 0 end) as returned_items,
        avg(receipt_date - ship_date) as avg_transit_days,
        avg(receipt_date - commit_date) as avg_delay_days
    from {{ ref('fct_orders_items') }}
    where receipt_date is not null
    group by 1

),
revenue as (

    select
        supplier_key,
        sum(gross_item_sales_amount) as total_revenue,
        sum(net_item_sales_amount) as total_net_revenue,
        count(distinct order_key) as order_count,
        count(distinct customer_key) as customer_reach,
        count(distinct part_key) as parts_supplied
    from {{ ref('fct_orders_items') }}
    group by 1

),
inventory as (

    select
        supplier_key,
        sum(supplier_availabe_quantity) as total_available_qty,
        avg(supplier_cost_amount) as avg_supply_cost
    from {{ ref('dim_part_supplier_xrf') }}
    group by 1

)
select
    s.supplier_key,
    s.supplier_name,
    s.supplier_nation_name,
    s.supplier_region_name,
    s.supplier_account_balance,
    r.total_revenue,
    r.total_net_revenue,
    r.order_count,
    r.customer_reach,
    r.parts_supplied,
    d.total_shipments,
    d.on_time_shipments,
    round(d.on_time_shipments::decimal / nullif(d.total_shipments, 0) * 100, 2) as on_time_pct,
    d.returned_items,
    round(d.returned_items::decimal / nullif(d.total_shipments, 0) * 100, 2) as return_rate_pct,
    round(d.avg_transit_days, 1) as avg_transit_days,
    round(d.avg_delay_days, 1) as avg_delay_days,
    coalesce(i.total_available_qty, 0) as total_available_qty,
    round(i.avg_supply_cost, 2) as avg_supply_cost
from {{ ref('dim_supplier') }} s
left join revenue r on s.supplier_key = r.supplier_key
left join delivery d on s.supplier_key = d.supplier_key
left join inventory i on s.supplier_key = i.supplier_key
