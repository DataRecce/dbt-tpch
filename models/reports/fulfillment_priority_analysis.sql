-- Fulfillment priority: order urgency segmentation and delivery performance
with items as (

    select
        i.order_key,
        i.order_date,
        i.ship_date,
        o.order_priority_code,
        i.ship_mode_name,
        i.customer_key,
        i.quantity,
        i.gross_item_sales_amount,
        i.ship_date - i.order_date as fulfillment_days
    from {{ ref('fct_orders_items') }} i
    join {{ ref('fct_orders') }} o on i.order_key = o.order_key

)
select
    order_priority_code,
    ship_mode_name,
    count(*) as line_item_count,
    count(distinct order_key) as order_count,
    count(distinct customer_key) as customer_count,
    sum(gross_item_sales_amount) as total_revenue,
    round(avg(fulfillment_days), 1) as avg_fulfillment_days,
    min(fulfillment_days) as min_fulfillment_days,
    max(fulfillment_days) as max_fulfillment_days,
    round(avg(quantity), 1) as avg_quantity_per_line,
    sum(case when fulfillment_days <= 7 then 1 else 0 end) as fulfilled_within_7d,
    sum(case when fulfillment_days <= 14 then 1 else 0 end) as fulfilled_within_14d,
    round(sum(case when fulfillment_days <= 7 then 1 else 0 end) * 100.0
        / count(*), 2) as pct_within_7d,
    round(sum(case when fulfillment_days <= 14 then 1 else 0 end) * 100.0
        / count(*), 2) as pct_within_14d
from items
group by 1, 2
