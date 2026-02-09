-- Analyzes order behavior across discount bands
with items as (

    select
        *,
        case
            when discount_percentage = 0 then '0_no_discount'
            when discount_percentage <= 0.03 then '1_low_1_3pct'
            when discount_percentage <= 0.06 then '2_medium_4_6pct'
            when discount_percentage <= 0.08 then '3_high_7_8pct'
            else '4_deep_9pct_plus'
        end as discount_band
    from {{ ref('fct_orders_items') }}

)
select
    discount_band,
    count(*) as line_item_count,
    round(count(*)::decimal / sum(count(*)) over () * 100, 2) as pct_of_items,
    sum(quantity) as total_quantity,
    sum(gross_item_sales_amount) as gross_revenue,
    sum(discounted_item_sales_amount) as discounted_revenue,
    sum(abs(item_discount_amount)) as total_discount_given,
    round(avg(quantity), 2) as avg_quantity_per_item,
    round(avg(gross_item_sales_amount), 2) as avg_item_revenue,
    count(distinct order_key) as distinct_orders,
    count(distinct customer_key) as distinct_customers
from items
group by 1
