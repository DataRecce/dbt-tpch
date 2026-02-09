-- Distribution of orders by status with revenue breakdown
with orders as (

    select * from {{ ref('fct_orders') }}

)
select
    order_status_code,
    count(distinct order_key) as order_count,
    round(count(distinct order_key)::decimal
        / sum(count(distinct order_key)) over () * 100, 2) as pct_of_orders,
    sum(gross_item_sales_amount) as total_revenue,
    round(sum(gross_item_sales_amount)
        / sum(sum(gross_item_sales_amount)) over () * 100, 2) as pct_of_revenue,
    round(avg(gross_item_sales_amount), 2) as avg_order_value,
    count(distinct customer_key) as unique_customers
from orders
group by 1
