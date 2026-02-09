-- Order count, value, and status breakdown per customer
with orders as (

    select * from {{ ref('fct_orders') }}

),
customers as (

    select * from {{ ref('dim_customer') }}

)
select
    c.customer_key,
    c.customer_name,
    c.customer_nation_name,
    c.customer_region_name,
    c.customer_market_segment_name,
    count(distinct o.order_key) as order_count,
    sum(o.gross_item_sales_amount) as total_revenue,
    sum(o.net_item_sales_amount) as total_net_revenue,
    avg(o.gross_item_sales_amount) as avg_order_value,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date,
    count(distinct case when o.order_status_code = 'F' then o.order_key end) as fulfilled_orders,
    count(distinct case when o.order_status_code = 'O' then o.order_key end) as open_orders,
    count(distinct case when o.order_status_code = 'P' then o.order_key end) as partial_orders
from
    customers c
    left join orders o on c.customer_key = o.customer_key
group by 1, 2, 3, 4, 5
