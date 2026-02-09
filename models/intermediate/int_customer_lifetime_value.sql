-- Cumulative revenue, order frequency, and value metrics per customer
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
    c.customer_account_balance,
    count(distinct o.order_key) as lifetime_orders,
    sum(o.gross_item_sales_amount) as lifetime_revenue,
    sum(o.net_item_sales_amount) as lifetime_net_revenue,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date,
    (max(o.order_date) - min(o.order_date)) as customer_tenure_days,
    round(sum(o.gross_item_sales_amount) / nullif(count(distinct o.order_key), 0), 2) as avg_order_value
from
    customers c
    left join orders o on c.customer_key = o.customer_key
group by 1, 2, 3, 4, 5, 6
