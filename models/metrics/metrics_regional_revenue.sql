-- Revenue by region and nation over time
with orders as (

    select * from {{ ref('fct_orders') }}

),
customers as (

    select * from {{ ref('dim_customer') }}

)
select
    date_trunc('month', o.order_date) as order_month,
    c.customer_region_name as region_name,
    c.customer_nation_name as nation_name,
    count(distinct o.order_key) as order_count,
    count(distinct o.customer_key) as customer_count,
    sum(o.gross_item_sales_amount) as gross_revenue,
    sum(o.net_item_sales_amount) as net_revenue
from
    orders o
    join customers c on o.customer_key = c.customer_key
group by 1, 2, 3
