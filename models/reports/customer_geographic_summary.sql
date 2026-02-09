-- Customer metrics aggregated by nation and region for geographic dashboards
with orders as (

    select * from {{ ref('fct_orders') }}

),
customers as (

    select * from {{ ref('dim_customer') }}

)
select
    c.customer_region_name as region_name,
    c.customer_nation_name as nation_name,
    count(distinct c.customer_key) as customer_count,
    count(distinct o.order_key) as order_count,
    sum(o.gross_item_sales_amount) as total_revenue,
    sum(o.net_item_sales_amount) as total_net_revenue,
    round(avg(o.gross_item_sales_amount), 2) as avg_order_value,
    round(sum(o.gross_item_sales_amount) / nullif(count(distinct c.customer_key), 0), 2) as revenue_per_customer,
    round(count(distinct o.order_key)::decimal / nullif(count(distinct c.customer_key), 0), 2) as orders_per_customer,
    sum(c.customer_account_balance) as total_account_balance
from
    customers c
    left join orders o on c.customer_key = o.customer_key
group by 1, 2
