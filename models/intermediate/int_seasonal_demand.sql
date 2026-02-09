-- Order volume and revenue by month and quarter
with orders as (

    select * from {{ ref('fct_orders') }}

)
select
    extract(year from o.order_date) as order_year,
    extract(quarter from o.order_date) as order_quarter,
    extract(month from o.order_date) as order_month,
    count(distinct o.order_key) as order_count,
    sum(o.gross_item_sales_amount) as total_revenue,
    sum(o.net_item_sales_amount) as total_net_revenue,
    avg(o.gross_item_sales_amount) as avg_order_value,
    sum(o.item_discount_amount) as total_discounts
from
    orders o
group by 1, 2, 3
