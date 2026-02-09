-- Monthly order volume and value
select
    date_trunc('month', o.order_date) as order_month,
    count(distinct o.order_key) as order_count,
    sum(o.gross_item_sales_amount) as gross_revenue,
    sum(o.net_item_sales_amount) as net_revenue,
    avg(o.gross_item_sales_amount) as avg_order_value,
    sum(o.item_discount_amount) as total_discounts,
    count(distinct o.customer_key) as unique_customers
from
    {{ ref('fct_orders') }} o
group by 1
order by 1
