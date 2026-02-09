-- Daily revenue time series
select
    o.order_date,
    count(distinct o.order_key) as order_count,
    sum(o.gross_item_sales_amount) as gross_revenue,
    sum(o.net_item_sales_amount) as net_revenue,
    sum(o.item_discount_amount) as total_discounts,
    sum(o.item_tax_amount) as total_tax
from
    {{ ref('fct_orders') }} o
group by 1
order by 1
