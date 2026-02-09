-- Revenue analysis with and without discounts per order
with items as (

    select * from {{ ref('fct_orders_items') }}

)
select
    i.order_key,
    i.order_date,
    i.customer_key,
    count(*) as line_item_count,
    sum(i.quantity) as total_quantity,
    sum(i.gross_item_sales_amount) as revenue_before_discount,
    sum(i.discounted_item_sales_amount) as revenue_after_discount,
    sum(i.item_discount_amount) as total_discount_amount,
    sum(i.net_item_sales_amount) as net_revenue,
    round(abs(sum(i.item_discount_amount))
        / nullif(sum(i.gross_item_sales_amount), 0) * 100, 2) as effective_discount_pct,
    avg(i.discount_percentage) as avg_line_discount_pct,
    sum(case when i.discount_percentage > 0 then 1 else 0 end) as discounted_line_items,
    sum(case when i.discount_percentage = 0 then 1 else 0 end) as full_price_line_items
from
    items i
group by 1, 2, 3
