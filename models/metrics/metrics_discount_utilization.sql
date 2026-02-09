-- Discount usage patterns by month
with items as (

    select * from {{ ref('fct_orders_items') }}

)
select
    date_trunc('month', i.ship_date) as ship_month,
    count(*) as total_line_items,
    sum(case when i.discount_percentage > 0 then 1 else 0 end) as discounted_items,
    sum(case when i.discount_percentage = 0 then 1 else 0 end) as full_price_items,
    round(sum(case when i.discount_percentage > 0 then 1 else 0 end)::decimal
        / nullif(count(*), 0) * 100, 2) as discount_usage_pct,
    avg(case when i.discount_percentage > 0 then i.discount_percentage end) as avg_discount_when_used,
    sum(i.gross_item_sales_amount) as gross_revenue,
    sum(abs(i.item_discount_amount)) as total_discount_given,
    round(sum(abs(i.item_discount_amount))
        / nullif(sum(i.gross_item_sales_amount), 0) * 100, 2) as discount_as_pct_of_revenue
from items i
group by 1
