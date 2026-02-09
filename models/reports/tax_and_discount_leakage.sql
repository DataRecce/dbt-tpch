-- Revenue waterfall by nation: gross → discounts → tax → net
with items as (

    select * from {{ ref('fct_orders_items') }}

),
customers as (

    select * from {{ ref('dim_customer') }}

)
select
    c.customer_region_name as region_name,
    c.customer_nation_name as nation_name,
    count(*) as line_item_count,
    sum(i.gross_item_sales_amount) as gross_revenue,
    sum(abs(i.item_discount_amount)) as total_discounts,
    sum(i.item_tax_amount) as total_tax,
    sum(i.net_item_sales_amount) as net_revenue,
    round(sum(abs(i.item_discount_amount)) / nullif(sum(i.gross_item_sales_amount), 0) * 100, 2) as discount_rate_pct,
    round(sum(i.item_tax_amount) / nullif(sum(i.gross_item_sales_amount), 0) * 100, 2) as effective_tax_rate_pct,
    round(sum(i.net_item_sales_amount) / nullif(sum(i.gross_item_sales_amount), 0) * 100, 2) as net_retention_pct
from
    items i
    join customers c on i.customer_key = c.customer_key
group by 1, 2
