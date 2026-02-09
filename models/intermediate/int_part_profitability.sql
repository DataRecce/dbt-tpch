-- Revenue vs cost analysis per part
with items as (

    select * from {{ ref('fct_orders_items') }}

),
parts as (

    select * from {{ ref('dim_part') }}

)
select
    p.part_key,
    p.part_name,
    p.part_type_name,
    p.part_brand_name,
    p.part_manufacturer_name,
    p.part_size,
    p.part_container_desc,
    p.retail_price,
    count(*) as total_line_items,
    sum(i.quantity) as total_quantity_sold,
    sum(i.gross_item_sales_amount) as total_revenue,
    sum(i.net_item_sales_amount) as total_net_revenue,
    sum(i.supplier_cost_amount * i.quantity) as total_cost,
    sum(i.gross_item_sales_amount) - sum(i.supplier_cost_amount * i.quantity) as total_profit,
    round((sum(i.gross_item_sales_amount) - sum(i.supplier_cost_amount * i.quantity))
        / nullif(sum(i.gross_item_sales_amount), 0) * 100, 2) as profit_margin_pct,
    avg(i.discount_percentage) as avg_discount_pct
from
    items i
    join parts p on i.part_key = p.part_key
group by 1, 2, 3, 4, 5, 6, 7, 8
