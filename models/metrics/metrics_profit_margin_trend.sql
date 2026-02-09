-- Margin trend by product type over time
with items as (

    select * from {{ ref('fct_orders_items') }}

),
parts as (

    select * from {{ ref('dim_part') }}

)
select
    date_trunc('month', i.order_date) as order_month,
    p.part_type_name,
    count(*) as line_item_count,
    sum(i.gross_item_sales_amount) as total_revenue,
    sum(i.supplier_cost_amount * i.quantity) as total_cost,
    sum(i.gross_item_sales_amount) - sum(i.supplier_cost_amount * i.quantity) as total_profit,
    round((sum(i.gross_item_sales_amount) - sum(i.supplier_cost_amount * i.quantity))
        / nullif(sum(i.gross_item_sales_amount), 0) * 100, 2) as profit_margin_pct
from
    items i
    join parts p on i.part_key = p.part_key
group by 1, 2
