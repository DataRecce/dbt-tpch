-- Sales performance aggregated by part type, brand, and size category
with items as (

    select * from {{ ref('fct_orders_items') }}

),
parts as (

    select * from {{ ref('dim_part') }}

)
select
    p.part_type_name,
    p.part_brand_name,
    case
        when p.part_size <= 10 then 'small'
        when p.part_size <= 30 then 'medium'
        else 'large'
    end as size_category,
    count(distinct p.part_key) as part_count,
    count(*) as line_item_count,
    count(distinct i.order_key) as order_count,
    count(distinct i.customer_key) as customer_count,
    sum(i.quantity) as total_units,
    sum(i.gross_item_sales_amount) as total_revenue,
    round(avg(i.base_price), 2) as avg_unit_price,
    round(avg(i.discount_percentage) * 100, 2) as avg_discount_pct,
    sum(i.gross_item_sales_amount - i.supplier_cost_amount * i.quantity) as total_profit
from
    items i
    join parts p on i.part_key = p.part_key
group by 1, 2, 3
