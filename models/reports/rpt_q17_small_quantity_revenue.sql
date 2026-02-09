/*
TPC-H Q17: Small-Quantity-Order Revenue
Average yearly revenue from orders for parts where quantity is below 20% of average.
*/
with items as (

    select * from {{ ref('fct_orders_items') }}

),
parts as (

    select * from {{ ref('dim_part') }}

),
avg_qty as (

    select
        part_key,
        0.2 * avg(quantity) as avg_qty_threshold
    from items
    group by 1

)
select
    round(sum(i.gross_item_sales_amount) / 7.0, 2) as avg_yearly_revenue
from
    items i
    join parts p on i.part_key = p.part_key
    join avg_qty aq on i.part_key = aq.part_key
where
    p.part_brand_name = 'Brand#23'
    and p.part_container_desc = 'MED BOX'
    and i.quantity < aq.avg_qty_threshold
