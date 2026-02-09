/*
TPC-H Q14: Promotion Effect
Percentage of revenue from promotional parts in a given month.
*/
with items as (

    select * from {{ ref('fct_orders_items') }}

),
parts as (

    select * from {{ ref('dim_part') }}

)
select
    round(100.0 * sum(case when p.part_type_name like 'PROMO%'
        then i.discounted_item_sales_amount else 0 end)
        / nullif(sum(i.discounted_item_sales_amount), 0), 2) as promo_revenue_pct
from
    items i
    join parts p on i.part_key = p.part_key
where
    i.ship_date >= date '1995-09-01'
    and i.ship_date < date '1995-10-01'
