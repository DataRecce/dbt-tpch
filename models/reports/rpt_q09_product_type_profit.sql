/*
TPC-H Q9: Product Type Profit Measure
Profit by supplier nation and year for parts containing a specific string.
*/
with items as (

    select * from {{ ref('fct_orders_items') }}

),
suppliers as (

    select * from {{ ref('dim_supplier') }}

),
parts as (

    select * from {{ ref('dim_part') }}

)
select
    s.supplier_nation_name as nation,
    extract(year from i.order_date) as o_year,
    sum(i.discounted_item_sales_amount - i.supplier_cost_amount * i.quantity) as sum_profit
from
    items i
    join parts p on i.part_key = p.part_key
    join suppliers s on i.supplier_key = s.supplier_key
where
    p.part_name like '%green%'
group by 1, 2
order by 1, 2 desc
