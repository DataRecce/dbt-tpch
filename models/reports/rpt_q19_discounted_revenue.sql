/*
TPC-H Q19: Discounted Revenue
Revenue for specific part/container/quantity combinations with discounts.
*/
with items as (

    select * from {{ ref('fct_orders_items') }}

),
parts as (

    select * from {{ ref('dim_part') }}

)
select
    sum(i.discounted_item_sales_amount) as revenue
from
    items i
    join parts p on i.part_key = p.part_key
where
    (
        p.part_brand_name = 'Brand#12'
        and p.part_container_desc in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and i.quantity >= 1 and i.quantity <= 11
        and p.part_size between 1 and 5
        and i.ship_mode_name in ('AIR', 'AIR REG')
    )
    or (
        p.part_brand_name = 'Brand#23'
        and p.part_container_desc in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and i.quantity >= 10 and i.quantity <= 20
        and p.part_size between 1 and 10
        and i.ship_mode_name in ('AIR', 'AIR REG')
    )
    or (
        p.part_brand_name = 'Brand#34'
        and p.part_container_desc in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and i.quantity >= 20 and i.quantity <= 30
        and p.part_size between 1 and 15
        and i.ship_mode_name in ('AIR', 'AIR REG')
    )
