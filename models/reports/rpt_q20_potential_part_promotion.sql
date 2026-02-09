/*
TPC-H Q20: Potential Part Promotion
Suppliers in a nation with excess stock of parts matching a pattern.
*/
with parts_sold as (

    select
        i.part_key,
        i.supplier_key,
        0.5 * sum(i.quantity) as half_qty_sold
    from {{ ref('fct_orders_items') }} i
    where
        i.ship_date >= date '1994-01-01'
        and i.ship_date < date '1995-01-01'
    group by 1, 2

),
xrf as (

    select * from {{ ref('dim_part_supplier_xrf') }}

)
select
    xrf.supplier_name,
    xrf.supplier_address
from
    xrf
    join parts_sold ps
        on xrf.part_key = ps.part_key
        and xrf.supplier_key = ps.supplier_key
where
    xrf.part_name like 'forest%'
    and xrf.supplier_nation_name = 'CANADA'
    and xrf.supplier_availabe_quantity > ps.half_qty_sold
order by xrf.supplier_name
