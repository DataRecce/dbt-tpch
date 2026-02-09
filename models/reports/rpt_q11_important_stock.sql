/*
TPC-H Q11: Important Stock Identification
Parts whose total supplier stock value exceeds a fraction of the nation total.
*/
with xrf as (

    select * from {{ ref('dim_part_supplier_xrf') }}

),
nation_total as (

    select
        sum(supplier_availabe_quantity * supplier_cost_amount) * 0.0001 as threshold
    from xrf
    where supplier_nation_name = 'GERMANY'

)
select
    xrf.part_key,
    sum(xrf.supplier_availabe_quantity * xrf.supplier_cost_amount) as stock_value
from
    xrf
cross join nation_total nt
where
    xrf.supplier_nation_name = 'GERMANY'
group by 1
having sum(xrf.supplier_availabe_quantity * xrf.supplier_cost_amount) > (select threshold from nation_total)
order by stock_value desc
