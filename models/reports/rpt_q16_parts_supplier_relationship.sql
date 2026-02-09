/*
TPC-H Q16: Parts/Supplier Relationship
Count of distinct suppliers per part brand/type/size combination.
*/
with xrf as (

    select * from {{ ref('dim_part_supplier_xrf') }}

)
select
    xrf.part_brand_name,
    xrf.part_type_name,
    xrf.part_size,
    count(distinct xrf.supplier_key) as supplier_cnt
from
    xrf
where
    xrf.part_brand_name <> 'Brand#45'
    and xrf.part_type_name not like 'MEDIUM POLISHED%'
    and xrf.part_size in (49, 14, 23, 45, 19, 3, 36, 9)
group by 1, 2, 3
order by supplier_cnt desc, 1, 2, 3
