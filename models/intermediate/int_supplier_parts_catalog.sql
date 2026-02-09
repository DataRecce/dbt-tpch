-- Supplier inventory with cost analysis and part details
with xrf as (

    select * from {{ ref('dim_part_supplier_xrf') }}

)
select
    xrf.supplier_key,
    xrf.supplier_name,
    xrf.supplier_nation_name,
    xrf.supplier_region_name,
    xrf.part_key,
    xrf.part_name,
    xrf.part_type_name,
    xrf.part_brand_name,
    xrf.part_size,
    xrf.supplier_availabe_quantity,
    xrf.supplier_cost_amount,
    xrf.retail_price,
    (xrf.retail_price - xrf.supplier_cost_amount) as unit_margin,
    round((xrf.retail_price - xrf.supplier_cost_amount)
        / nullif(xrf.retail_price, 0) * 100, 2) as margin_pct,
    (xrf.supplier_availabe_quantity * xrf.supplier_cost_amount) as inventory_value
from
    xrf
