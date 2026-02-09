-- Identifies single-source vs multi-source parts and supplier market share per part
with part_suppliers as (

    select
        part_key,
        supplier_key,
        supplier_availabe_quantity,
        supplier_cost_amount
    from {{ ref('dim_part_supplier_xrf') }}

),
part_supplier_count as (

    select
        part_key,
        count(distinct supplier_key) as supplier_count,
        sum(supplier_availabe_quantity) as total_available_qty
    from part_suppliers
    group by 1

),
supplier_share as (

    select
        ps.part_key,
        ps.supplier_key,
        ps.supplier_availabe_quantity,
        psc.supplier_count,
        psc.total_available_qty,
        round(ps.supplier_availabe_quantity::decimal
            / nullif(psc.total_available_qty, 0) * 100, 2) as supply_share_pct
    from part_suppliers ps
    join part_supplier_count psc on ps.part_key = psc.part_key

)
select
    ss.part_key,
    p.part_name,
    p.part_type_name,
    ss.supplier_key,
    s.supplier_name,
    s.supplier_nation_name,
    ss.supplier_count as total_suppliers_for_part,
    ss.supplier_availabe_quantity,
    ss.total_available_qty as part_total_supply,
    ss.supply_share_pct,
    case
        when ss.supplier_count = 1 then 'single_source'
        when ss.supply_share_pct > 80 then 'dominant_supplier'
        when ss.supplier_count <= 3 then 'limited_sources'
        else 'diversified'
    end as concentration_risk
from supplier_share ss
join {{ ref('dim_part') }} p on ss.part_key = p.part_key
join {{ ref('dim_supplier') }} s on ss.supplier_key = s.supplier_key
