-- Inventory turnover: how quickly parts move relative to available supply
with sales as (

    select
        i.part_key,
        i.supplier_key,
        sum(i.quantity) as units_sold,
        sum(i.gross_item_sales_amount) as revenue
    from {{ ref('fct_orders_items') }} i
    group by 1, 2

),
supply as (

    select
        ps.part_key,
        ps.supplier_key,
        ps.supplier_availabe_quantity as available_quantity,
        ps.supplier_cost_amount as supply_cost
    from {{ ref('parts_suppliers') }} ps

)
select
    s.part_key,
    p.part_name,
    p.part_type_name,
    s.supplier_key,
    sup.supplier_name,
    su.available_quantity,
    su.supply_cost,
    s.units_sold,
    s.revenue,
    round(s.units_sold * 1.0 / nullif(su.available_quantity, 0), 2) as turnover_ratio,
    case
        when su.available_quantity = 0 then 'out_of_stock'
        when s.units_sold * 1.0 / su.available_quantity > 2 then 'fast_mover'
        when s.units_sold * 1.0 / su.available_quantity > 0.5 then 'normal'
        when s.units_sold * 1.0 / su.available_quantity > 0 then 'slow_mover'
        else 'dead_stock'
    end as turnover_category
from sales s
join supply su on s.part_key = su.part_key and s.supplier_key = su.supplier_key
join {{ ref('dim_part') }} p on s.part_key = p.part_key
join {{ ref('dim_supplier') }} sup on s.supplier_key = sup.supplier_key
