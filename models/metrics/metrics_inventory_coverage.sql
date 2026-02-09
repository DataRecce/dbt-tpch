-- Supply vs demand ratio by part
with demand as (

    select
        part_key,
        sum(quantity) as total_demand,
        count(distinct order_key) as order_count
    from {{ ref('fct_orders_items') }}
    group by 1

),
supply as (

    select
        part_key,
        sum(supplier_availabe_quantity) as total_supply,
        count(distinct supplier_key) as supplier_count,
        avg(supplier_cost_amount) as avg_cost
    from {{ ref('dim_part_supplier_xrf') }}
    group by 1

),
parts as (

    select * from {{ ref('dim_part') }}

)
select
    p.part_key,
    p.part_name,
    p.part_type_name,
    p.part_brand_name,
    coalesce(s.total_supply, 0) as total_supply,
    coalesce(d.total_demand, 0) as total_demand,
    coalesce(s.supplier_count, 0) as supplier_count,
    coalesce(d.order_count, 0) as order_count,
    s.avg_cost,
    round(coalesce(s.total_supply, 0)::decimal
        / nullif(coalesce(d.total_demand, 0), 0), 2) as supply_demand_ratio
from
    parts p
    left join supply s on p.part_key = s.part_key
    left join demand d on p.part_key = d.part_key
