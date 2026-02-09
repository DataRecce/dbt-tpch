-- Supplier ranking by revenue within nation and globally
with supplier_revenue as (

    select
        i.supplier_key,
        s.supplier_name,
        s.supplier_nation_name,
        s.supplier_region_name,
        sum(i.gross_item_sales_amount) as total_revenue,
        count(distinct i.order_key) as order_count,
        count(distinct i.part_key) as parts_sold
    from {{ ref('fct_orders_items') }} i
    join {{ ref('dim_supplier') }} s on i.supplier_key = s.supplier_key
    group by 1, 2, 3, 4

),
nation_totals as (

    select
        supplier_nation_name,
        sum(total_revenue) as nation_total_revenue
    from supplier_revenue
    group by 1

)
select
    sr.supplier_key,
    sr.supplier_name,
    sr.supplier_nation_name,
    sr.supplier_region_name,
    sr.total_revenue,
    sr.order_count,
    sr.parts_sold,
    rank() over (order by sr.total_revenue desc) as global_rank,
    rank() over (partition by sr.supplier_nation_name order by sr.total_revenue desc) as nation_rank,
    nt.nation_total_revenue,
    round(sr.total_revenue / nt.nation_total_revenue * 100, 2) as nation_market_share_pct,
    round(sr.total_revenue / sum(sr.total_revenue) over () * 100, 4) as global_market_share_pct
from supplier_revenue sr
join nation_totals nt on sr.supplier_nation_name = nt.supplier_nation_name
