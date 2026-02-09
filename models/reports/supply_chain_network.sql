-- Supply chain network: supplier→part→customer trade edges with volume/revenue
with edges as (

    select
        i.supplier_key,
        i.part_key,
        i.customer_key,
        count(*) as shipment_count,
        sum(i.quantity) as total_quantity,
        sum(i.gross_item_sales_amount) as total_revenue
    from {{ ref('fct_orders_items') }} i
    group by 1, 2, 3

)
select
    e.supplier_key,
    s.supplier_name,
    s.supplier_nation_name as supplier_nation,
    e.part_key,
    p.part_name,
    p.part_type_name,
    e.customer_key,
    c.customer_name,
    c.customer_nation_name as customer_nation,
    e.shipment_count,
    e.total_quantity,
    e.total_revenue,
    case
        when s.supplier_nation_name = c.customer_nation_name then 'domestic'
        when s.supplier_region_name = c.customer_region_name then 'intra_regional'
        else 'inter_regional'
    end as trade_type
from edges e
join {{ ref('dim_supplier') }} s on e.supplier_key = s.supplier_key
join {{ ref('dim_part') }} p on e.part_key = p.part_key
join {{ ref('dim_customer') }} c on e.customer_key = c.customer_key
