-- Nation-to-nation and region-to-region shipping volumes and revenue
with items as (

    select * from {{ ref('fct_orders_items') }}

),
customers as (

    select * from {{ ref('dim_customer') }}

),
suppliers as (

    select * from {{ ref('dim_supplier') }}

)
select
    s.supplier_nation_name as supplier_nation,
    s.supplier_region_name as supplier_region,
    c.customer_nation_name as customer_nation,
    c.customer_region_name as customer_region,
    extract(year from i.ship_date) as ship_year,
    count(*) as total_shipments,
    sum(i.quantity) as total_quantity,
    sum(i.gross_item_sales_amount) as total_revenue,
    sum(i.net_item_sales_amount) as total_net_revenue,
    case
        when s.supplier_nation_name = c.customer_nation_name then 'domestic'
        when s.supplier_region_name = c.customer_region_name then 'intra_regional'
        else 'inter_regional'
    end as trade_type
from
    items i
    join customers c on i.customer_key = c.customer_key
    join suppliers s on i.supplier_key = s.supplier_key
group by 1, 2, 3, 4, 5, 10
