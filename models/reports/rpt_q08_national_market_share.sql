/*
TPC-H Q8: National Market Share
Market share of a nation for a given part type over time.
*/
with items as (

    select * from {{ ref('fct_orders_items') }}

),
customers as (

    select * from {{ ref('dim_customer') }}

),
suppliers as (

    select * from {{ ref('dim_supplier') }}

),
parts as (

    select * from {{ ref('dim_part') }}

),
all_nations as (

    select
        extract(year from i.order_date) as o_year,
        i.discounted_item_sales_amount as volume,
        s.supplier_nation_name
    from
        items i
        join parts p on i.part_key = p.part_key
        join suppliers s on i.supplier_key = s.supplier_key
        join customers c on i.customer_key = c.customer_key
    where
        c.customer_region_name = 'AMERICA'
        and p.part_type_name = 'ECONOMY ANODIZED STEEL'
        and i.order_date between date '1995-01-01' and date '1996-12-31'

)
select
    o_year,
    round(sum(case when supplier_nation_name = 'BRAZIL' then volume else 0 end)
        / nullif(sum(volume), 0) * 100, 2) as mkt_share_pct
from all_nations
group by 1
order by 1
