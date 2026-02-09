/*
TPC-H Q5: Local Supplier Volume
Revenue through local suppliers by nation in a given region.
*/
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
    c.customer_nation_name as nation_name,
    sum(i.discounted_item_sales_amount) as revenue
from
    items i
    join customers c on i.customer_key = c.customer_key
    join suppliers s on i.supplier_key = s.supplier_key
where
    c.customer_region_name = 'ASIA'
    and c.customer_nation_name = s.supplier_nation_name
    and i.order_date >= date '1994-01-01'
    and i.order_date < date '1995-01-01'
group by 1
order by revenue desc
