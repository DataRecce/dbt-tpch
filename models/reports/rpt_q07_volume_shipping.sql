/*
TPC-H Q7: Volume Shipping
Bilateral trade value between two nations by year.
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
    s.supplier_nation_name as supp_nation,
    c.customer_nation_name as cust_nation,
    extract(year from i.ship_date) as l_year,
    sum(i.discounted_item_sales_amount) as revenue
from
    items i
    join suppliers s on i.supplier_key = s.supplier_key
    join customers c on i.customer_key = c.customer_key
where
    i.ship_date between date '1995-01-01' and date '1996-12-31'
    and (
        (s.supplier_nation_name = 'FRANCE' and c.customer_nation_name = 'GERMANY')
        or (s.supplier_nation_name = 'GERMANY' and c.customer_nation_name = 'FRANCE')
    )
group by 1, 2, 3
order by 1, 2, 3
