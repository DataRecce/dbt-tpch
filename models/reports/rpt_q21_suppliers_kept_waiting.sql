/*
TPC-H Q21: Suppliers Who Kept Orders Waiting
Suppliers in a nation whose line items were received after commit date,
where no other supplier for the same order also failed.
*/
with items as (

    select * from {{ ref('fct_orders_items') }}

),
suppliers as (

    select * from {{ ref('dim_supplier') }}

),
orders as (

    select * from {{ ref('fct_orders') }}

),
late_supplier_items as (

    select
        i.supplier_key,
        i.order_key
    from items i
    where i.receipt_date > i.commit_date

),
multi_supplier_orders as (

    select distinct i.order_key
    from items i
    group by i.order_key
    having count(distinct i.supplier_key) > 1

)
select
    s.supplier_name,
    count(*) as numwait
from
    late_supplier_items lsi
    join suppliers s on lsi.supplier_key = s.supplier_key
    join orders o on lsi.order_key = o.order_key
where
    s.supplier_nation_name = 'SAUDI ARABIA'
    and o.order_status_code = 'F'
    and lsi.order_key in (select order_key from multi_supplier_orders)
group by 1
order by numwait desc, 1
limit 100
