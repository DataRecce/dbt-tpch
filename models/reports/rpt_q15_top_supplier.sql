/*
TPC-H Q15: Top Supplier
Supplier with the highest revenue in a given quarter.
*/
with supplier_revenue as (

    select
        i.supplier_key,
        sum(i.discounted_item_sales_amount) as total_revenue
    from {{ ref('fct_orders_items') }} i
    where
        i.ship_date >= date '1996-01-01'
        and i.ship_date < date '1996-04-01'
    group by 1

),
max_revenue as (

    select max(total_revenue) as max_rev
    from supplier_revenue

)
select
    s.supplier_key,
    s.supplier_name,
    s.supplier_address,
    s.supplier_phone_number,
    sr.total_revenue
from
    supplier_revenue sr
    join {{ ref('dim_supplier') }} s on sr.supplier_key = s.supplier_key
    cross join max_revenue mr
where
    sr.total_revenue = mr.max_rev
order by s.supplier_key
