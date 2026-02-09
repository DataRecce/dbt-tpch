/*
TPC-H Q10: Returned Item Reporting
Top customers with significant returns in a given quarter.
*/
with items as (

    select * from {{ ref('fct_orders_items') }}

),
customers as (

    select * from {{ ref('dim_customer') }}

)
select
    c.customer_key,
    c.customer_name,
    sum(i.discounted_item_sales_amount) as revenue,
    c.customer_account_balance,
    c.customer_nation_name,
    c.customer_address,
    c.customer_phone_number
from
    items i
    join customers c on i.customer_key = c.customer_key
where
    i.order_date >= date '1993-10-01'
    and i.order_date < date '1994-01-01'
    and i.return_status_code = 'R'
group by 1, 2, 4, 5, 6, 7
order by revenue desc
limit 20
