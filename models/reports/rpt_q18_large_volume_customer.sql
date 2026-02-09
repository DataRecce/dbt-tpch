/*
TPC-H Q18: Large Volume Customer
Customers who have placed orders with total quantity exceeding a threshold.
*/
with large_orders as (

    select
        order_key,
        sum(quantity) as total_quantity
    from {{ ref('fct_orders_items') }}
    group by 1
    having sum(quantity) > 300

),
orders as (

    select * from {{ ref('fct_orders') }}

),
customers as (

    select * from {{ ref('dim_customer') }}

)
select
    c.customer_name,
    c.customer_key,
    o.order_key,
    o.order_date,
    o.gross_item_sales_amount as total_price,
    lo.total_quantity
from
    large_orders lo
    join orders o on lo.order_key = o.order_key
    join customers c on o.customer_key = c.customer_key
order by
    total_price desc,
    o.order_date
limit 100
