/*
TPC-H Q3: Shipping Priority
Top unshipped orders by revenue for a given market segment, ordered before a date.
*/
with orders as (

    select * from {{ ref('fct_orders') }}

),
items as (

    select * from {{ ref('fct_orders_items') }}

),
customers as (

    select * from {{ ref('dim_customer') }}

)
select
    o.order_key,
    sum(i.discounted_item_sales_amount) as revenue,
    o.order_date,
    o.shipping_priority
from
    orders o
    join items i on o.order_key = i.order_key
    join customers c on o.customer_key = c.customer_key
where
    c.customer_market_segment_name = 'BUILDING'
    and o.order_date < date '1995-03-15'
    and i.ship_date > date '1995-03-15'
group by 1, 3, 4
order by
    revenue desc,
    o.order_date
limit 10
