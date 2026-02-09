/*
TPC-H Q4: Order Priority Checking
Count of orders per priority where at least one line item was received late.
*/
with orders as (

    select * from {{ ref('fct_orders') }}

),
late_items as (

    select distinct order_key
    from {{ ref('fct_orders_items') }}
    where receipt_date > commit_date

)
select
    o.order_priority_code,
    count(*) as order_count
from
    orders o
where
    o.order_date >= date '1993-07-01'
    and o.order_date < date '1993-10-01'
    and exists (
        select 1 from late_items li where li.order_key = o.order_key
    )
group by 1
order by 1
