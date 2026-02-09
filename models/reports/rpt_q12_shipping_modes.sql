/*
TPC-H Q12: Shipping Modes and Order Priority
High vs low priority order line items by shipping mode for late deliveries.
*/
with items as (

    select * from {{ ref('fct_orders_items') }}

),
orders as (

    select * from {{ ref('fct_orders') }}

)
select
    i.ship_mode_name,
    sum(case when o.order_priority_code in ('1-URGENT', '2-HIGH') then 1 else 0 end) as high_line_count,
    sum(case when o.order_priority_code not in ('1-URGENT', '2-HIGH') then 1 else 0 end) as low_line_count
from
    items i
    join orders o on i.order_key = o.order_key
where
    i.ship_mode_name in ('MAIL', 'SHIP')
    and i.commit_date < i.receipt_date
    and i.ship_date < i.commit_date
    and i.receipt_date >= date '1994-01-01'
    and i.receipt_date < date '1995-01-01'
group by 1
order by 1
