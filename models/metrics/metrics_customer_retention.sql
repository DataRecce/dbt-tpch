-- Repeat order rate by customer cohort (first order month)
with customer_first_order as (

    select
        customer_key,
        date_trunc('month', min(order_date)) as cohort_month
    from {{ ref('fct_orders') }}
    group by 1

),
monthly_orders as (

    select
        o.customer_key,
        date_trunc('month', o.order_date) as order_month,
        count(distinct o.order_key) as orders_in_month
    from {{ ref('fct_orders') }} o
    group by 1, 2

)
select
    cfo.cohort_month,
    mo.order_month,
    (extract(year from mo.order_month) - extract(year from cfo.cohort_month)) * 12
        + (extract(month from mo.order_month) - extract(month from cfo.cohort_month)) as months_since_first,
    count(distinct mo.customer_key) as active_customers,
    sum(mo.orders_in_month) as total_orders
from
    customer_first_order cfo
    join monthly_orders mo on cfo.customer_key = mo.customer_key
group by 1, 2, 3
