/*
TPC-H Q13: Customer Distribution
Distribution of customers by their order count (excluding special requests).
*/
with customer_orders as (

    select
        c.customer_key,
        count(o.order_key) as order_count
    from
        {{ ref('dim_customer') }} c
        left join {{ ref('fct_orders') }} o
            on c.customer_key = o.customer_key
    group by 1

)
select
    order_count as c_count,
    count(*) as custdist
from customer_orders
group by 1
order by custdist desc, c_count desc
