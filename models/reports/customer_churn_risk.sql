-- Flags customers at risk of churn based on order recency vs historical cadence
with order_gaps as (

    select
        customer_key,
        order_date,
        lag(order_date) over (partition by customer_key order by order_date) as prev_order_date,
        (order_date - lag(order_date) over (partition by customer_key order by order_date)) as days_between_orders
    from {{ ref('fct_orders') }}

),
customer_cadence as (

    select
        customer_key,
        count(*) as total_orders,
        avg(days_between_orders) as avg_days_between_orders,
        max(order_date) as last_order_date
    from order_gaps
    group by 1

),
reference as (

    select max(order_date) as reference_date
    from {{ ref('fct_orders') }}

)
select
    cc.customer_key,
    c.customer_name,
    c.customer_nation_name,
    c.customer_market_segment_name,
    cc.total_orders,
    cc.last_order_date,
    (r.reference_date - cc.last_order_date) as days_since_last_order,
    round(cc.avg_days_between_orders, 1) as avg_days_between_orders,
    case
        when cc.total_orders <= 1 then null
        else round((r.reference_date - cc.last_order_date)::decimal
            / nullif(cc.avg_days_between_orders, 0), 2)
    end as recency_ratio,
    case
        when cc.total_orders <= 1 then 'insufficient_data'
        when (r.reference_date - cc.last_order_date) <= cc.avg_days_between_orders then 'active'
        when (r.reference_date - cc.last_order_date) <= cc.avg_days_between_orders * 2 then 'at_risk'
        else 'churned'
    end as churn_status
from customer_cadence cc
cross join reference r
join {{ ref('dim_customer') }} c on cc.customer_key = c.customer_key
