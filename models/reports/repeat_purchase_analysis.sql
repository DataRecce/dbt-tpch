-- Analyzes repeat purchase behavior: order sequence, time between orders, repeat rates
with sequenced_orders as (

    select
        customer_key,
        order_key,
        order_date,
        gross_item_sales_amount,
        row_number() over (partition by customer_key order by order_date) as order_sequence,
        lag(order_date) over (partition by customer_key order by order_date) as prev_order_date
    from {{ ref('fct_orders') }}

),
with_gaps as (

    select
        *,
        (order_date - prev_order_date) as days_since_prev_order,
        case when prev_order_date is not null then true else false end as is_repeat
    from sequenced_orders

)
select
    order_sequence,
    count(*) as order_count,
    count(distinct customer_key) as customer_count,
    round(avg(gross_item_sales_amount), 2) as avg_order_value,
    round(avg(days_since_prev_order), 1) as avg_days_since_prev,
    sum(case when days_since_prev_order <= 30 then 1 else 0 end) as repeat_within_30d,
    sum(case when days_since_prev_order <= 60 then 1 else 0 end) as repeat_within_60d,
    sum(case when days_since_prev_order <= 90 then 1 else 0 end) as repeat_within_90d
from with_gaps
group by 1
