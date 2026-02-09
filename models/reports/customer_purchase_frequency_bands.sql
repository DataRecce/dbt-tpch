-- Segments customers into purchase frequency bands
with customer_orders as (

    select
        o.customer_key,
        count(distinct o.order_key) as order_count,
        sum(o.gross_item_sales_amount) as total_revenue
    from {{ ref('fct_orders') }} o
    group by 1

),
banded as (

    select
        *,
        case
            when order_count = 1 then 'one_time'
            when order_count between 2 and 5 then 'occasional'
            when order_count between 6 and 15 then 'regular'
            when order_count > 15 then 'power_buyer'
        end as frequency_band
    from customer_orders

)
select
    frequency_band,
    count(*) as customer_count,
    round(count(*)::decimal / sum(count(*)) over () * 100, 2) as pct_of_customers,
    sum(total_revenue) as band_total_revenue,
    round(sum(total_revenue) / sum(sum(total_revenue)) over () * 100, 2) as pct_of_revenue,
    round(avg(total_revenue), 2) as avg_revenue_per_customer,
    avg(order_count) as avg_orders_per_customer,
    min(order_count) as min_orders,
    max(order_count) as max_orders
from banded
group by 1
