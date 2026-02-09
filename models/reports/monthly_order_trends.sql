-- Monthly order trends with new vs returning customer breakdown
with customer_first as (

    select
        customer_key,
        min(order_date) as first_order_date
    from {{ ref('fct_orders') }}
    group by 1

),
orders as (

    select
        o.*,
        cf.first_order_date,
        case
            when date_trunc('month', o.order_date) = date_trunc('month', cf.first_order_date)
                then 'new'
            else 'returning'
        end as customer_type
    from {{ ref('fct_orders') }} o
    join customer_first cf on o.customer_key = cf.customer_key

)
select
    date_trunc('month', order_date) as order_month,
    count(distinct order_key) as total_orders,
    count(distinct customer_key) as unique_customers,
    count(distinct case when customer_type = 'new' then customer_key end) as new_customers,
    count(distinct case when customer_type = 'returning' then customer_key end) as returning_customers,
    sum(gross_item_sales_amount) as total_revenue,
    sum(case when customer_type = 'new' then gross_item_sales_amount else 0 end) as new_customer_revenue,
    sum(case when customer_type = 'returning' then gross_item_sales_amount else 0 end) as returning_customer_revenue,
    round(avg(gross_item_sales_amount), 2) as avg_order_value,
    lag(sum(gross_item_sales_amount)) over (order by date_trunc('month', order_date)) as prev_month_revenue,
    round((sum(gross_item_sales_amount) - lag(sum(gross_item_sales_amount)) over (order by date_trunc('month', order_date)))
        / nullif(lag(sum(gross_item_sales_amount)) over (order by date_trunc('month', order_date)), 0) * 100, 2) as revenue_mom_growth_pct
from orders
group by 1
