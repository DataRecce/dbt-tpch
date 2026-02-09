-- Monthly revenue KPI dashboard with MoM growth and rolling averages
with monthly as (

    select
        date_trunc('month', order_date) as order_month,
        count(distinct order_key) as order_count,
        count(distinct customer_key) as unique_customers,
        sum(gross_item_sales_amount) as gross_revenue,
        sum(net_item_sales_amount) as net_revenue,
        sum(item_discount_amount) as total_discounts,
        round(avg(gross_item_sales_amount), 2) as avg_order_value
    from {{ ref('fct_orders') }}
    group by 1

)
select
    order_month,
    order_count,
    unique_customers,
    gross_revenue,
    net_revenue,
    total_discounts,
    avg_order_value,
    lag(gross_revenue) over (order by order_month) as prev_month_revenue,
    round((gross_revenue - lag(gross_revenue) over (order by order_month))
        / nullif(lag(gross_revenue) over (order by order_month), 0) * 100, 2) as revenue_mom_growth_pct,
    round(avg(gross_revenue) over (
        order by order_month rows between 2 preceding and current row
    ), 2) as rolling_3m_avg_revenue,
    sum(gross_revenue) over (
        order by order_month rows between unbounded preceding and current row
    ) as cumulative_revenue
from monthly
