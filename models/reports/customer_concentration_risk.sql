-- Pareto (80/20) analysis of customer revenue concentration
-- Identifies which customers drive the majority of revenue
with customer_revenue as (

    select
        o.customer_key,
        c.customer_name,
        c.customer_nation_name,
        c.customer_market_segment_name,
        sum(o.gross_item_sales_amount) as total_revenue,
        count(distinct o.order_key) as order_count
    from {{ ref('fct_orders') }} o
    join {{ ref('dim_customer') }} c on o.customer_key = c.customer_key
    group by 1, 2, 3, 4

),
ranked as (

    select
        *,
        row_number() over (order by total_revenue desc) as revenue_rank,
        count(*) over () as total_customers,
        sum(total_revenue) over () as grand_total_revenue,
        sum(total_revenue) over (order by total_revenue desc) as cumulative_revenue
    from customer_revenue

)
select
    revenue_rank,
    customer_key,
    customer_name,
    customer_nation_name,
    customer_market_segment_name,
    order_count,
    total_revenue,
    cumulative_revenue,
    grand_total_revenue,
    round(total_revenue / grand_total_revenue * 100, 4) as pct_of_total_revenue,
    round(cumulative_revenue / grand_total_revenue * 100, 2) as cumulative_pct,
    round(revenue_rank::decimal / total_customers * 100, 2) as customer_percentile,
    case
        when cumulative_revenue / grand_total_revenue <= 0.80 then 'top_80_pct'
        when cumulative_revenue / grand_total_revenue <= 0.95 then 'middle_15_pct'
        else 'bottom_5_pct'
    end as pareto_tier
from ranked
