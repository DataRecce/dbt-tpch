-- Most ordered parts by period with trend
with monthly_parts as (

    select
        date_trunc('month', i.order_date) as order_month,
        i.part_key,
        p.part_name,
        p.part_type_name,
        sum(i.quantity) as total_quantity,
        sum(i.gross_item_sales_amount) as total_revenue,
        count(distinct i.order_key) as order_count
    from {{ ref('fct_orders_items') }} i
    join {{ ref('dim_part') }} p on i.part_key = p.part_key
    group by 1, 2, 3, 4

),
ranked as (

    select
        *,
        rank() over (partition by order_month order by total_revenue desc) as revenue_rank,
        lag(total_revenue) over (partition by part_key order by order_month) as prev_month_revenue
    from monthly_parts

)
select
    order_month,
    part_key,
    part_name,
    part_type_name,
    total_quantity,
    total_revenue,
    order_count,
    revenue_rank,
    prev_month_revenue,
    round((total_revenue - coalesce(prev_month_revenue, 0))
        / nullif(coalesce(prev_month_revenue, 0), 0) * 100, 2) as revenue_growth_pct
from ranked
where revenue_rank <= 50
