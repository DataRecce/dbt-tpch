-- Part sales velocity: identifies fast movers vs slow movers
with part_monthly as (

    select
        i.part_key,
        date_trunc('month', i.order_date) as order_month,
        sum(i.quantity) as monthly_units,
        sum(i.gross_item_sales_amount) as monthly_revenue
    from {{ ref('fct_orders_items') }} i
    group by 1, 2

),
part_stats as (

    select
        part_key,
        count(distinct order_month) as active_months,
        sum(monthly_units) as total_units,
        sum(monthly_revenue) as total_revenue,
        avg(monthly_units) as avg_monthly_units,
        avg(monthly_revenue) as avg_monthly_revenue
    from part_monthly
    group by 1

)
select
    ps.part_key,
    p.part_name,
    p.part_type_name,
    p.part_brand_name,
    ps.active_months,
    ps.total_units,
    ps.total_revenue,
    round(ps.avg_monthly_units, 2) as avg_monthly_units,
    round(ps.avg_monthly_revenue, 2) as avg_monthly_revenue,
    ntile(4) over (order by ps.avg_monthly_units) as velocity_quartile,
    case
        when ntile(4) over (order by ps.avg_monthly_units) = 4 then 'fast_mover'
        when ntile(4) over (order by ps.avg_monthly_units) = 3 then 'moderate'
        when ntile(4) over (order by ps.avg_monthly_units) = 2 then 'slow_mover'
        else 'very_slow'
    end as velocity_category
from part_stats ps
join {{ ref('dim_part') }} p on ps.part_key = p.part_key
