-- ABC inventory classification based on revenue contribution
-- A = top 80% revenue, B = next 15%, C = bottom 5%
with part_revenue as (

    select
        i.part_key,
        p.part_name,
        p.part_type_name,
        p.part_brand_name,
        p.part_manufacturer_name,
        sum(i.gross_item_sales_amount) as total_revenue,
        sum(i.quantity) as total_quantity,
        count(distinct i.order_key) as order_count
    from {{ ref('fct_orders_items') }} i
    join {{ ref('dim_part') }} p on i.part_key = p.part_key
    group by 1, 2, 3, 4, 5

),
ranked as (

    select
        *,
        sum(total_revenue) over () as grand_total,
        sum(total_revenue) over (order by total_revenue desc) as running_total,
        row_number() over (order by total_revenue desc) as revenue_rank
    from part_revenue

)
select
    revenue_rank,
    part_key,
    part_name,
    part_type_name,
    part_brand_name,
    part_manufacturer_name,
    total_revenue,
    total_quantity,
    order_count,
    round(total_revenue / grand_total * 100, 4) as pct_of_revenue,
    round(running_total / grand_total * 100, 2) as cumulative_pct,
    case
        when running_total / grand_total <= 0.80 then 'A'
        when running_total / grand_total <= 0.95 then 'B'
        else 'C'
    end as abc_class
from ranked
