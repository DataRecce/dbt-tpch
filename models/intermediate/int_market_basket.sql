-- Co-purchased parts analysis: which parts appear together in orders
with order_parts as (

    select
        order_key,
        part_key
    from {{ ref('fct_orders_items') }}
    group by 1, 2

),
part_pairs as (

    select
        a.part_key as part_a,
        b.part_key as part_b,
        count(distinct a.order_key) as co_occurrence_count
    from
        order_parts a
        join order_parts b
            on a.order_key = b.order_key
            and a.part_key < b.part_key
    group by 1, 2

),
part_totals as (

    select
        part_key,
        count(distinct order_key) as order_count
    from order_parts
    group by 1

)
select
    pp.part_a,
    pp.part_b,
    pp.co_occurrence_count,
    pa.order_count as part_a_orders,
    pb.order_count as part_b_orders,
    round(pp.co_occurrence_count::decimal / nullif(pa.order_count, 0) * 100, 2) as pct_of_part_a_orders,
    round(pp.co_occurrence_count::decimal / nullif(pb.order_count, 0) * 100, 2) as pct_of_part_b_orders
from
    part_pairs pp
    join part_totals pa on pp.part_a = pa.part_key
    join part_totals pb on pp.part_b = pb.part_key
where
    pp.co_occurrence_count >= 5
