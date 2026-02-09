-- Compares order behavior for discounted vs non-discounted purchases
with items as (

    select
        order_key,
        customer_key,
        order_date,
        case when discount_percentage > 0 then true else false end as is_discounted,
        quantity,
        gross_item_sales_amount,
        discounted_item_sales_amount,
        item_discount_amount
    from {{ ref('fct_orders_items') }}

),
order_level as (

    select
        order_key,
        customer_key,
        order_date,
        sum(case when is_discounted then 1 else 0 end) as discounted_lines,
        sum(case when not is_discounted then 1 else 0 end) as full_price_lines,
        case
            when sum(case when is_discounted then 1 else 0 end) = 0 then 'all_full_price'
            when sum(case when not is_discounted then 1 else 0 end) = 0 then 'all_discounted'
            else 'mixed'
        end as order_discount_type,
        sum(quantity) as total_quantity,
        sum(gross_item_sales_amount) as gross_revenue,
        sum(abs(item_discount_amount)) as discount_amount
    from items
    group by 1, 2, 3

)
select
    order_discount_type,
    count(distinct order_key) as order_count,
    count(distinct customer_key) as customer_count,
    round(avg(total_quantity), 2) as avg_items_per_order,
    round(avg(gross_revenue), 2) as avg_order_value,
    sum(gross_revenue) as total_revenue,
    sum(discount_amount) as total_discount_given,
    round(sum(discount_amount) / nullif(sum(gross_revenue), 0) * 100, 2) as effective_discount_rate
from order_level
group by 1
