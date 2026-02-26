{{
    config(
        materialized = 'incremental',
        unique_key = 'order_date',
        incremental_strategy = 'delete+insert'
    )
}}

-- SAFE INCREMENTAL: deterministic else branch — NO false alarm.
--
-- This model IS incremental, but its else branch (used on fresh CI builds)
-- uses a FIXED date range with no target.name or current_date() dependency.
-- Both pg-base and pg-current produce identical SQL on first build.
--
-- Contrast with metrics_daily_shipments which has target.name in the else
-- branch → different SQL per target → false alarm.
--
-- Key insight: is_incremental() alone does NOT cause false alarms.
-- The false alarm comes from non-deterministic logic INSIDE the branches.

{% set reference_date = "'1998-08-02'" %}

select
    o.order_date,
    count(distinct o.order_key) as order_count,
    count(distinct o.customer_key) as customer_count,
    sum(o.gross_item_sales_amount)::decimal(16,4) as total_revenue,
    avg(o.gross_item_sales_amount)::decimal(16,4) as avg_order_value
from
    {{ ref('fct_orders') }} o
where
    o.order_date is not null
    {% if is_incremental() %}
    and o.order_date > (select max(order_date) from {{ this }})
    and o.order_date <= {{ reference_date }}::date
    {% else %}
    and o.order_date <= {{ reference_date }}::date
    {% endif %}
group by
    o.order_date
order by
    o.order_date
