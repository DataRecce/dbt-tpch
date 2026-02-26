{{
    config(
        materialized = 'incremental',
        unique_key = 'ship_date',
        incremental_strategy = 'delete+insert'
    )
}}

-- Demonstrates why incremental models cause false alarms in Recce.
--
-- The root cause is NOT "data accumulation" — it's the conditional logic
-- below that produces DIFFERENT SQL depending on build context:
--
--   is_incremental() = true  → filters from max(ship_date) in existing table
--   is_incremental() = false → filters last N days from a reference date
--     (N depends on target: prod gets 365 days, dev/PR gets 90 days)
--
-- Two environments built at different times or with different history
-- will run different SQL → different results → false alarm diffs.
--
-- This mirrors real-world patterns like the fct_cmab_strategy_reward example
-- where prod gets -8 days and dev gets -2 days from current_date().

{% set reference_date = "'1998-08-02'" %}

select
    oi.ship_date,
    count(*) as shipment_count,
    count(distinct oi.order_key) as order_count,
    count(distinct oi.supplier_key) as supplier_count,
    sum(oi.gross_item_sales_amount)::decimal(16,4) as total_revenue,
    avg(oi.gross_item_sales_amount)::decimal(16,4) as avg_revenue_per_item
from
    {{ ref('orders_items') }} oi
where
    oi.ship_date is not null
    {% if is_incremental() %}
    and oi.ship_date > (select max(ship_date) from {{ this }})
    and oi.ship_date <= {{ reference_date }}::date
    {% else %}
    and oi.ship_date >= {{ reference_date }}::date - interval '{{ 365 if target.name == "pg-base" else 90 }} days'
    and oi.ship_date <= {{ reference_date }}::date
    {% endif %}
group by
    oi.ship_date
order by
    oi.ship_date
