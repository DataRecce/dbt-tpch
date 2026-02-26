{{
    config(
        materialized = 'view'
    )
}}

-- Monthly order summary by priority level
--
-- FALSE ALARM DEMO: VIEW with target-dependent date window.
-- pg-base gets 5 years of history, pg-current gets 1 year.
-- Even views — which don't store data — produce different results
-- when their SQL definition varies by build context.
-- This is NOT incremental, NOT a table — it's a plain view.

{% set reference_date = "'1998-08-02'" %}

select
    date_trunc('month', o.order_date) as order_month,
    o.order_priority_code as priority,
    count(distinct o.order_key) as order_count,
    count(distinct o.customer_key) as customer_count,
    sum(o.gross_item_sales_amount)::decimal(16,4) as total_revenue,
    avg(o.gross_item_sales_amount)::decimal(16,4) as avg_order_value
from
    {{ ref('fct_orders') }} o
where
    o.order_date >= {{ reference_date }}::date - interval '{{ 1825 if target.name == "pg-base" else 365 }} days'
    and o.order_date <= {{ reference_date }}::date
group by 1, 2
