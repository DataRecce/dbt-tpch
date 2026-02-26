-- Average delivery time by shipping mode per month
--
-- FALSE ALARM DEMO: TABLE model with target.name branching.
-- pg-base analyzes 7 years of ship dates, pg-current only 2 years.
-- Mirrors real pattern: prod builds full history, dev builds subset.
-- This is NOT incremental — it's a plain table with conditional logic.

{% set reference_date = "'1998-08-02'" %}

with items as (

    select * from {{ ref('fct_orders_items') }}

)
select
    date_trunc('month', i.ship_date) as ship_month,
    i.ship_mode_name,
    count(*) as shipment_count,
    avg(i.receipt_date - i.ship_date) as avg_transit_days,
    avg(i.ship_date - i.order_date) as avg_processing_days,
    avg(i.receipt_date - i.order_date) as avg_total_days,
    sum(case when i.receipt_date > i.commit_date then 1 else 0 end) as late_count,
    round(sum(case when i.receipt_date > i.commit_date then 1 else 0 end)::decimal
        / nullif(count(*), 0) * 100, 2) as late_pct
from items i
where
    i.receipt_date is not null
    {% if target.name == 'pg-base' %}
    and i.ship_date >= {{ reference_date }}::date - interval '2555 days'
    {% else %}
    and i.ship_date >= {{ reference_date }}::date - interval '730 days'
    {% endif %}
    and i.ship_date <= {{ reference_date }}::date
group by 1, 2
