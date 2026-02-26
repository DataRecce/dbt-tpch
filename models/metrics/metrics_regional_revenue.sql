-- Revenue by region and nation over time
--
-- FALSE ALARM DEMO: TABLE model with target-dependent date window.
-- pg-base gets 7 years of history, pg-current gets 2 years.
-- Same pattern as prod vs dev environments with different data needs.
-- This is NOT incremental — it's a plain table with conditional logic.

{% set reference_date = "'1998-08-02'" %}

with orders as (

    select * from {{ ref('fct_orders') }}

),
customers as (

    select * from {{ ref('dim_customer') }}

)
select
    date_trunc('month', o.order_date) as order_month,
    c.customer_region_name as region_name,
    c.customer_nation_name as nation_name,
    count(distinct o.order_key) as order_count,
    count(distinct o.customer_key) as customer_count,
    sum(o.gross_item_sales_amount) as gross_revenue,
    sum(o.net_item_sales_amount) as net_revenue
from
    orders o
    join customers c on o.customer_key = c.customer_key
where
    o.order_date >= {{ reference_date }}::date - interval '{{ 2555 if target.name == "pg-base" else 730 }} days'
    and o.order_date <= {{ reference_date }}::date
group by 1, 2, 3
