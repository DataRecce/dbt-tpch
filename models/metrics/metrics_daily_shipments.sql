{{
    config(
        materialized = 'incremental',
        unique_key = 'ship_date',
        incremental_strategy = 'delete+insert'
    )
}}

-- Incremental model: daily shipment metrics
-- This model demonstrates the isolated base scenario:
-- Production accumulates years of data, while a fresh PR build
-- only sees recent shipments, causing row count false alarms.

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
    {% endif %}
group by
    oi.ship_date
order by
    oi.ship_date
