-- Lead time distribution per supplier with percentiles and variability
with shipments as (

    select
        supplier_key,
        (receipt_date - order_date) as lead_time_days,
        (ship_date - order_date) as processing_days,
        (receipt_date - ship_date) as transit_days
    from {{ ref('fct_orders_items') }}
    where receipt_date is not null

)
select
    sh.supplier_key,
    s.supplier_name,
    s.supplier_nation_name,
    count(*) as shipment_count,
    round(avg(sh.lead_time_days), 1) as avg_lead_time,
    round(avg(sh.processing_days), 1) as avg_processing_days,
    round(avg(sh.transit_days), 1) as avg_transit_days,
    min(sh.lead_time_days) as min_lead_time,
    max(sh.lead_time_days) as max_lead_time,
    percentile_cont(0.50) within group (order by sh.lead_time_days) as p50_lead_time,
    percentile_cont(0.90) within group (order by sh.lead_time_days) as p90_lead_time,
    percentile_cont(0.95) within group (order by sh.lead_time_days) as p95_lead_time,
    round(stddev(sh.lead_time_days), 2) as lead_time_stddev,
    round(stddev(sh.lead_time_days) / nullif(avg(sh.lead_time_days), 0), 3) as lead_time_cv
from shipments sh
join {{ ref('dim_supplier') }} s on sh.supplier_key = s.supplier_key
group by 1, 2, 3
