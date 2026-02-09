-- Late deliveries with delay duration and contributing factors
with items as (

    select * from {{ ref('fct_orders_items') }}

),
suppliers as (

    select * from {{ ref('dim_supplier') }}

)
select
    i.order_item_key,
    i.order_key,
    i.order_date,
    i.ship_date,
    i.commit_date,
    i.receipt_date,
    i.ship_mode_name,
    i.customer_key,
    i.supplier_key,
    s.supplier_name,
    s.supplier_nation_name,
    i.part_key,
    i.quantity,
    i.gross_item_sales_amount,
    (i.receipt_date - i.commit_date) as days_past_commit,
    (i.receipt_date - i.ship_date) as transit_days,
    (i.ship_date - i.order_date) as processing_days,
    case
        when i.receipt_date > i.commit_date then 'late'
        when i.receipt_date = i.commit_date then 'on_time'
        else 'early'
    end as delivery_status
from
    items i
    join suppliers s on i.supplier_key = s.supplier_key
where
    i.receipt_date is not null
