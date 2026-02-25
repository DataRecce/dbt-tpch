{{
    config(
        materialized = 'table'
    )
}}
with orders as (

    select * from {{ ref('base_orders') }}

),
line_items as (

    select * from {{ ref('base_line_item') }}

),
joined as (
    select

        {{ dbt_utils.generate_surrogate_key(['o.order_key', 'l.order_line_number']) }} as order_item_key,

        o.order_key,
        o.order_date,
        o.customer_key,
        o.order_status_code,

        l.part_key,
        l.supplier_key,
        l.return_status_code,
        l.order_line_number,
        l.order_line_status_code,
        l.ship_date,
        l.commit_date,
        l.receipt_date,
        l.ship_mode_name,

        l.quantity,
        l.discount_percentage,
        l.tax_rate,
        l.extended_price,

        -- extended_price is actually the line item total,
        -- so we back out the extended price per item
        (l.extended_price/nullif(l.quantity, 0)){{ money() }} as base_price,
        (l.extended_price * (1 - l.discount_percentage)){{ money() }} as discounted_item_sales_amount,
        (-1 * l.extended_price * l.discount_percentage){{ money() }} as item_discount_amount

    from
        orders o
        join
        line_items l
            on o.order_key = l.order_key
)
select
    order_item_key,
    order_key,
    order_date,
    customer_key,
    order_status_code,
    part_key,
    supplier_key,
    return_status_code,
    order_line_number,
    order_line_status_code,
    ship_date,
    commit_date,
    receipt_date,
    ship_mode_name,
    quantity,
    base_price,
    discount_percentage,
    (base_price * (1 - discount_percentage)){{ money() }} as discounted_price,
    extended_price as gross_item_sales_amount,
    discounted_item_sales_amount,
    -- We model discounts as negative amounts
    item_discount_amount,
    tax_rate,
    ((extended_price + item_discount_amount) * tax_rate){{ money() }} as item_tax_amount,
    (
        extended_price +
        item_discount_amount +
        ((extended_price + item_discount_amount) * tax_rate)
    ){{ money() }} as net_item_sales_amount
from
    joined
order by
    order_date
