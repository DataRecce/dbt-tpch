-- Fully enriched order line items with customer, supplier, part, and nation details
with fct as (

    select * from {{ ref('fct_orders_items') }}

),
customers as (

    select * from {{ ref('dim_customer') }}

),
suppliers as (

    select * from {{ ref('dim_supplier') }}

),
parts as (

    select * from {{ ref('dim_part') }}

)
select
    f.order_item_key,
    f.order_key,
    f.order_date,
    f.order_status_code,
    f.order_line_number,
    f.order_line_status_code,
    f.return_status_code,
    f.ship_date,
    f.commit_date,
    f.receipt_date,
    f.ship_mode_name,

    c.customer_key,
    c.customer_name,
    c.customer_nation_name,
    c.customer_region_name,
    c.customer_market_segment_name,

    s.supplier_key,
    s.supplier_name,
    s.supplier_nation_name,
    s.supplier_region_name,

    p.part_key,
    p.part_name,
    p.part_type_name,
    p.part_brand_name,
    p.part_manufacturer_name,
    p.part_size,

    f.quantity,
    f.base_price,
    f.discount_percentage,
    f.tax_rate,
    f.supplier_cost_amount,
    f.gross_item_sales_amount,
    f.discounted_item_sales_amount,
    f.item_discount_amount,
    f.item_tax_amount,
    f.net_item_sales_amount,
    (f.gross_item_sales_amount - f.supplier_cost_amount * f.quantity) as profit_amount
from
    fct f
    left join customers c on f.customer_key = c.customer_key
    left join suppliers s on f.supplier_key = s.supplier_key
    left join parts p on f.part_key = p.part_key
