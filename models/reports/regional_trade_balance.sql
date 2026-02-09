-- Regional trade balance: import/export analysis by nation
with flows as (

    select
        s.supplier_nation_name as exporter_nation,
        s.supplier_region_name as exporter_region,
        c.customer_nation_name as importer_nation,
        c.customer_region_name as importer_region,
        sum(i.gross_item_sales_amount) as trade_value,
        sum(i.quantity) as trade_volume,
        count(distinct i.order_key) as order_count
    from {{ ref('fct_orders_items') }} i
    join {{ ref('dim_supplier') }} s on i.supplier_key = s.supplier_key
    join {{ ref('dim_customer') }} c on i.customer_key = c.customer_key
    group by 1, 2, 3, 4

),
exports as (

    select
        exporter_nation as nation,
        exporter_region as region,
        sum(trade_value) as export_value,
        sum(trade_volume) as export_volume
    from flows
    where exporter_nation != importer_nation
    group by 1, 2

),
imports as (

    select
        importer_nation as nation,
        importer_region as region,
        sum(trade_value) as import_value,
        sum(trade_volume) as import_volume
    from flows
    where exporter_nation != importer_nation
    group by 1, 2

),
domestic as (

    select
        exporter_nation as nation,
        exporter_region as region,
        sum(trade_value) as domestic_value,
        sum(trade_volume) as domestic_volume
    from flows
    where exporter_nation = importer_nation
    group by 1, 2

)
select
    coalesce(e.nation, i.nation, d.nation) as nation,
    coalesce(e.region, i.region, d.region) as region,
    coalesce(e.export_value, 0) as export_value,
    coalesce(i.import_value, 0) as import_value,
    coalesce(d.domestic_value, 0) as domestic_value,
    coalesce(e.export_value, 0) - coalesce(i.import_value, 0) as trade_balance,
    coalesce(e.export_volume, 0) as export_volume,
    coalesce(i.import_volume, 0) as import_volume,
    coalesce(d.domestic_volume, 0) as domestic_volume,
    round(coalesce(d.domestic_value, 0) * 100.0
        / nullif(coalesce(e.export_value, 0) + coalesce(i.import_value, 0) + coalesce(d.domestic_value, 0), 0),
        2) as domestic_share_pct,
    case
        when coalesce(e.export_value, 0) > coalesce(i.import_value, 0) then 'surplus'
        when coalesce(e.export_value, 0) < coalesce(i.import_value, 0) then 'deficit'
        else 'balanced'
    end as trade_position
from exports e
full outer join imports i on e.nation = i.nation
full outer join domestic d on coalesce(e.nation, i.nation) = d.nation
