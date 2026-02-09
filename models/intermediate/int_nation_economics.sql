-- Economic indicators aggregated by nation
with customer_stats as (

    select
        c.customer_nation_name as nation_name,
        c.customer_region_name as region_name,
        count(distinct c.customer_key) as customer_count,
        sum(c.customer_account_balance) as total_customer_balance,
        avg(c.customer_account_balance) as avg_customer_balance
    from {{ ref('dim_customer') }} c
    group by 1, 2

),
supplier_stats as (

    select
        s.supplier_nation_name as nation_name,
        count(distinct s.supplier_key) as supplier_count,
        sum(s.supplier_account_balance) as total_supplier_balance,
        avg(s.supplier_account_balance) as avg_supplier_balance
    from {{ ref('dim_supplier') }} s
    group by 1

),
order_stats as (

    select
        c.customer_nation_name as nation_name,
        sum(o.gross_item_sales_amount) as total_order_revenue,
        sum(o.net_item_sales_amount) as total_net_revenue,
        count(distinct o.order_key) as total_orders
    from {{ ref('fct_orders') }} o
    join {{ ref('dim_customer') }} c on o.customer_key = c.customer_key
    group by 1

)
select
    cs.nation_name,
    cs.region_name,
    cs.customer_count,
    coalesce(ss.supplier_count, 0) as supplier_count,
    cs.total_customer_balance,
    cs.avg_customer_balance,
    coalesce(ss.total_supplier_balance, 0) as total_supplier_balance,
    coalesce(ss.avg_supplier_balance, 0) as avg_supplier_balance,
    coalesce(os.total_order_revenue, 0) as total_order_revenue,
    coalesce(os.total_net_revenue, 0) as total_net_revenue,
    coalesce(os.total_orders, 0) as total_orders,
    round(coalesce(os.total_order_revenue, 0) / nullif(cs.customer_count, 0), 2) as revenue_per_customer
from
    customer_stats cs
    left join supplier_stats ss on cs.nation_name = ss.nation_name
    left join order_stats os on cs.nation_name = os.nation_name
