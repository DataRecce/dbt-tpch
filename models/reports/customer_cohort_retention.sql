-- Customer retention by first-purchase cohort
-- Shows how many customers from each cohort remain active in subsequent months
with customer_cohort as (

    select
        customer_key,
        date_trunc('month', min(order_date)) as cohort_month
    from {{ ref('fct_orders') }}
    group by 1

),
customer_activity as (

    select
        o.customer_key,
        date_trunc('month', o.order_date) as activity_month
    from {{ ref('fct_orders') }} o
    group by 1, 2

),
cohort_activity as (

    select
        cc.cohort_month,
        ca.activity_month,
        cast(
            (extract(year from ca.activity_month) - extract(year from cc.cohort_month)) * 12
            + extract(month from ca.activity_month) - extract(month from cc.cohort_month)
        as int) as period_number,
        count(distinct ca.customer_key) as active_customers
    from customer_cohort cc
    join customer_activity ca on cc.customer_key = ca.customer_key
    group by 1, 2, 3

),
cohort_sizes as (

    select
        cohort_month,
        count(distinct customer_key) as cohort_size
    from customer_cohort
    group by 1

)
select
    ca.cohort_month,
    ca.activity_month,
    ca.period_number,
    cs.cohort_size,
    ca.active_customers,
    round(ca.active_customers::decimal / cs.cohort_size * 100, 2) as retention_pct
from cohort_activity ca
join cohort_sizes cs on ca.cohort_month = cs.cohort_month
