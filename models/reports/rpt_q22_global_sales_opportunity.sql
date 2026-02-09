/*
TPC-H Q22: Global Sales Opportunity
Customers with above-average balance who haven't placed orders, by country code.
*/
with customers as (

    select * from {{ ref('dim_customer') }}

),
orders as (

    select * from {{ ref('fct_orders') }}

),
customer_phones as (

    select
        c.customer_key,
        substring(c.customer_phone_number, 1, 2) as cntrycode,
        c.customer_account_balance
    from customers c
    where substring(c.customer_phone_number, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')

),
avg_balance as (

    select avg(customer_account_balance) as avg_bal
    from customer_phones
    where customer_account_balance > 0

),
no_orders as (

    select cp.customer_key, cp.cntrycode, cp.customer_account_balance
    from customer_phones cp
    cross join avg_balance ab
    where
        cp.customer_account_balance > ab.avg_bal
        and not exists (
            select 1 from orders o where o.customer_key = cp.customer_key
        )

)
select
    cntrycode,
    count(*) as numcust,
    sum(customer_account_balance) as totacctbal
from no_orders
group by 1
order by 1
