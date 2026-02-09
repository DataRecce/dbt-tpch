/*
TPC-H Q6: Forecasting Revenue Change
Revenue impact if discounts in a range were eliminated.
*/
select
    sum(i.gross_item_sales_amount * i.discount_percentage) as revenue_change
from
    {{ ref('fct_orders_items') }} i
where
    i.ship_date >= date '1994-01-01'
    and i.ship_date < date '1995-01-01'
    and i.discount_percentage between 0.05 and 0.07
    and i.quantity < 24
