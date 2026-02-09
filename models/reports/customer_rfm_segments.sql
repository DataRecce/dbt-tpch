-- RFM (Recency, Frequency, Monetary) customer segmentation
-- Assigns each customer a 1-5 score per dimension and maps to named segments
with order_metrics as (

    select
        customer_key,
        max(order_date) as last_order_date,
        count(distinct order_key) as frequency,
        sum(gross_item_sales_amount) as monetary
    from {{ ref('fct_orders') }}
    group by 1

),
max_date as (

    select max(order_date) as reference_date
    from {{ ref('fct_orders') }}

),
rfm_scores as (

    select
        om.customer_key,
        om.last_order_date,
        (md.reference_date - om.last_order_date) as recency_days,
        om.frequency,
        om.monetary,
        ntile(5) over (order by (md.reference_date - om.last_order_date) desc) as r_score,
        ntile(5) over (order by om.frequency) as f_score,
        ntile(5) over (order by om.monetary) as m_score
    from order_metrics om
    cross join max_date md

)
select
    r.customer_key,
    c.customer_name,
    c.customer_nation_name,
    c.customer_market_segment_name,
    r.last_order_date,
    r.recency_days,
    r.frequency,
    r.monetary,
    r.r_score,
    r.f_score,
    r.m_score,
    (r.r_score + r.f_score + r.m_score) as rfm_total,
    case
        when r.r_score >= 4 and r.f_score >= 4 and r.m_score >= 4 then 'champion'
        when r.r_score >= 3 and r.f_score >= 3 and r.m_score >= 3 then 'loyal'
        when r.r_score >= 4 and r.f_score <= 2 then 'new_customer'
        when r.r_score >= 3 and r.f_score >= 3 and r.m_score <= 2 then 'potential_loyalist'
        when r.r_score <= 2 and r.f_score >= 3 and r.m_score >= 3 then 'at_risk'
        when r.r_score <= 2 and r.f_score >= 4 and r.m_score >= 4 then 'cant_lose_them'
        when r.r_score <= 2 and r.f_score <= 2 then 'hibernating'
        else 'need_attention'
    end as rfm_segment
from rfm_scores r
join {{ ref('dim_customer') }} c on r.customer_key = c.customer_key
