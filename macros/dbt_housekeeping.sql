{% macro dbt_housekeeping() -%}
    cast('{{ invocation_id }}' as varchar) as dbt_batch_id,
    cast('{{ run_started_at }}' as timestamp) as dbt_batch_ts
{%- endmacro %}
