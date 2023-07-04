-- model: incremental_loading_from_external_table.sql
{{
    config(
        schema='landing_zone',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
          "field": "col_c_ts",
          "data_type": "timestamp",
          "granularity": "hour"
        }
    )
}}

-- This will be defined before DBT generates the SQL model bellow
{% call set_sql_header(config) %}
declare min_incremental_ts timestamp;
set min_incremental_ts = (select timestamp_sub(current_timestamp(), interval 24 hour) );
{%- endcall %}

{% if is_incremental() %}
    with increment_from_here as (
        select
            -- we want to increment from the latest partition given our minimal threshold
            -- if there aren't unloaded partitions within the threshold, we will increment from it
            coalesce(max(col_c_ts), min_incremental_ts)
        from
            {{this}}
         where
            col_c_ts >= min_incremental_ts
    )
{% endif %}

    select
        col_a,
        col_b,
        TIMESTAMP_MILLIS(col_c) as col_c_ts, -- our little `t` between the `E` and the `L`
        current_timestamp() as _dbt_run_ts
    from
        {{ source( 'landing_zone', 'my_external_table') }}

{% if is_incremental() %}
    where
        -- taking from the external table only partitions > threshold
        timestamp(datetime(year, month, day,  hour, 0, 0), 'Etc/UTC') > (select * from increment_from_here)
{% endif %}
