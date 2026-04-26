-- MetricFlow Time Spine
-- Required for time-based metric calculations (cumulative, period-over-period)
-- Generates a daily date dimension from 2020 to 2030

{{
    config(
        materialized='table',
        tags=['semantic', 'utility']
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
)

select
    cast(date_day as date) as date_day
from date_spine
