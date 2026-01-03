-- Fail only if the % of bad delivery rows exceeds a threshold (e.g., 5%)
with base as (
    select *
    from {{ ref('stg_delivery_events') }}
    where lower(event_type) = 'delivery'
),

bad as (
    select *
    from base
    where actual_ts < scheduled_ts
      and datediff('hour', actual_ts, scheduled_ts) > 2
),

rates as (
    select
        (select count(*) from bad) as bad_rows,
        (select count(*) from base) as total_rows
)

select *
from rates
where bad_rows / nullif(total_rows, 0) > 0.07
