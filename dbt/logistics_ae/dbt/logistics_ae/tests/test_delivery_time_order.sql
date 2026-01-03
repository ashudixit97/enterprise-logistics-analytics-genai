select *
from {{ ref('stg_delivery_events') }}
where actual_ts < scheduled_ts
  and datediff('hour', actual_ts, scheduled_ts) > 2