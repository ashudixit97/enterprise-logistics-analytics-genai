select *
from {{ ref('stg_delivery_events') }}
where detention_minutes < 0