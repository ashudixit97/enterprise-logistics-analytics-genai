with source as (
    select * from {{ source('raw', 'DELIVERY_EVENTS_RAW') }}
)

select
    *,
    current_timestamp() as _loaded_at
from source
