with source as (
    select * from {{ source('raw', 'ROUTES_RAW') }}
)

select
    ROUTE_ID::string as route_id,
    ORIGIN_CITY::string as origin_city,
    ORIGIN_STATE::string as origin_state,
    DESTINATION_CITY::string as destination_city,
    DESTINATION_STATE::string as destination_state,

    TYPICAL_DISTANCE_MILES::number as typical_distance_miles,
    BASE_RATE_PER_MILE::number as base_rate_per_mile,
    FUEL_SURCHARGE_RATE::number as fuel_surcharge_rate,
    TYPICAL_TRANSIT_DAYS::number as typical_transit_days
from source
