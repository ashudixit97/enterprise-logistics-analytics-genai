with source as (
    select * from {{ source('raw', 'TRIPS_RAW') }}
)

select
    TRIP_ID::string as trip_id,
    LOAD_ID::string as load_id,
    DRIVER_ID::string as driver_id,
    TRUCK_ID::string as truck_id,
    TRAILER_ID::string as trailer_id,

    try_to_date(DISPATCH_DATE) as dispatch_date,

    ACTUAL_DISTANCE_MILES::number as actual_distance_miles,
    ACTUAL_DURATION_HOURS::number as actual_duration_hours,
    FUEL_GALLONS_USED::number as fuel_gallons_used,
    AVERAGE_MPG::number as average_mpg,
    IDLE_TIME_HOURS::number as idle_time_hours,

    TRIP_STATUS::string as trip_status
from source
