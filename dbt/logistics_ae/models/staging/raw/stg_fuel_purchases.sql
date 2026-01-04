with source as (
    select * from {{ source('raw', 'FUEL_PURCHASES_RAW') }}
)

select
    FUEL_PURCHASE_ID::string as fuel_purchase_id,
    TRIP_ID::string          as trip_id,
    TRUCK_ID::string         as truck_id,
    DRIVER_ID::string        as driver_id,

    try_to_date(PURCHASE_DATE) as purchase_date,

    LOCATION_CITY::string    as location_city,
    LOCATION_STATE::string   as location_state,

    GALLONS::number          as gallons,
    PRICE_PER_GALLON::number as price_per_gallon,
    TOTAL_COST::number       as total_cost
from source
