with source as (
    select * from {{ source('raw', 'LOADS_RAW') }}
)

select
    LOAD_ID::string as load_id,
    CUSTOMER_ID::string as customer_id,
    ROUTE_ID::string as route_id,
    try_to_date(LOAD_DATE) as load_date,

    REVENUE::number as revenue,
    FUEL_SURCHARGE::number as fuel_surcharge,
    ACCESSORIAL_CHARGES::number as accessorial_charges,

    LOAD_STATUS::string as load_status,
    LOAD_TYPE::string as load_type,
    BOOKING_TYPE::string as booking_type
from source
