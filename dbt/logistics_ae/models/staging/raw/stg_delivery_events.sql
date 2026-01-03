with source as (
    select * from {{ source('raw', 'DELIVERY_EVENTS_RAW') }}
),

typed as (
    select
        EVENT_ID::string                    as event_id,
        LOAD_ID::string                     as load_id,
        TRIP_ID::string                     as trip_id,
        EVENT_TYPE::string                  as event_type,
        FACILITY_ID::string                 as facility_id,

        try_to_timestamp_ntz(SCHEDULED_DATETIME) as scheduled_ts,
        try_to_timestamp_ntz(ACTUAL_DATETIME)    as actual_ts,

        DETENTION_MINUTES::number           as detention_minutes,
        ON_TIME_FLAG::string                as on_time_flag,

        LOCATION_CITY::string               as location_city,
        LOCATION_STATE::string              as location_state,

        current_timestamp()                 as _loaded_at
    from source
)

select * from typed
