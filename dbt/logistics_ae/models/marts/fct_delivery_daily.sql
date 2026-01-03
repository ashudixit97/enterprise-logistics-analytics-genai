with deliveries as (
    select *
    from {{ ref('stg_delivery_events') }}
    where lower(event_type) = 'delivery'
),

final as (
    select
        cast(actual_ts as date) as delivery_date,
        location_state,

        count(*) as deliveries,
        sum(case when on_time_flag = 'Y' then 1 else 0 end) as on_time_deliveries,
        on_time_deliveries / nullif(deliveries, 0) as on_time_rate,

        avg(detention_minutes) as avg_detention_minutes
    from deliveries
    group by 1, 2
)

select * from final
