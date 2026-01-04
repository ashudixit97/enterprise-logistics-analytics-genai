with loads as (
    select
        load_id,
        route_id,
        load_date,
        revenue
    from {{ ref('stg_loads') }}
),

trips as (
    select
        trip_id,
        load_id,
        dispatch_date,
        actual_distance_miles
    from {{ ref('stg_trips') }}
),

fuel as (
    -- sum fuel cost per trip per day
    select
        trip_id,
        purchase_date,
        sum(total_cost) as fuel_cost
    from {{ ref('stg_fuel_purchases') }}
    group by 1, 2
),

joined as (
    select
        coalesce(f.purchase_date, t.dispatch_date) as activity_date,
        l.route_id,

        l.revenue as revenue,
        t.actual_distance_miles as miles,

        -- fuel cost might be missing if trip has no purchases in dataset
        coalesce(f.fuel_cost, 0) as fuel_cost
    from trips t
    join loads l
      on t.load_id = l.load_id
    left join fuel f
      on t.trip_id = f.trip_id
),

final as (
    select
        activity_date,
        route_id,

        sum(revenue) as revenue,
        sum(miles) as miles,
        sum(fuel_cost) as fuel_cost,

        (sum(revenue) - sum(fuel_cost)) as profit,
        (sum(revenue) - sum(fuel_cost)) / nullif(sum(miles), 0) as profit_per_mile,
        sum(fuel_cost) / nullif(sum(miles), 0) as fuel_cost_per_mile
    from joined
    group by 1, 2
),

routes as (
    select
        route_id,
        origin_city,
        origin_state,
        destination_city,
        destination_state,
        typical_distance_miles
    from {{ ref('dim_routes') }}
)

select
    f.activity_date,
    f.route_id,

    r.origin_city,
    r.origin_state,
    r.destination_city,
    r.destination_state,
    r.typical_distance_miles,

    f.revenue,
    f.miles,
    f.fuel_cost,
    f.profit,
    f.profit_per_mile,
    f.fuel_cost_per_mile
from final f
left join routes r
  on f.route_id = r.route_id
