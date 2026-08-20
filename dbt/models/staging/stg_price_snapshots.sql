-- Staging view over raw Amadeus price snapshots.
-- One row per (polled_at, route, carrier, cabin_class).

with source as (
    select * from {{ source('raw', 'price_snapshots') }}
),

cleaned as (
    select
        snapshot_id,
        polled_at,
        least(origin, dest)    as route_origin,
        greatest(origin, dest) as route_dest,
        carrier,
        cabin_class,
        cast(price_usd as numeric)           as price_usd,
        cast(seats_remaining as integer)     as seats_remaining,
        departure_date,
        cast(days_to_departure as integer)   as days_to_departure
    from source
    where
        price_usd > 0
        and origin is not null
        and dest   is not null
        and origin != dest
)

select * from cleaned
