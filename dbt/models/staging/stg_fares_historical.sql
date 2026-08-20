-- Staging view over raw DOT DB1B fare data.
-- Casts types, standardises route direction (always origin < dest alphabetically),
-- and filters out obvious bad rows before anything downstream touches the data.

with source as (
    select * from {{ source('raw', 'fares_historical') }}
),

cleaned as (
    select
        year,
        quarter,
        -- Normalise route so LAX→JFK and JFK→LAX are the same route key
        least(origin, dest)    as route_origin,
        greatest(origin, dest) as route_dest,
        airline_id,
        carrier,
        cast(mkt_fare_usd as numeric)    as fare_usd,
        cast(passengers as integer)      as passengers,
        cast(distance_miles as integer)  as distance_miles,
        loaded_at
    from source
    where
        mkt_fare_usd > 0
        and passengers > 0
        and origin is not null
        and dest    is not null
        and origin != dest
)

select
    {{ dbt_utils.generate_surrogate_key(['year', 'quarter', 'route_origin', 'route_dest', 'carrier']) }} as fare_id,
    *
from cleaned
