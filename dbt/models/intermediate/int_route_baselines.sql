-- Route-level fare baselines from DOT historical data.
--
-- Buckets fares by quarter (as a seasonal proxy) and computes the
-- median and p25/p75 fare per route. Median is more robust than mean
-- for fare data because outlier business/first-class fares skew the mean.
--
-- Grain: one row per (route_origin, route_dest, quarter_bucket)

with fares as (
    select * from {{ ref('stg_fares_historical') }}
),

-- Aggregate baseline stats per route + quarter
aggregated as (
    select
        route_origin,
        route_dest,
        quarter as quarter_bucket,
        approx_quantiles(fare_usd, 100)[offset(25)] as p25_fare_usd,
        approx_quantiles(fare_usd, 100)[offset(50)] as median_fare_usd,
        approx_quantiles(fare_usd, 100)[offset(75)] as p75_fare_usd,
        count(*) as sample_size,
        sum(passengers) as total_passengers,
        min(year) as earliest_year,
        max(year) as latest_year
    from fares
    group by route_origin, route_dest, quarter
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['route_origin', 'route_dest', 'quarter_bucket']) }} as baseline_id,
    route_origin,
    route_dest,
    quarter_bucket,
    p25_fare_usd,
    median_fare_usd,
    p75_fare_usd,
    sample_size,
    total_passengers,
    earliest_year,
    latest_year
from aggregated
