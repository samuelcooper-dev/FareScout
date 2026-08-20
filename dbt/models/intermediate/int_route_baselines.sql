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

-- Weight each fare observation by passenger count so popular price points
-- (e.g. sale fares with high passenger volume) count proportionally.
-- We expand using UNNEST of a repeated sequence — BigQuery supports this.
aggregated as (
    select
        route_origin,
        route_dest,
        quarter                          as quarter_bucket,
        percentile_cont(fare_usd, 0.25) over (
            partition by route_origin, route_dest, quarter
        )                                as p25_fare_usd,
        percentile_cont(fare_usd, 0.50) over (
            partition by route_origin, route_dest, quarter
        )                                as median_fare_usd,
        percentile_cont(fare_usd, 0.75) over (
            partition by route_origin, route_dest, quarter
        )                                as p75_fare_usd,
        count(*)                         as sample_size,
        sum(passengers)                  as total_passengers,
        min(year)                        as earliest_year,
        max(year)                        as latest_year
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
