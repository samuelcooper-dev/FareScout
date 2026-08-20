-- Deal score mart.
--
-- Joins each live price snapshot against its route's historical baseline
-- for the matching quarter, then computes a deal score:
--
--   deal_score = (baseline_median - live_price) / baseline_median
--
-- Positive = cheaper than median (good deal). Negative = more expensive.
-- A score >= 0.15 (15%) triggers the price-drop event pipeline.
--
-- Grain: one row per price snapshot (snapshot_id)

with snapshots as (
    select * from {{ ref('stg_price_snapshots') }}
),

baselines as (
    select * from {{ ref('int_route_baselines') }}
),

-- Map each snapshot to its seasonal quarter for baseline lookup.
-- departure_date quarter is a better proxy than polled_at quarter because
-- we want to compare against fares for that travel season.
scored as (
    select
        s.snapshot_id,
        s.polled_at,
        s.route_origin,
        s.route_dest,
        s.carrier,
        s.cabin_class,
        s.price_usd,
        s.seats_remaining,
        s.departure_date,
        s.days_to_departure,
        extract(quarter from s.departure_date)  as departure_quarter,
        b.median_fare_usd                        as baseline_median_usd,
        b.p25_fare_usd                           as baseline_p25_usd,
        b.p75_fare_usd                           as baseline_p75_usd,
        b.sample_size                            as baseline_sample_size,
        -- Core deal score: fraction below median (positive = deal)
        safe_divide(b.median_fare_usd - s.price_usd, b.median_fare_usd) as deal_score,
        -- Convenience flag for the alert pipeline
        safe_divide(b.median_fare_usd - s.price_usd, b.median_fare_usd) >= 0.15 as is_deal
    from snapshots s
    left join baselines b
        on  s.route_origin = b.route_origin
        and s.route_dest   = b.route_dest
        and extract(quarter from s.departure_date) = b.quarter_bucket
)

select * from scored
