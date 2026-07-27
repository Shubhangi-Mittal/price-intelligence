-- models/intermediate/int_listings_enriched.sql
-- Enrich staged listings with:
--   1. 7-day rolling price benchmarks per category × source
--   2. Z-score anomaly detection
--   3. Deal scoring (great_deal → overpriced)
--   4. Seller quality tiers
--   5. Platform fee impact analysis (new — covers all 6 sources)
--   6. Category-level price volatility flags
--
-- Supports all 6 sources and 10 categories.

{{
  config(
    materialized='table',
    tags=['intermediate', 'pricing'],
    description='Enriched listings: benchmarks, anomaly flags, seller tiers, fee analysis — 6 sources × 10 categories',
    partition_by={
      "field": "listing_date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

with staged as (

    select * from {{ ref('stg_listings') }}

),

-- ── Rolling 7-day price stats per category + source ────────────────────────
-- The benchmark grain is (category, source, day) — NOT the listing.
--
-- Windowing the row-level feed with `rows between 6 preceding and current row`
-- does not mean "the last 7 days": ROWS counts rows, so with thousands of
-- listings per day it means "the last 7 listings". Ordering by day alone also
-- leaves rows within a day in arbitrary order, so which 7 rows you get is
-- non-deterministic.
--
-- Instead, collect the distinct (category, source, day) keys and join every
-- listing that falls in that key's trailing 7-day span. Aggregating over the
-- joined rows gives exact trailing statistics, keeps the window gap-aware (a
-- day with no listings drops out rather than shifting the window), and uses
-- only plain aggregates, so no warehouse-specific windowed-percentile syntax
-- is required.
daily_prices as (

    select
        category,
        source,
        cast(listing_created_at as date)                    as listing_date,
        price_usd

    from staged

),

benchmark_days as (

    select distinct category, source, listing_date
    from daily_prices

),

price_benchmarks as (

    select
        d.category,
        d.source,
        d.listing_date,

        count(*)                                            as rolling_7d_listing_count,
        avg(w.price_usd)                                    as rolling_7d_avg_price,
        stddev_samp(w.price_usd)                            as rolling_7d_stddev_price,

        percentile_cont(0.10) within group (order by w.price_usd) as rolling_7d_p10_price,
        percentile_cont(0.50) within group (order by w.price_usd) as rolling_7d_median_price,
        percentile_cont(0.90) within group (order by w.price_usd) as rolling_7d_p90_price

    from benchmark_days d
    join daily_prices w
        on  w.category = d.category
        and w.source   = d.source
        and w.listing_date >  d.listing_date - interval 7 day
        and w.listing_date <= d.listing_date

    group by 1, 2, 3

),

-- ── Category-level volatility (all sources combined) ──────────────────────
category_volatility as (

    select
        category,
        date_trunc('day', listing_created_at)   as vol_date,
        stddev(price_usd) / nullif(avg(price_usd), 0) as price_cv  -- coefficient of variation

    from staged
    group by 1, 2

),

-- ── Platform fee reference table ───────────────────────────────────────────
-- Used to compare effective cost to buyer and net to seller across platforms
platform_fees as (

    select source, avg(platform_fee_usd) as avg_fee_usd,
           avg(platform_fee_usd / nullif(price_usd, 0)) as avg_fee_pct
    from staged
    group by 1

),

-- ── Seller quality tiers ───────────────────────────────────────────────────
seller_stats as (

    select
        seller_id,
        count(distinct listing_id)                          as total_listings,
        avg(price_usd)                                      as avg_seller_price,
        avg(seller_rating)                                  as avg_seller_rating,
        sum(case when is_sold then 1 else 0 end)            as sold_count,
        sum(case when is_sold then 1 else 0 end)::float
            / nullif(count(*), 0)                           as sell_through_rate

    from staged
    group by 1

),

enriched as (

    select
        s.*,

        -- ── Date dimensions ─────────────────────────────────────────────────
        cast(s.listing_created_at as date)                  as listing_date,
        extract(year  from s.listing_created_at)            as listing_year,
        extract(month from s.listing_created_at)            as listing_month,
        extract(dow   from s.listing_created_at)            as listing_day_of_week,
        -- to_char is Postgres/Snowflake only (DuckDB has strftime, BigQuery has
        -- format_timestamp). extract + lpad is the same result everywhere.
        cast(extract(year from s.listing_created_at) as varchar)
            || '-'
            || lpad(cast(extract(month from s.listing_created_at) as varchar), 2, '0')
                                                            as year_month,

        -- ── Price benchmarks ────────────────────────────────────────────────
        pb.rolling_7d_listing_count,
        pb.rolling_7d_avg_price,
        pb.rolling_7d_stddev_price,
        pb.rolling_7d_median_price,
        pb.rolling_7d_p10_price,
        pb.rolling_7d_p90_price,

        -- ── Price vs market ─────────────────────────────────────────────────
        s.price_usd - pb.rolling_7d_median_price           as price_vs_median_usd,
        case
            when pb.rolling_7d_median_price > 0
            then (s.price_usd - pb.rolling_7d_median_price)
                 / pb.rolling_7d_median_price
        end                                                 as price_vs_median_pct,

        -- ── Deal scoring ────────────────────────────────────────────────────
        case
            when s.price_usd < pb.rolling_7d_p10_price     then 'great_deal'
            when s.price_usd < pb.rolling_7d_median_price  then 'good_deal'
            when s.price_usd < pb.rolling_7d_p90_price     then 'fair_price'
            else                                                 'overpriced'
        end                                                 as deal_score,

        -- ── Anomaly detection (z-score) ──────────────────────────────────
        case
            when pb.rolling_7d_stddev_price > 0
            then abs(s.price_usd - pb.rolling_7d_avg_price)
                 / pb.rolling_7d_stddev_price
            else 0
        end                                                 as price_z_score,

        case
            when pb.rolling_7d_stddev_price > 0
              and abs(s.price_usd - pb.rolling_7d_avg_price)
                  / pb.rolling_7d_stddev_price > 3          then true
            when pb.rolling_7d_median_price > 0
              and abs(s.price_usd - pb.rolling_7d_median_price)
                  / pb.rolling_7d_median_price
                  > {{ var('price_anomaly_threshold') }}     then true
            else false
        end                                                 as is_price_anomaly,

        -- ── Discount from original ──────────────────────────────────────────
        case
            when s.original_price_usd > 0
            then (s.original_price_usd - s.price_usd) / s.original_price_usd
        end                                                 as discount_pct,

        -- ── Platform fee analysis ────────────────────────────────────────────
        pf.avg_fee_pct                                      as source_avg_fee_pct,
        s.price_usd * (1 - coalesce(pf.avg_fee_pct, 0.10)) as estimated_net_seller,

        -- ── Category volatility ─────────────────────────────────────────────
        cv.price_cv                                         as category_price_cv,
        case
            when cv.price_cv > 0.50 then 'very_volatile'
            when cv.price_cv > 0.25 then 'volatile'
            when cv.price_cv > 0.10 then 'moderate'
            else                         'stable'
        end                                                 as category_volatility_tier,

        -- ── Seller quality ───────────────────────────────────────────────────
        case
            when ss.avg_seller_rating >= 4.8
              and ss.total_listings   >= 100                then 'power_seller'
            when ss.avg_seller_rating >= 4.5
              and ss.total_listings   >= 20                 then 'trusted_seller'
            when ss.avg_seller_rating >= {{ var('min_seller_rating') }}
                                                            then 'standard_seller'
            else                                                 'new_or_flagged'
        end                                                 as seller_tier,

        ss.sell_through_rate                                as seller_sell_through_rate,
        ss.total_listings                                   as seller_total_listings

    from staged s
    left join price_benchmarks pb
        on  s.category = pb.category
        and s.source   = pb.source
        and cast(s.listing_created_at as date) = pb.listing_date
    left join seller_stats ss
        on  s.seller_id = ss.seller_id
    left join platform_fees pf
        on  s.source = pf.source
    left join category_volatility cv
        on  s.category  = cv.category
        and cast(s.listing_created_at as date) = cv.vol_date

)

select * from enriched
