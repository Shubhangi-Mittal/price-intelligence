-- models/marts/mart_price_anomalies.sql
-- Price anomaly tracking — all 6 sources, 10 categories.
-- Used for alerting, investigation, and ML training data.
--
-- v2 additions: source_display, category_display, category_volatility_tier,
-- authentication status, platform fee context.

{{
  config(
    materialized='table',
    tags=['marts', 'anomalies'],
    description='Price anomalies with severity scoring — 6 sources × 10 categories'
  )
}}

with enriched as (

    select * from {{ ref('int_listings_enriched') }}
    where is_price_anomaly = true

),

anomaly_context as (

    select
        listing_id,
        source,
        source_display,
        category,
        category_display,
        brand,
        title,
        condition,
        listing_date,

        -- ── Prices ──────────────────────────────────────────────────────────
        price_usd,
        total_cost_usd,
        platform_fee_usd,
        net_seller_revenue_usd,
        rolling_7d_median_price                             as market_median_price,
        rolling_7d_avg_price                                as market_avg_price,
        rolling_7d_p10_price,
        rolling_7d_p90_price,

        -- ── Anomaly metrics ──────────────────────────────────────────────────
        price_z_score,
        price_vs_median_pct,
        price_vs_median_usd,

        -- ── Anomaly type ─────────────────────────────────────────────────────
        case
            when price_usd > rolling_7d_median_price       then 'price_spike'
            else                                                 'price_drop'
        end                                                 as anomaly_type,

        -- ── Severity ─────────────────────────────────────────────────────────
        case
            when price_z_score > 5
              or abs(price_vs_median_pct) > 0.75           then 'critical'
            when price_z_score > 3
              or abs(price_vs_median_pct) > 0.50           then 'high'
            when price_z_score > 2
              or abs(price_vs_median_pct) > 0.30           then 'medium'
            else                                                 'low'
        end                                                 as anomaly_severity,

        -- ── Context: is this a naturally volatile category? ────────────────
        -- (trading_cards / collectibles have high legitimate variance)
        category_volatility_tier,
        case
            when category_volatility_tier in ('very_volatile', 'volatile')
            then true else false
        end                                                 as high_volatility_category,

        -- ── Authentication context ──────────────────────────────────────────
        is_authenticated,
        case
            when source in ('stockx', 'goat') then true else false
        end                                                 as authenticated_platform,

        -- ── Seller context ───────────────────────────────────────────────────
        seller_id,
        seller_tier,
        seller_rating,
        seller_sell_through_rate,

        -- ── Listing context ──────────────────────────────────────────────────
        is_sold,
        sold_price_usd,
        listing_url,
        image_url,
        is_auction,
        num_bids,

        ingested_at,
        batch_id,
        current_timestamp                                   as dbt_updated_at

    from enriched

)

select * from anomaly_context
order by listing_date desc, price_z_score desc
