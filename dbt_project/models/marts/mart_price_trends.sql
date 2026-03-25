-- models/marts/mart_price_trends.sql
-- Final analytics mart: daily price trends by category × source.
-- Powers the dashboard, reports, and alerting.
--
-- New in v2: platform fee breakdown, volatility tier, authentication rate,
-- local pickup rate, international shipping availability.

{{
  config(
    materialized='table',
    tags=['marts', 'price_trends'],
    description='Daily price trend summary — 6 sources × 10 categories. Primary dashboard table.',
    partition_by={
      "field": "listing_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["category", "source"]
  )
}}

with enriched as (

    select * from {{ ref('int_listings_enriched') }}

),

daily_agg as (

    select
        listing_date,
        category,
        category_display,
        source,
        source_display,

        -- ── Volume ───────────────────────────────────────────────────────────
        count(distinct listing_id)                                  as total_listings,
        count(distinct case when is_sold then listing_id end)       as sold_listings,
        count(distinct case when is_price_anomaly then listing_id end) as anomalous_listings,

        count(distinct case when is_authenticated then listing_id end) as authenticated_listings,
        count(distinct case when has_local_pickup  then listing_id end) as local_pickup_listings,
        count(distinct case when ships_international then listing_id end) as intl_ship_listings,

        -- ── Sell-through ────────────────────────────────────────────────────
        count(distinct case when is_sold then listing_id end)::float
            / nullif(count(distinct listing_id), 0)                 as sell_through_rate,

        -- ── Price stats ──────────────────────────────────────────────────────
        round(avg(price_usd),    2)                                 as avg_price,
        round(min(price_usd),    2)                                 as min_price,
        round(max(price_usd),    2)                                 as max_price,
        round(stddev(price_usd), 2)                                 as price_stddev,

        -- Percentiles
        round(percentile_cont(0.05) within group (order by price_usd), 2) as p5_price,
        round(percentile_cont(0.25) within group (order by price_usd), 2) as p25_price,
        round(percentile_cont(0.50) within group (order by price_usd), 2) as median_price,
        round(percentile_cont(0.75) within group (order by price_usd), 2) as p75_price,
        round(percentile_cont(0.95) within group (order by price_usd), 2) as p95_price,

        -- Sold price
        round(avg(sold_price_usd), 2)                               as avg_sold_price,
        round(percentile_cont(0.50) within group (order by sold_price_usd), 2) as median_sold_price,

        -- ── Platform fee metrics (new) ────────────────────────────────────
        round(avg(platform_fee_usd),  2)                            as avg_platform_fee,
        round(avg(source_avg_fee_pct) * 100, 2)                    as avg_fee_pct,
        round(avg(net_seller_revenue_usd), 2)                       as avg_net_seller,

        -- Total cost (price + shipping) — what buyer actually pays
        round(avg(total_cost_usd), 2)                               as avg_total_cost,
        round(avg(shipping_cost_usd), 2)                            as avg_shipping_cost,

        -- ── Deal distribution ────────────────────────────────────────────────
        count(case when deal_score = 'great_deal'  then 1 end)      as great_deal_count,
        count(case when deal_score = 'good_deal'   then 1 end)      as good_deal_count,
        count(case when deal_score = 'fair_price'  then 1 end)      as fair_price_count,
        count(case when deal_score = 'overpriced'  then 1 end)      as overpriced_count,

        -- ── Condition mix ────────────────────────────────────────────────────
        count(case when condition = 'new'       then 1 end)         as new_count,
        count(case when condition = 'like_new'  then 1 end)         as like_new_count,
        count(case when condition = 'good'      then 1 end)         as good_count,
        count(case when condition = 'fair'      then 1 end)         as fair_count,
        count(case when condition = 'poor'      then 1 end)         as poor_count,

        -- ── Auction ─────────────────────────────────────────────────────────
        count(case when is_auction         then 1 end)              as auction_count,
        round(avg(case when is_auction     then price_usd end), 2)  as avg_auction_price,
        round(avg(case when not is_auction then price_usd end), 2)  as avg_fixed_price,

        -- ── Volatility ──────────────────────────────────────────────────────
        max(category_volatility_tier)                               as volatility_tier,
        round(avg(category_price_cv), 4)                            as avg_price_cv,

        -- ── Seller quality ───────────────────────────────────────────────────
        count(case when seller_tier = 'power_seller'   then 1 end)  as power_seller_count,
        count(case when seller_tier = 'trusted_seller' then 1 end)  as trusted_seller_count,
        round(avg(seller_sell_through_rate), 3)                     as avg_seller_sell_through

    from enriched
    group by 1, 2, 3, 4, 5

),

with_wow as (

    select
        *,

        -- Week-over-week change
        lag(median_price,    7) over (partition by category, source order by listing_date)
                                                                    as median_price_1w_ago,
        lag(total_listings,  7) over (partition by category, source order by listing_date)
                                                                    as total_listings_1w_ago,

        -- Rolling averages
        avg(median_price) over (
            partition by category, source
            order by listing_date
            rows between 6 preceding and current row
        )                                                           as rolling_7d_avg_median_price,

        avg(median_price) over (
            partition by category, source
            order by listing_date
            rows between 29 preceding and current row
        )                                                           as rolling_30d_avg_median_price

    from daily_agg

),

final as (

    select
        *,

        -- WoW price change
        case
            when median_price_1w_ago > 0
            then round((median_price - median_price_1w_ago) / median_price_1w_ago, 4)
        end                                                         as price_wow_change_pct,

        -- WoW volume change
        case
            when total_listings_1w_ago > 0
            then round((total_listings - total_listings_1w_ago)::float / total_listings_1w_ago, 4)
        end                                                         as volume_wow_change_pct,

        -- Price trend signal
        case
            when rolling_7d_avg_median_price > rolling_30d_avg_median_price * 1.05 then 'trending_up'
            when rolling_7d_avg_median_price < rolling_30d_avg_median_price * 0.95 then 'trending_down'
            else                                                                         'stable'
        end                                                         as price_trend,

        -- Authentication rate
        round(authenticated_listings::float / nullif(total_listings, 0) * 100, 1)
                                                                    as authentication_rate_pct,

        current_timestamp                                           as dbt_updated_at

    from with_wow

)

select * from final
