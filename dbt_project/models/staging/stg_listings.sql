-- models/staging/stg_listings.sql
-- Stage raw marketplace listing data.
-- Responsibilities:
--   1. Rename columns to snake_case standard
--   2. Cast types explicitly
--   3. Apply basic filters (remove obviously invalid rows)
--   4. No business logic
--
-- Supports all 6 sources: ebay, amazon, stockx, facebook_marketplace, goat, mercari
-- Supports all 10 categories: sneakers, trading_cards, electronics, furniture,
--   watches, handbags, vintage_clothing, video_games, collectibles, cameras

{{
  config(
    materialized='view',
    tags=['staging', 'listings'],
    description='Staged marketplace listings: typed, renamed, deduplicated — 6 sources, 10 categories'
  )
}}

with raw_listings as (

    select * from {{ source('raw', 'listings') }}

),

renamed_and_cast as (

    select
        -- ── Identifiers ─────────────────────────────────────────────────────
        cast(listing_id        as varchar)                          as listing_id,
        lower(trim(source))                                         as source,
        lower(trim(coalesce(source_display, source)))               as source_display,

        -- ── Product dimensions ──────────────────────────────────────────────
        lower(trim(category))                                       as category,
        lower(trim(coalesce(category_display, category)))           as category_display,
        initcap(trim(brand))                                        as brand,
        trim(title)                                                 as title,
        lower(trim(condition))                                      as condition,

        -- ── Pricing ─────────────────────────────────────────────────────────
        cast(price              as numeric(14,2))                   as price_usd,
        cast(coalesce(original_price, price) as numeric(14,2))     as original_price_usd,
        cast(coalesce(shipping_cost, 0)      as numeric(10,2))     as shipping_cost_usd,
        cast(coalesce(total_cost, price + coalesce(shipping_cost,0)) as numeric(14,2))
                                                                    as total_cost_usd,
        cast(coalesce(platform_fee, 0)       as numeric(10,2))     as platform_fee_usd,
        cast(coalesce(net_seller_revenue, price) as numeric(14,2)) as net_seller_revenue_usd,
        upper(trim(coalesce(currency, 'USD')))                      as currency,

        -- ── Seller info ──────────────────────────────────────────────────────
        lower(trim(seller_id))                                      as seller_id,
        cast(seller_rating          as numeric(3,1))                as seller_rating,
        cast(coalesce(num_seller_reviews, 0) as integer)           as num_seller_reviews,

        -- ── Location ────────────────────────────────────────────────────────
        initcap(trim(location_city))                                as location_city,
        upper(trim(location_state))                                 as location_state,

        -- ── Listing mechanics ────────────────────────────────────────────────
        cast(coalesce(is_auction,      false) as boolean)           as is_auction,
        cast(coalesce(num_bids,        0)     as integer)           as num_bids,
        cast(coalesce(days_listed,     0)     as integer)           as days_listed,
        cast(coalesce(is_sold,         false) as boolean)           as is_sold,
        cast(sold_price                as numeric(14,2))            as sold_price_usd,

        -- ── Source-level flags ───────────────────────────────────────────────
        cast(coalesce(is_authenticated,   false) as boolean)        as is_authenticated,
        cast(coalesce(has_local_pickup,   false) as boolean)        as has_local_pickup,
        cast(coalesce(ships_international,false) as boolean)        as ships_international,

        -- ── URLs ─────────────────────────────────────────────────────────────
        listing_url,
        image_url,

        -- ── Timestamps ──────────────────────────────────────────────────────
        cast(created_at   as timestamp)                             as listing_created_at,
        cast(updated_at   as timestamp)                             as listing_updated_at,
        cast(ingested_at  as timestamp)                             as ingested_at,
        cast(batch_id     as varchar)                               as batch_id,

        -- ── Audit ────────────────────────────────────────────────────────────
        current_timestamp                                           as dbt_updated_at

    from raw_listings

),

deduplicated as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by listing_id
                order by ingested_at desc, listing_updated_at desc
            ) as row_num
        from renamed_and_cast
    )
    where row_num = 1

),

filtered as (

    select * from deduplicated
    where
        listing_id          is not null
        and price_usd        > 0
        and price_usd        < 5000000          -- covers high-end watches, handbags
        and source in (
            'ebay', 'amazon', 'stockx',
            'facebook_marketplace', 'goat', 'mercari'
        )
        and category in (
            'sneakers', 'trading_cards', 'electronics', 'furniture',
            'watches', 'handbags', 'vintage_clothing', 'video_games',
            'collectibles', 'cameras'
        )
        and listing_created_at <= current_timestamp
        and listing_created_at >= '2020-01-01'

)

select * from filtered
