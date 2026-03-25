"""
mock_feed_generator.py
Generates realistic marketplace listing/price data.

Sources:  eBay, Amazon, StockX, Facebook Marketplace, GOAT, Mercari
Categories: Sneakers, Trading Cards, Electronics, Furniture, Watches,
            Handbags, Vintage Clothing, Video Games, Collectibles, Cameras

Usage:
    python ingestion/mock_feed_generator.py --days 90 --records 400
"""

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

_CITIES  = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
            "San Antonio", "San Diego", "Dallas", "San Jose", "Austin",
            "Seattle", "Denver", "Miami", "Atlanta", "Boston"]
_STATES  = ["NY", "CA", "IL", "TX", "AZ", "PA", "FL", "WA", "CO", "GA", "NC", "MI"]
_NAMES   = ["swift_trader", "deal_hunter", "sneakerhead", "card_king", "tech_flips",
            "vintage_vibe", "rare_finds", "quick_sell", "market_pro", "best_bargains",
            "furniture_flips", "luxury_resell", "retro_gamer", "lens_lab", "watch_works"]

class _Fake:
    def city(self):       return random.choice(_CITIES)
    def state_abbr(self): return random.choice(_STATES)
    def user_name(self):  return f"{random.choice(_NAMES)}_{random.randint(100,9999)}"

fake = _Fake()

# ─────────────────────────────────────────────────────────────────────────────
# Source profiles — each platform has its own category strengths & fee model
# ─────────────────────────────────────────────────────────────────────────────
SOURCES = {
    "ebay": {
        "display": "eBay",
        "url": "https://www.ebay.com/itm",
        "fee_pct": 0.1295,
        "strong_categories": ["electronics", "trading_cards", "collectibles", "video_games", "cameras"],
        "has_auction": True,
        "ships_internationally": True,
    },
    "amazon": {
        "display": "Amazon",
        "url": "https://www.amazon.com/dp",
        "fee_pct": 0.15,
        "strong_categories": ["electronics", "cameras", "video_games"],
        "has_auction": False,
        "ships_internationally": False,
        "price_premium": 0.05,   # Amazon listings tend slightly higher
    },
    "stockx": {
        "display": "StockX",
        "url": "https://stockx.com/product",
        "fee_pct": 0.095,
        "strong_categories": ["sneakers", "watches", "handbags", "trading_cards"],
        "has_auction": False,
        "ships_internationally": True,
        "authentication": True,   # StockX authenticates all items
        "price_premium": 0.12,
    },
    "facebook_marketplace": {
        "display": "Facebook Marketplace",
        "url": "https://www.facebook.com/marketplace/item",
        "fee_pct": 0.05,
        "strong_categories": ["furniture", "electronics", "vintage_clothing", "collectibles"],
        "has_auction": False,
        "ships_internationally": False,
        "local_only": True,
        "price_discount": 0.15,  # FB tends cheaper — no auth, local pickup
    },
    "goat": {
        "display": "GOAT",
        "url": "https://www.goat.com/sneakers",
        "fee_pct": 0.098,
        "strong_categories": ["sneakers", "handbags", "watches"],
        "has_auction": False,
        "ships_internationally": True,
        "authentication": True,
        "price_premium": 0.08,
    },
    "mercari": {
        "display": "Mercari",
        "url": "https://www.mercari.com/us/item",
        "fee_pct": 0.10,
        "strong_categories": ["vintage_clothing", "trading_cards", "collectibles",
                               "video_games", "handbags", "cameras"],
        "has_auction": False,
        "ships_internationally": False,
        "price_discount": 0.08,
    },
}

SOURCE_NAMES = list(SOURCES.keys())

# ─────────────────────────────────────────────────────────────────────────────
# Category catalog — 10 categories, each with realistic price dynamics
# ─────────────────────────────────────────────────────────────────────────────
CATEGORIES = {
    "sneakers": {
        "display": "Sneakers",
        "icon": "👟",
        "brands": ["Nike", "Adidas", "New Balance", "Jordan", "Yeezy", "Asics", "Salehe Bembury", "New Balance Collab"],
        "models": ["Air Max 90", "Dunk Low", "Ultraboost", "990v5", "Retro 1 High OG", "350 V2 Zebra",
                   "Gel-Kayano 14", "Gel-Nimbus 9", "574 New Balance", "Dunk High Pro"],
        "base_price_range": (85, 650),
        "price_volatility": 0.18,
        "size_relevant": True,
        "best_sources": ["stockx", "goat", "ebay"],
    },
    "trading_cards": {
        "display": "Trading Cards",
        "icon": "🃏",
        "brands": ["Pokemon", "Magic: The Gathering", "NBA Topps", "NFL Panini", "One Piece", "Dragon Ball Z"],
        "models": ["Charizard Base Set", "Black Lotus Alpha", "Rookie Card PSA 10", "Holographic Rare",
                   "Booster Box Sealed", "Limited Collector Edition", "1st Edition", "Graded BGS 9.5"],
        "base_price_range": (5, 5000),
        "price_volatility": 0.40,   # highest volatility category
        "size_relevant": False,
        "best_sources": ["ebay", "mercari", "stockx"],
    },
    "electronics": {
        "display": "Electronics",
        "icon": "💻",
        "brands": ["Apple", "Samsung", "Sony", "Bose", "LG", "Dell", "Dyson", "Meta"],
        "models": ["iPhone 15 Pro", "Galaxy S24 Ultra", "WH-1000XM5", "QuietComfort 45",
                   "MacBook Pro M3", "OLED C3 TV", "AirPods Pro 2", "Quest 3"],
        "base_price_range": (80, 2200),
        "price_volatility": 0.09,
        "size_relevant": False,
        "best_sources": ["ebay", "amazon", "facebook_marketplace"],
    },
    "furniture": {
        "display": "Furniture",
        "icon": "🛋️",
        "brands": ["IKEA", "West Elm", "CB2", "Restoration Hardware", "Herman Miller", "Knoll", "Eames"],
        "models": ["Eames Lounge Chair", "Aeron Chair", "Sectional Sofa", "Mid-Century Dresser",
                   "Dining Table Walnut", "Barcelona Chair", "Noguchi Coffee Table", "Malm Bed Frame"],
        "base_price_range": (50, 8000),
        "price_volatility": 0.22,
        "size_relevant": False,
        "best_sources": ["facebook_marketplace", "ebay", "mercari"],
        "heavy_shipping": True,
    },
    "watches": {
        "display": "Watches",
        "icon": "⌚",
        "brands": ["Rolex", "Omega", "Seiko", "TAG Heuer", "Patek Philippe", "AP", "Grand Seiko"],
        "models": ["Submariner Date", "Speedmaster Moonwatch", "SARB033", "Carrera Chronograph",
                   "Nautilus 5711", "Royal Oak 15500", "Snowflake SBGA211", "Datejust 41"],
        "base_price_range": (200, 85000),
        "price_volatility": 0.14,
        "size_relevant": False,
        "best_sources": ["stockx", "ebay", "goat"],
        "authentication_critical": True,
    },
    "handbags": {
        "display": "Handbags",
        "icon": "👜",
        "brands": ["Louis Vuitton", "Chanel", "Gucci", "Prada", "Hermès", "Bottega Veneta", "Celine"],
        "models": ["Neverfull MM", "Classic Flap Medium", "Dionysus GG", "Re-Edition 2000",
                   "Birkin 30", "Cassette Intrecciato", "16 Bag Small", "Speedy 30"],
        "base_price_range": (400, 45000),
        "price_volatility": 0.12,
        "size_relevant": False,
        "best_sources": ["stockx", "goat", "ebay", "mercari"],
        "authentication_critical": True,
    },
    "vintage_clothing": {
        "display": "Vintage Clothing",
        "icon": "👕",
        "brands": ["Vintage Levi's", "Supreme", "Stüssy", "Carhartt WIP", "Polo Ralph Lauren",
                   "Tommy Hilfiger", "Band Tee", "Vintage Nike"],
        "models": ["501 Denim Jeans", "Box Logo Hoodie", "World Tour Tee", "OG Boxy T-Shirt",
                   "1990s Windbreaker", "Fleece Jacket", "Varsity Jacket", "Graphic Tee 90s"],
        "base_price_range": (15, 1200),
        "price_volatility": 0.28,
        "size_relevant": True,
        "best_sources": ["mercari", "ebay", "facebook_marketplace"],
    },
    "video_games": {
        "display": "Video Games",
        "icon": "🎮",
        "brands": ["Nintendo", "Sony", "Microsoft", "Sega", "Atari", "Limited Run Games"],
        "models": ["PS5 Console", "Nintendo Switch OLED", "Xbox Series X", "Pokemon Red CIB",
                   "Zelda Tears of the Kingdom", "Chrono Trigger SNES", "Metal Gear Solid PS1", "NES Classic Edition"],
        "base_price_range": (15, 1800),
        "price_volatility": 0.20,
        "size_relevant": False,
        "best_sources": ["ebay", "mercari", "amazon"],
    },
    "collectibles": {
        "display": "Collectibles",
        "icon": "🏆",
        "brands": ["Funko Pop", "LEGO", "Hot Wheels", "Bearbrick", "KAWS", "Medicom Toy"],
        "models": ["KAWS Companion BFF", "Bearbrick 1000%", "LEGO UCS Millennium Falcon",
                   "Hot Wheels Redline", "Funko Exclusive Chase", "Air Jordan 1 Diorama",
                   "Mickey Mouse Original", "Vintage Action Figure MOC"],
        "base_price_range": (12, 12000),
        "price_volatility": 0.32,
        "size_relevant": False,
        "best_sources": ["ebay", "mercari", "stockx"],
    },
    "cameras": {
        "display": "Cameras & Lenses",
        "icon": "📷",
        "brands": ["Sony", "Canon", "Nikon", "Fujifilm", "Leica", "Hasselblad", "Olympus"],
        "models": ["A7 IV", "R5 Mark II", "Z8", "X-T5", "M11", "X2D 100C",
                   "85mm f/1.4 GM", "50mm f/1.2L", "Nikkor Z 35mm", "Fujinon 56mm"],
        "base_price_range": (200, 9500),
        "price_volatility": 0.11,
        "size_relevant": False,
        "best_sources": ["ebay", "amazon", "mercari"],
    },
}

CATEGORY_NAMES = list(CATEGORIES.keys())
CONDITIONS      = ["new", "like_new", "good", "fair", "poor"]
COND_WEIGHTS    = [0.20, 0.35, 0.28, 0.12, 0.05]


def _source_weight_for_category(category: str) -> list[float]:
    """Give higher weight to sources that are strong in this category."""
    weights = []
    for src_name, src in SOURCES.items():
        if category in src.get("strong_categories", []):
            weights.append(3.0)
        else:
            weights.append(1.0)
    return weights


def generate_listing(listing_id: str, category: str, event_date: datetime) -> dict:
    cat    = CATEGORIES[category]
    brand  = random.choice(cat["brands"])
    model  = random.choice(cat["models"])
    lo, hi = cat["base_price_range"]
    base_price = random.uniform(lo, hi)

    # Source — weighted toward the category's natural platforms
    src_weights = _source_weight_for_category(category)
    source_key  = random.choices(SOURCE_NAMES, weights=src_weights, k=1)[0]
    source_info = SOURCES[source_key]

    # Price adjustments per source
    price = base_price
    if "price_premium" in source_info:
        price *= (1 + source_info["price_premium"])
    if "price_discount" in source_info:
        price *= (1 - source_info["price_discount"])

    # Anomaly spike ~4% of time
    if random.random() < 0.04:
        price *= random.uniform(1.5, 3.0)
    else:
        noise = random.gauss(0, cat["price_volatility"])
        price *= (1 + noise)

    price = round(max(price, 1.0), 2)

    condition = random.choices(CONDITIONS, weights=COND_WEIGHTS)[0]

    # Source-specific shipping logic
    if source_info.get("local_only"):
        shipping = 0.0
    elif cat.get("heavy_shipping"):
        shipping = round(random.choice([0, 49, 79, 99, 149]), 2)
    else:
        shipping = round(random.choice([0, 4.99, 7.99, 12.99, 15.99, 19.99]), 2)

    # Auction only on platforms that support it
    is_auction = source_info.get("has_auction", False) and random.random() < 0.25
    num_bids   = random.randint(1, 32) if is_auction else 0

    # Title construction
    if cat.get("size_relevant") and category == "sneakers":
        size_str = f"Size {random.choice([7, 7.5, 8, 8.5, 9, 9.5, 10, 10.5, 11, 12])}"
        title = f"{brand} {model} {size_str} {condition.replace('_', ' ').title()}"
    elif cat.get("size_relevant"):
        size_str = random.choice(["XS", "S", "M", "L", "XL", "XXL"])
        title = f"Vintage {brand} {model} Size {size_str}"
    else:
        title = f"{brand} {model} — {condition.replace('_', ' ').title()}"

    # Platform fee effect on net seller revenue
    platform_fee = round(price * source_info["fee_pct"], 2)
    net_seller   = round(price - platform_fee, 2)

    return {
        "listing_id":           listing_id,
        "source":               source_key,
        "source_display":       source_info["display"],
        "category":             category,
        "category_display":     cat["display"],
        "brand":                brand,
        "model":                model,
        "title":                title,
        "condition":            condition,
        "price":                price,
        "original_price":       round(base_price, 2),
        "currency":             "USD",
        "shipping_cost":        shipping,
        "total_cost":           round(price + shipping, 2),
        "platform_fee":         platform_fee,
        "net_seller_revenue":   net_seller,
        "seller_id":            f"seller_{fake.user_name()}",
        "seller_rating":        round(random.uniform(3.2, 5.0), 1),
        "num_seller_reviews":   random.randint(0, 8000),
        "location_city":        fake.city(),
        "location_state":       fake.state_abbr(),
        "is_auction":           is_auction,
        "num_bids":             num_bids,
        "days_listed":          random.randint(0, 45),
        "is_sold":              random.random() < 0.38,
        "sold_price":           None,
        "is_authenticated":     source_info.get("authentication", False),
        "has_local_pickup":     source_info.get("local_only", False),
        "ships_international":  source_info.get("ships_internationally", False),
        "listing_url":          f"{source_info['url']}/{listing_id}",
        "created_at":           event_date.isoformat(),
        "updated_at":           (event_date + timedelta(hours=random.randint(0, 18))).isoformat(),
        "ingested_at":          datetime.utcnow().isoformat(),
        "batch_id":             f"batch_{event_date.strftime('%Y%m%d')}",
    }


def introduce_dq_issues(df: pd.DataFrame, rate: float = 0.015) -> pd.DataFrame:
    n = len(df)
    k = int(n * rate)
    df.loc[df.sample(k).index, "price"] = None
    dupes = df.sample(min(k, 60)).copy()
    df = pd.concat([df, dupes], ignore_index=True)
    df.loc[df.sample(min(k, 25)).index, "price"] = \
        -df["price"].dropna().sample(min(k, 25)).values
    future = datetime.utcnow() + timedelta(days=30)
    df.loc[df.sample(min(k, 12)).index, "created_at"] = future.isoformat()
    return df


def generate_dataset(days=90, records_per_day=400, output_dir="mock_data", inject_issues=True):
    out = Path(output_dir)
    out.mkdir(exist_ok=True)

    end_date   = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=days)
    all_rows   = []

    print(f"Generating ~{days * records_per_day:,} listings "
          f"({start_date.date()} → {end_date.date()})...")
    print(f"  Sources:    {', '.join(SOURCES.keys())}")
    print(f"  Categories: {', '.join(CATEGORIES.keys())}")

    for day_offset in range(days):
        event_date   = start_date + timedelta(days=day_offset)
        daily_count  = int(records_per_day * random.uniform(0.75, 1.25))

        for _ in range(daily_count):
            cat     = random.choice(CATEGORY_NAMES)
            listing = generate_listing(str(uuid.uuid4()), cat, event_date)
            if listing["is_sold"]:
                listing["sold_price"] = round(listing["price"] * random.uniform(0.82, 1.08), 2)
            all_rows.append(listing)

    df = pd.DataFrame(all_rows)

    if inject_issues:
        print("  Injecting DQ issues for pipeline testing...")
        df = introduce_dq_issues(df)

    df["created_at"]    = pd.to_datetime(df["created_at"], format="ISO8601")
    df["date_partition"] = df["created_at"].dt.strftime("%Y-%m-%d")

    for date_str, grp in df.groupby("date_partition"):
        d = out / date_str
        d.mkdir(exist_ok=True)
        grp.drop(columns=["date_partition"]).to_csv(d / "listings.csv", index=False)

    combined = out / "all_listings.csv"
    df.drop(columns=["date_partition"]).to_csv(combined, index=False)

    meta = {
        "generated_at":  datetime.utcnow().isoformat(),
        "total_records": len(df),
        "date_range":    {"start": str(start_date.date()), "end": str(end_date.date())},
        "categories":    {k: v["display"] for k, v in CATEGORIES.items()},
        "sources":       {k: v["display"] for k, v in SOURCES.items()},
    }
    (out / "metadata.json").write_text(json.dumps(meta, indent=2))

    print(f"\n✅ Generated {len(df):,} records → {out}/")
    for cat_key, cat_val in CATEGORIES.items():
        n = (df["category"] == cat_key).sum()
        print(f"   {cat_val['icon']} {cat_val['display']:20s}: {n:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days",      type=int,  default=90)
    parser.add_argument("--records",   type=int,  default=400)
    parser.add_argument("--output",    type=str,  default="mock_data")
    parser.add_argument("--no-issues", action="store_true")
    args = parser.parse_args()

    generate_dataset(
        days=args.days,
        records_per_day=args.records,
        output_dir=args.output,
        inject_issues=not args.no_issues,
    )
