"""
scripts/build_dashboard_data.py
Reads mock_data/all_listings.csv and writes dashboard/data.json
with pre-computed analytics for the static HTML dashboard.

Usage:
    python scripts/build_dashboard_data.py
"""

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
CSV = ROOT / "mock_data" / "all_listings.csv"
OUT = ROOT / "dashboard" / "data.json"

CAT_DISPLAY = {
    "sneakers": "Sneakers", "trading_cards": "Trading Cards",
    "electronics": "Electronics", "furniture": "Furniture",
    "watches": "Watches", "handbags": "Handbags",
    "vintage_clothing": "Vintage Clothing", "video_games": "Video Games",
    "collectibles": "Collectibles", "cameras": "Cameras & Lenses",
}

SOURCE_DISPLAY = {
    "ebay": "eBay", "amazon": "Amazon", "stockx": "StockX",
    "facebook_marketplace": "Facebook Marketplace", "goat": "GOAT", "mercari": "Mercari",
}


def build():
    df = pd.read_csv(CSV, low_memory=False)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df = df[df["price"].notna() & (df["price"] > 0)].copy()

    for col in ["is_sold", "is_auction", "is_authenticated"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().map(
                {"true": True, "false": False, "1": True, "0": False}
            ).fillna(False)

    for col in ["shipping_cost", "platform_fee", "sold_price", "seller_rating"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["listing_date"] = df["created_at"].dt.date
    df["month"] = df["created_at"].dt.to_period("M").astype(str)
    df["cat_display"] = df["category"].map(CAT_DISPLAY).fillna(df["category"])
    df["src_display"] = df["source"].map(SOURCE_DISPLAY).fillna(df["source"])

    overall_median = float(df["price"].median())

    # KPIs
    kpis = {
        "total_listings": int(len(df)),
        "total_gmv": round(float(df["price"].sum()), 2),
        "overall_median": round(overall_median, 2),
        "sell_through_pct": round(float(df["is_sold"].mean() * 100), 1) if "is_sold" in df.columns else 0,
        "auction_pct": round(float(df["is_auction"].mean() * 100), 1) if "is_auction" in df.columns else 0,
    }

    # Category stats
    cat_stats = []
    for cat, g in df.groupby("category"):
        cat_stats.append({
            "category": cat,
            "category_display": CAT_DISPLAY.get(cat, cat),
            "listings": int(len(g)),
            "median_price": round(float(g["price"].median()), 2),
            "avg_price": round(float(g["price"].mean()), 2),
            "min_price": round(float(g["price"].min()), 2),
            "max_price": round(float(g["price"].max()), 2),
            "sold_count": int(g["is_sold"].sum()) if "is_sold" in g.columns else 0,
            "avg_shipping": round(float(g["shipping_cost"].mean()), 2) if "shipping_cost" in g.columns else 0,
            "sell_through": round(float(g["is_sold"].mean() * 100), 1) if "is_sold" in g.columns else 0,
        })

    # Source stats
    src_stats = []
    for src, g in df.groupby("source"):
        src_stats.append({
            "source": src,
            "source_display": SOURCE_DISPLAY.get(src, src),
            "listings": int(len(g)),
            "median_price": round(float(g["price"].median()), 2),
            "avg_fee": round(float(g["platform_fee"].mean()), 2) if "platform_fee" in g.columns else 0,
            "sold_count": int(g["is_sold"].sum()) if "is_sold" in g.columns else 0,
            "sell_through": round(float(g["is_sold"].mean() * 100), 1) if "is_sold" in g.columns else 0,
        })

    # Heatmap: source x category median price
    heat = []
    for (src, cat), g in df.groupby(["source", "category"]):
        heat.append({
            "source": SOURCE_DISPLAY.get(src, src),
            "category": CAT_DISPLAY.get(cat, cat),
            "median_price": round(float(g["price"].median()), 2),
        })

    # Monthly trends
    monthly = []
    for (m, cat), g in df.groupby(["month", "category"]):
        monthly.append({
            "month": m,
            "category": CAT_DISPLAY.get(cat, cat),
            "median_price": round(float(g["price"].median()), 2),
        })

    # Top brands per category (top 5 by median price, min 5 listings)
    top_brands = []
    for cat, cg in df.groupby("category"):
        if "brand" not in cg.columns:
            continue
        brand_agg = cg.groupby("brand").agg(
            count=("price", "size"), median_price=("price", "median")
        ).reset_index()
        brand_agg = brand_agg[brand_agg["count"] >= 5].sort_values("median_price", ascending=False).head(5)
        for _, row in brand_agg.iterrows():
            top_brands.append({
                "category_display": CAT_DISPLAY.get(cat, cat),
                "brand": row["brand"],
                "count": int(row["count"]),
                "median_price": round(float(row["median_price"]), 2),
            })

    # Daily volume (last 5 days sample for sparklines)
    recent = df.sort_values("created_at").tail(5000)
    daily_vol = []
    for (d, src), g in recent.groupby([recent["listing_date"].astype(str), "source"]):
        daily_vol.append({
            "date": d,
            "source_display": SOURCE_DISPLAY.get(src, src),
            "count": int(len(g)),
        })

    # Anomalies (top 40 by price ratio to category median)
    cat_medians = df.groupby("category")["price"].median()
    df["cat_median"] = df["category"].map(cat_medians)
    df["price_ratio"] = df["price"] / df["cat_median"]
    anomalies_df = df.sort_values("price_ratio", ascending=False).head(40)
    anomalies = []
    for _, r in anomalies_df.iterrows():
        anomalies.append({
            "title": r.get("title", ""),
            "category_display": CAT_DISPLAY.get(r["category"], r["category"]),
            "source_display": SOURCE_DISPLAY.get(r["source"], r["source"]),
            "price": round(float(r["price"]), 2),
            "cat_median": round(float(r["cat_median"]), 2),
            "price_ratio": round(float(r["price_ratio"]), 2),
            "condition": r.get("condition", ""),
        })

    # Source premium vs overall median
    src_premium = []
    for src, g in df.groupby("source"):
        prem = ((g["price"].median() - overall_median) / overall_median) * 100
        src_premium.append({
            "source_display": SOURCE_DISPLAY.get(src, src),
            "premium": round(float(prem), 2),
        })

    # Condition x category median price
    cond = []
    if "condition" in df.columns:
        for (c, cat), g in df.groupby(["condition", "category"]):
            cond.append({
                "condition": c,
                "category": CAT_DISPLAY.get(cat, cat),
                "median_price": round(float(g["price"].median()), 2),
            })

    data = {
        "kpis": kpis,
        "cat_stats": cat_stats,
        "src_stats": src_stats,
        "heat": heat,
        "monthly": monthly,
        "top_brands": top_brands,
        "daily_vol": daily_vol,
        "anomalies": anomalies,
        "src_premium": src_premium,
        "cond": cond,
    }

    OUT.write_text(json.dumps(data, indent=2))
    print(f"Wrote {OUT} ({OUT.stat().st_size:,} bytes)")


if __name__ == "__main__":
    build()
