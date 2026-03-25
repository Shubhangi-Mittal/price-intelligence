# 🛒 Marketplace Price Intelligence Pipeline v2

> End-to-end data engineering project — 6 marketplace sources × 10 product categories

## What's new in v2

| | v1 | v2 |
|---|---|---|
| Sources | 5 (eBay, StockX, GOAT, Mercari, FB) | **6** (+ Amazon) |
| Categories | 3 (sneakers, cards, electronics) | **10** (+ furniture, watches, handbags, vintage clothing, video games, collectibles, cameras) |
| Price range | $5–$2,000 | **$5–$85,000** (watches, handbags) |
| Platform fee tracking | ❌ | ✅ per-source fee model |
| Authentication tracking | ❌ | ✅ StockX/GOAT verified flag |
| Category volatility | ❌ | ✅ CV-based volatility tier |
| Coverage monitoring | ❌ | ✅ 6×10 source×category gap detection |
| Local pickup flag | ❌ | ✅ Facebook Marketplace support |

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                  PRICE INTELLIGENCE PIPELINE v2                       │
│                                                                        │
│  ┌────────────┐   ┌──────────┐   ┌─────────────┐   ┌────────────┐  │
│  │ INGESTION  │──▶│ STORAGE  │──▶│  TRANSFORM  │──▶│ ANALYTICS  │  │
│  │            │   │          │   │             │   │            │  │
│  │ • eBay API │   │ • GCS/S3 │   │ stg_listings│   │ Trend dash │  │
│  │ • Amazon * │   │ • BigQ / │   │ int_enriched│   │ Anomaly    │  │
│  │ • StockX * │   │   Snowflk│   │ mart_trends │   │   alerts   │  │
│  │ • FB Mkt * │   │          │   │ mart_anomaly│   │ Source     │  │
│  │ • GOAT   * │   │          │   │             │   │   coverage │  │
│  │ • Mercari* │   │          │   │             │   │            │  │
│  └────────────┘   └──────────┘   └─────────────┘   └────────────┘  │
│                                                                        │
│  * = mock feed today; plug in real API client to go live              │
│                                                                        │
│  ─────────── Airflow DAG  +  dbt tests  +  Monitoring  ───────────── │
└──────────────────────────────────────────────────────────────────────┘
```

## Categories

| Icon | Category | Sources | Price Range | Volatility |
|------|----------|---------|-------------|------------|
| 👟 | Sneakers | StockX, GOAT, eBay | $85–$650 | Medium |
| 🃏 | Trading Cards | eBay, Mercari, StockX | $5–$5,000 | **Very High** |
| 💻 | Electronics | eBay, Amazon, FB | $80–$2,200 | Low |
| 🛋️ | Furniture | FB, eBay, Mercari | $50–$8,000 | Medium |
| ⌚ | Watches | StockX, eBay, GOAT | $200–$85,000 | Medium |
| 👜 | Handbags | StockX, GOAT, eBay | $400–$45,000 | Low |
| 👕 | Vintage Clothing | Mercari, eBay, FB | $15–$1,200 | High |
| 🎮 | Video Games | eBay, Mercari, Amazon | $15–$1,800 | Medium |
| 🏆 | Collectibles | eBay, Mercari, StockX | $12–$12,000 | High |
| 📷 | Cameras | eBay, Amazon, Mercari | $200–$9,500 | Low |

## Source Profiles

| Source | Fee | Authentication | Ships Intl | Best For |
|--------|-----|----------------|------------|---------|
| eBay | 12.95% | No | Yes | Electronics, Cards, Collectibles |
| Amazon | 15% | No | No | Electronics, Cameras, Games |
| StockX | 9.5% | ✅ Yes | Yes | Sneakers, Watches, Handbags |
| Facebook Marketplace | 5% | No | No (local) | Furniture, Electronics |
| GOAT | 9.8% | ✅ Yes | Yes | Sneakers, Handbags, Watches |
| Mercari | 10% | No | No | Vintage, Cards, Collectibles |

## Quick Start

```bash
# One command — no API keys needed
bash scripts/setup_and_run.sh --launch

# Or step by step:
python ingestion/mock_feed_generator.py --days 90 --records 400
cd dbt_project && dbt run && dbt test
cd dashboard && python3 -m http.server 8080    # open index.html
streamlit run dashboard/app.py                  # alternative
python monitoring/monitor.py                    # run checks
```

## Project Structure

```
price-intelligence/
├── ingestion/
│   ├── mock_feed_generator.py   # 6 sources × 10 categories
│   ├── ebay_extractor.py        # eBay Finding API + mock fallback
│   └── gcs_uploader.py          # GCS/S3 upload with idempotency
├── dbt_project/
│   └── models/
│       ├── staging/
│       │   └── stg_listings.sql         # 6 sources, 10 categories, new columns
│       ├── intermediate/
│       │   └── int_listings_enriched.sql # + platform fee, volatility tier
│       └── marts/
│           ├── mart_price_trends.sql     # + auth rate, fee metrics
│           └── mart_price_anomalies.sql  # + volatility context
├── orchestration/
│   └── airflow/dags/
│       └── price_intelligence_pipeline.py  # all 6 sources in DAG
├── monitoring/
│   └── monitor.py               # + coverage gap detection (6×10 matrix)
├── dashboard/
│   ├── index.html               # dark aesthetic interactive dashboard
│   ├── data.json                # pre-computed analytics
│   └── app.py                   # Streamlit version
└── scripts/
    └── setup_and_run.sh
```
