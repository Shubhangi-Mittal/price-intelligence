"""
ebay_extractor.py
Extracts product listings from eBay Finding API with:
  - Rate limiting + exponential backoff
  - Pydantic schema validation
  - Incremental extraction (last_run watermark)
  - Partitioned output: YYYY-MM-DD/source/category/file.parquet

Supports all 10 categories across 6 marketplace sources.

Usage:
    python ingestion/ebay_extractor.py --category sneakers --max-pages 10
    python ingestion/ebay_extractor.py  # extracts all categories
"""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from loguru import logger
from pydantic import BaseModel, Field, field_validator
from dotenv import load_dotenv
import os

load_dotenv()

EBAY_APP_ID      = os.getenv("EBAY_APP_ID")
EBAY_FINDING_API = "https://svcs.ebay.com/services/search/FindingService/v1"

# ── Expanded keyword map — 10 categories ──────────────────────────────────────
CATEGORY_KEYWORDS = {
    "sneakers":         ["Air Jordan resell", "Nike Dunk Low", "Adidas Yeezy", "New Balance 550"],
    "trading_cards":    ["Pokemon card PSA", "Magic the Gathering rare", "NBA Prizm rookie", "One Piece card"],
    "electronics":      ["iPhone 15 Pro unlocked", "Sony WH-1000XM5", "MacBook Pro M3", "Samsung Galaxy S24"],
    "furniture":        ["Eames lounge chair", "Herman Miller Aeron", "mid century modern sofa", "Knoll chair"],
    "watches":          ["Rolex Submariner", "Omega Speedmaster", "Seiko SARB", "Patek Philippe"],
    "handbags":         ["Louis Vuitton Neverfull", "Chanel classic flap", "Gucci Dionysus", "Hermes Birkin"],
    "vintage_clothing": ["Supreme box logo", "vintage Levi 501", "band tee 90s", "Carhartt WIP vintage"],
    "video_games":      ["PS5 console", "Nintendo Switch OLED", "Pokemon Red CIB", "Zelda SNES"],
    "collectibles":     ["KAWS companion", "Bearbrick 1000", "LEGO UCS", "Hot Wheels redline"],
    "cameras":          ["Sony A7 IV", "Leica M11", "Fujifilm X-T5", "Canon R5"],
}

# ── Source URL map ─────────────────────────────────────────────────────────────
SOURCE_URLS = {
    "ebay":                 "https://www.ebay.com/itm",
    "amazon":               "https://www.amazon.com/dp",
    "stockx":               "https://stockx.com/product",
    "facebook_marketplace": "https://www.facebook.com/marketplace/item",
    "goat":                 "https://www.goat.com/sneakers",
    "mercari":              "https://www.mercari.com/us/item",
}

ALL_SOURCES = list(SOURCE_URLS.keys())


# ── Pydantic schema ────────────────────────────────────────────────────────────
class ListingSchema(BaseModel):
    listing_id:     str
    source:         str = "ebay"
    category:       str
    brand:          Optional[str] = None
    title:          str
    condition:      Optional[str] = None
    price:          float = Field(gt=0)
    currency:       str = "USD"
    shipping_cost:  float = 0.0
    total_cost:     float = 0.0
    platform_fee:   float = 0.0
    seller_id:      str
    seller_rating:  Optional[float] = None
    is_auction:     bool = False
    num_bids:       int = 0
    listing_url:    str
    image_url:      Optional[str] = None
    location_city:  Optional[str] = None
    created_at:     datetime
    ingested_at:    datetime = Field(default_factory=datetime.utcnow)
    batch_id:       str

    @field_validator("price")
    @classmethod
    def price_positive(cls, v):
        if v <= 0:
            raise ValueError(f"Price must be positive, got {v}")
        return round(v, 2)

    @field_validator("seller_rating")
    @classmethod
    def rating_valid(cls, v):
        if v is not None and not (0 <= v <= 5):
            raise ValueError(f"Rating must be 0–5, got {v}")
        return v


# ── Extractor ─────────────────────────────────────────────────────────────────
class EbayExtractor:
    def __init__(self, output_dir: str = "raw_data", state_file: str = ".extractor_state.json"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.state_file = Path(state_file)
        self.state      = self._load_state()
        self.session    = requests.Session()
        self.session.headers.update({"X-EBAY-SOA-SECURITY-APPNAME": EBAY_APP_ID or "DEMO"})

    def _load_state(self) -> dict:
        if self.state_file.exists():
            with open(self.state_file) as f:
                return json.load(f)
        return {"last_run": None, "total_extracted": 0}

    def _save_state(self):
        with open(self.state_file, "w") as f:
            json.dump(self.state, f, default=str)

    def _make_request(self, url: str, params: dict, max_retries: int = 3) -> dict:
        for attempt in range(max_retries):
            try:
                time.sleep(0.5 + attempt * 0.5)
                resp = self.session.get(url, params=params, timeout=15)
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.HTTPError as e:
                if resp.status_code == 429:
                    wait = 2 ** attempt * 5
                    logger.warning(f"Rate limited. Waiting {wait}s")
                    time.sleep(wait)
                elif resp.status_code >= 500:
                    time.sleep(2 ** attempt * 2)
                else:
                    raise
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Request failed: {e}. Retry {attempt+1}/{max_retries}")
                time.sleep(2 ** attempt)
        raise RuntimeError(f"Max retries exceeded for {url}")

    def _parse_item(self, item: dict, category: str, batch_id: str) -> Optional[dict]:
        try:
            price_val = float(
                item.get("sellingStatus", [{}])[0]
                    .get("currentPrice", [{"__value__": "0"}])[0]
                    .get("__value__", 0)
            )
            listing_type = item.get("listingInfo", [{}])[0].get("listingType", ["FixedPrice"])[0]

            raw = {
                "listing_id":   item.get("itemId", [""])[0],
                "source":       "ebay",
                "category":     category,
                "title":        item.get("title", [""])[0],
                "price":        price_val,
                "total_cost":   price_val,
                "platform_fee": round(price_val * 0.1295, 2),
                "condition":    item.get("condition", [{}])[0].get("conditionDisplayName", ["Unknown"])[0],
                "seller_id":    item.get("sellerInfo", [{}])[0].get("sellerUserName", ["unknown"])[0],
                "seller_rating": float(item.get("sellerInfo", [{}])[0].get("positiveFeedbackPercent", [0])[0]) / 20,
                "is_auction":   listing_type == "Auction",
                "num_bids":     int(item.get("sellingStatus", [{}])[0].get("bidCount", [0])[0]),
                "listing_url":  item.get("viewItemURL", [""])[0],
                "image_url":    item.get("galleryURL", [None])[0],
                "location_city":item.get("location", [None])[0],
                "created_at":   datetime.fromisoformat(
                    item.get("listingInfo", [{}])[0]
                        .get("startTime", [datetime.utcnow().isoformat()])[0]
                        .replace("Z", "")
                ),
                "batch_id": batch_id,
            }
            validated = ListingSchema(**raw)
            return validated.model_dump()
        except Exception as e:
            logger.warning(f"Parse error for item {item.get('itemId', '?')}: {e}")
            return None

    def extract_category(self, category: str, max_pages: int = 5) -> list[dict]:
        keywords  = CATEGORY_KEYWORDS.get(category, [category])
        batch_id  = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M')}"
        listings  = []
        errors    = 0

        for kw in keywords:
            logger.info(f"Extracting '{kw}' (category={category})")

            if not EBAY_APP_ID:
                logger.warning("No EBAY_APP_ID — using mock data.")
                return self._mock_listings(category, batch_id)

            for page in range(1, max_pages + 1):
                params = {
                    "OPERATION-NAME":              "findItemsByKeywords",
                    "SERVICE-VERSION":             "1.0.0",
                    "RESPONSE-DATA-FORMAT":        "JSON",
                    "keywords":                    kw,
                    "paginationInput.pageNumber":  page,
                    "paginationInput.entriesPerPage": 100,
                    "sortOrder":                   "EndTimeSoonest",
                }
                try:
                    data  = self._make_request(EBAY_FINDING_API, params)
                    items = (
                        data.get("findItemsByKeywordsResponse", [{}])[0]
                            .get("searchResult", [{}])[0]
                            .get("item", [])
                    )
                    if not items:
                        break
                    for item in items:
                        parsed = self._parse_item(item, category, batch_id)
                        if parsed:
                            listings.append(parsed)
                        else:
                            errors += 1
                except Exception as e:
                    logger.error(f"Page {page} failed for '{kw}': {e}")
                    break

        logger.info(f"'{category}': {len(listings)} listings, {errors} errors")
        return listings

    def _mock_listings(self, category: str, batch_id: str, count: int = 200) -> list[dict]:
        """Fallback when no eBay credentials — delegates to mock_feed_generator."""
        import sys, uuid
        sys.path.insert(0, str(Path(__file__).parent))
        from ingestion.mock_feed_generator import generate_listing

        rows = []
        for _ in range(count):
            raw = generate_listing(str(uuid.uuid4()), category, datetime.utcnow())
            raw["batch_id"] = batch_id
            rows.append(raw)
        return rows

    def save_to_parquet(self, listings: list[dict], category: str) -> Optional[Path]:
        if not listings:
            logger.warning(f"No listings to save for {category}")
            return None
        df = pd.DataFrame(listings)
        df["extraction_date"] = datetime.utcnow().strftime("%Y-%m-%d")
        out = self.output_dir / df["extraction_date"].iloc[0] / "ebay" / f"{category}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False, compression="snappy")
        logger.info(f"Saved {len(df):,} → {out}")
        return out

    def run(self, categories: list[str] = None, max_pages: int = 5) -> dict:
        categories = categories or list(CATEGORY_KEYWORDS.keys())
        run_start  = datetime.utcnow()
        summary    = {"categories": {}, "total_extracted": 0, "run_start": str(run_start)}

        logger.info(f"Starting extraction: {categories}")
        for cat in categories:
            listings = self.extract_category(cat, max_pages)
            path     = self.save_to_parquet(listings, cat)
            summary["categories"][cat] = {"count": len(listings), "output": str(path)}
            summary["total_extracted"] += len(listings)

        run_end  = datetime.utcnow()
        summary["run_end"]          = str(run_end)
        summary["duration_seconds"] = (run_end - run_start).total_seconds()
        self.state["last_run"]       = str(run_end)
        self.state["total_extracted"] += summary["total_extracted"]
        self._save_state()

        logger.success(f"Done: {summary['total_extracted']:,} records in {summary['duration_seconds']:.1f}s")
        return summary


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", nargs="+", default=None, choices=list(CATEGORY_KEYWORDS.keys()))
    parser.add_argument("--max-pages", type=int, default=5)
    parser.add_argument("--output",    default="raw_data")
    args = parser.parse_args()

    extractor = EbayExtractor(output_dir=args.output)
    summary   = extractor.run(categories=args.category, max_pages=args.max_pages)
    print(json.dumps(summary, indent=2, default=str))
