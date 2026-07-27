"""
load_duckdb.py
Land the raw marketplace feed in the local DuckDB warehouse as raw.listings.

    python scripts/load_duckdb.py

This is the local-development counterpart to the load_to_bigquery task in
orchestration/airflow/dags/price_intelligence_pipeline.py. It exists so the dbt
project can be built and tested end to end without cloud credentials:

    python ingestion/mock_feed_generator.py --days 90 --records 400
    python scripts/load_duckdb.py
    cd dbt_project && dbt deps && dbt build

The warehouse file is named price_intelligence.duckdb because DuckDB derives the
catalog name from the filename, and models/staging/sources.yml declares
`database: price_intelligence`. Renaming the file breaks source resolution
unless DBT_DATABASE is set to match.
"""

import os
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(
    os.getenv("DUCKDB_PATH", REPO_ROOT / "warehouse" / "price_intelligence.duckdb")
)
CSV_PATH = Path(os.getenv("RAW_CSV_PATH", REPO_ROOT / "mock_data" / "all_listings.csv"))

if not CSV_PATH.exists():
    raise FileNotFoundError(
        f"Missing {CSV_PATH}. Generate it first:\n"
        f"  python ingestion/mock_feed_generator.py --days 90 --records 400"
    )

DB_PATH.parent.mkdir(parents=True, exist_ok=True)
con = duckdb.connect(str(DB_PATH))

con.execute("create schema if not exists raw;")
con.execute("drop table if exists raw.listings;")

# The raw table is loaded as-is, defects included — the feed deliberately
# carries null prices, duplicate listing_ids and future-dated rows. Cleaning it
# is stg_listings' job, and the warn-level source tests measure what arrives.
con.execute(
    """
    create table raw.listings as
    select * from read_csv_auto(?, header=true);
    """,
    [str(CSV_PATH)],
)

cnt = con.execute("select count(*) from raw.listings").fetchone()[0]
con.close()

print(f"✅ Loaded {cnt:,} rows into {DB_PATH} (raw.listings)")
