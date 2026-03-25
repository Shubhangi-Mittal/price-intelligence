#!/usr/bin/env bash
# scripts/setup_and_run.sh
# One-command local setup — no cloud credentials required.
#
# Usage:
#   bash scripts/setup_and_run.sh           # setup only
#   bash scripts/setup_and_run.sh --launch  # setup + open dashboard

set -e

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'

echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Marketplace Price Intelligence Pipeline v2             ║${NC}"
echo -e "${BLUE}║   6 Sources · 10 Categories · End-to-End                ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""

# ── 1. Python ────────────────────────────────────────────────────────────────
echo -e "${YELLOW}[1/5] Checking Python...${NC}"
python3 --version || { echo -e "${RED}Python 3 required.${NC}"; exit 1; }

# ── 2. Dependencies ───────────────────────────────────────────────────────────
echo -e "${YELLOW}[2/5] Installing dependencies...${NC}"
pip install -q pandas loguru pydantic python-dotenv duckdb streamlit plotly dbt-core requests
echo -e "${GREEN}✓ Dependencies installed${NC}"

# ── 3. Generate mock data ─────────────────────────────────────────────────────
echo -e "${YELLOW}[3/5] Generating mock marketplace data...${NC}"
echo -e "  Sources:    ${CYAN}eBay · Amazon · StockX · Facebook Marketplace · GOAT · Mercari${NC}"
echo -e "  Categories: ${CYAN}Sneakers · Trading Cards · Electronics · Furniture · Watches${NC}"
echo -e "              ${CYAN}Handbags · Vintage Clothing · Video Games · Collectibles · Cameras${NC}"
python3 ingestion/mock_feed_generator.py --days 90 --records 400 --output mock_data
echo -e "${GREEN}✓ Mock data generated${NC}"

# ── 4. dbt setup ──────────────────────────────────────────────────────────────
echo -e "${YELLOW}[4/5] Configuring dbt with DuckDB...${NC}"
cd dbt_project

cat > packages.yml << 'PKGS'
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
PKGS

mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << 'PROF'
price_intelligence:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /tmp/price_intelligence_v2.duckdb
      threads: 4
    prod_bq:
      type: bigquery
      method: service-account
      project: "{{ env_var('GCP_PROJECT_ID') }}"
      dataset: price_intelligence
      location: US
      threads: 4
      keyfile: "{{ env_var('GCP_KEYFILE_PATH', '~/.gcp/keyfile.json') }}"
PROF

dbt deps --quiet 2>/dev/null || echo "  (dbt deps — install dbt-core for full run)"
cd ..
echo -e "${GREEN}✓ dbt configured${NC}"

# ── 5. Validate ───────────────────────────────────────────────────────────────
echo -e "${YELLOW}[5/5] Validating data...${NC}"
python3 - << 'PYEOF'
import pandas as pd
df = pd.read_csv('mock_data/all_listings.csv')
df = df[df['price'].notna() & (df['price'] > 0)]
cats = df['category'].value_counts()
srcs = df['source'].value_counts()
print(f"  Total records : {len(df):,}")
print(f"  Date range    : {df['created_at'].min()[:10]} → {df['created_at'].max()[:10]}")
print(f"  Categories ({len(cats)}):")
for cat, n in cats.items():
    icon_map = {
        'sneakers':'👟','trading_cards':'🃏','electronics':'💻','furniture':'🛋️',
        'watches':'⌚','handbags':'👜','vintage_clothing':'👕','video_games':'🎮',
        'collectibles':'🏆','cameras':'📷',
    }
    print(f"    {icon_map.get(cat,'•')} {cat}: {n:,}")
print(f"  Sources ({len(srcs)}):")
for src, n in srcs.items():
    print(f"    • {src}: {n:,}")
PYEOF
echo -e "${GREEN}✓ Data validated${NC}"

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Setup Complete!                                         ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${GREEN}Local development:${NC}"
echo -e "    Dashboard:   ${YELLOW}cd dashboard && python3 -m http.server 8080${NC}"
echo -e "    Streamlit:   ${YELLOW}streamlit run dashboard/app.py${NC}"
echo -e "    dbt run:     ${YELLOW}cd dbt_project && dbt run --target dev${NC}"
echo -e "    dbt test:    ${YELLOW}cd dbt_project && dbt test${NC}"
echo -e "    Monitor:     ${YELLOW}python monitoring/monitor.py${NC}"
echo ""
echo -e "  ${CYAN}Cloud deployment (.env):${NC}"
echo -e "    EBAY_APP_ID, GCP_PROJECT_ID, GCS_BUCKET, SLACK_WEBHOOK_URL"
echo -e "    # Amazon/StockX/GOAT/Mercari API keys as you acquire them"
echo ""

if [[ "$1" == "--launch" ]]; then
    echo -e "${GREEN}Starting dashboard server on http://localhost:8080 ...${NC}"
    cd dashboard && python3 -m http.server 8080
fi
