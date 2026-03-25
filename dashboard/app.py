"""
dashboard/app.py
━━━━━━━━━━━━━━━━
Streamlit Price Intelligence Dashboard v2

Sources:    eBay · Amazon · StockX · Facebook Marketplace · GOAT · Mercari
Categories: Sneakers · Trading Cards · Electronics · Furniture · Watches
            Handbags · Vintage Clothing · Video Games · Collectibles · Cameras

Run:
    streamlit run dashboard/app.py
"""

import os
from datetime import datetime, timedelta, date
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="Price Intelligence v2",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

ALL_SOURCES = ["ebay","amazon","stockx","facebook_marketplace","goat","mercari"]

SOURCE_DISPLAY = {
    "ebay":"eBay","amazon":"Amazon","stockx":"StockX",
    "facebook_marketplace":"Facebook Marketplace","goat":"GOAT","mercari":"Mercari",
}

SOURCE_COLORS = {
    "ebay":"#e53238","amazon":"#ff9900","stockx":"#00bb00",
    "facebook_marketplace":"#1877f2","goat":"#8b8b8b","mercari":"#e8173a",
}

CAT_DISPLAY = {
    "sneakers":"👟 Sneakers","trading_cards":"🃏 Trading Cards",
    "electronics":"💻 Electronics","furniture":"🛋️ Furniture",
    "watches":"⌚ Watches","handbags":"👜 Handbags",
    "vintage_clothing":"👕 Vintage Clothing","video_games":"🎮 Video Games",
    "collectibles":"🏆 Collectibles","cameras":"📷 Cameras & Lenses",
}

THEME = "plotly_white"


@st.cache_data(ttl=300)
def load_csv(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        st.error(f"Data file not found: `{p}`\n\nRun: `python ingestion/mock_feed_generator.py --days 90 --records 400`")
        return pd.DataFrame()
    df = pd.read_csv(p, low_memory=False)
    for col in ["created_at","updated_at","ingested_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", format="ISO8601")
    for col in ["price","total_cost","shipping_cost","platform_fee","sold_price","seller_rating"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["is_sold","is_auction","is_authenticated","has_local_pickup","ships_international"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().map(
                {"true":True,"false":False,"1":True,"0":False}).fillna(False)
    df["listing_date"] = df["created_at"].dt.date
    df = df[df["price"] > 0].dropna(subset=["price"])
    return df


@st.cache_data(ttl=300)
def load_bigquery(project: str, dataset: str) -> pd.DataFrame:
    try:
        from google.cloud import bigquery
        client = bigquery.Client()
        return client.query(
            f"SELECT * FROM `{project}.{dataset}.mart_price_trends` LIMIT 100000"
        ).to_dataframe()
    except Exception as e:
        st.warning(f"BigQuery unavailable: {e}")
        return pd.DataFrame()


def flag_anomalies(df: pd.DataFrame, threshold: float = 0.30) -> pd.DataFrame:
    if df.empty: return df
    daily_med = df.groupby(["listing_date","category"])["price"].transform("median")
    df = df.copy()
    df["price_vs_median_pct"] = (df["price"] - daily_med) / daily_med
    df["is_anomaly"] = df["price_vs_median_pct"].abs() > threshold
    return df


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 Price Intelligence v2")
    st.caption("6 sources · 10 categories")
    st.divider()

    data_source = st.radio("Data source", ["Local CSV","BigQuery"], index=0)

    if data_source == "Local CSV":
        csv_path = st.text_input("CSV path", value="mock_data/all_listings.csv")
        df_raw = load_csv(csv_path)
    else:
        proj = st.text_input("GCP project", value=os.getenv("GCP_PROJECT_ID",""))
        dset = st.text_input("Dataset", value="price_intelligence_marts")
        df_raw = load_bigquery(proj, dset)
        if df_raw.empty:
            df_raw = load_csv("mock_data/all_listings.csv")

    if df_raw.empty:
        st.stop()

    st.divider()
    st.markdown("### Filters")

    dates = pd.to_datetime(df_raw["listing_date"]).dt.date.dropna()
    min_d, max_d = dates.min(), dates.max()
    date_range = st.date_input("Date range",
        value=(max_d - timedelta(days=30), max_d), min_value=min_d, max_value=max_d)

    avail_cats = sorted(df_raw["category"].dropna().unique()) if "category" in df_raw.columns else []
    sel_cats = st.multiselect("Categories", avail_cats, default=avail_cats,
        format_func=lambda c: CAT_DISPLAY.get(c, c))

    avail_srcs = sorted(df_raw["source"].dropna().unique()) if "source" in df_raw.columns else []
    sel_srcs = st.multiselect("Sources", avail_srcs, default=avail_srcs,
        format_func=lambda s: SOURCE_DISPLAY.get(s, s))

    avail_conds = sorted(df_raw["condition"].dropna().unique()) if "condition" in df_raw.columns else []
    sel_conds = st.multiselect("Condition", avail_conds, default=avail_conds) if avail_conds else []

    pmin = float(df_raw["price"].min())
    pmax = float(df_raw["price"].quantile(0.99))
    price_range = st.slider("Price range ($)", pmin, pmax, (pmin, pmax), format="$%.0f")

    mask = (
        (pd.to_datetime(df_raw["listing_date"]).dt.date >= date_range[0])
        & (pd.to_datetime(df_raw["listing_date"]).dt.date <= date_range[1])
        & (df_raw["price"] >= price_range[0])
        & (df_raw["price"] <= price_range[1])
    )
    if sel_cats: mask &= df_raw["category"].isin(sel_cats)
    if sel_srcs: mask &= df_raw["source"].isin(sel_srcs)
    if sel_conds and "condition" in df_raw.columns: mask &= df_raw["condition"].isin(sel_conds)
    df = df_raw[mask].copy()

    st.divider()
    st.caption(f"Showing **{len(df):,}** of {len(df_raw):,} listings")


# ── Header + KPIs ──────────────────────────────────────────────────────────────
st.title("📊 Marketplace Price Intelligence")
st.caption(f"{date_range[0]} → {date_range[1]}  ·  {len(sel_srcs)} sources  ·  {len(sel_cats)} categories  ·  {len(df):,} listings")

k1,k2,k3,k4,k5,k6 = st.columns(6)
k1.metric("Total Listings",   f"{len(df):,}")
k2.metric("Median Price",     f"${df['price'].median():,.0f}" if len(df) else "—")
k3.metric("Avg Price",        f"${df['price'].mean():,.0f}" if len(df) else "—")
k4.metric("Sell-Through",     f"{df['is_sold'].mean()*100:.1f}%" if "is_sold" in df.columns and len(df) else "—")
k5.metric("Auth Rate",        f"{df['is_authenticated'].mean()*100:.1f}%" if "is_authenticated" in df.columns and len(df) else "—", help="StockX/GOAT verified")
k6.metric("Avg Platform Fee", f"${df['platform_fee'].mean():,.0f}" if "platform_fee" in df.columns and len(df) else "—")

st.divider()


# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1,tab2,tab3,tab4,tab5 = st.tabs(["📈 Price Trends","🏪 Source Analysis","🔍 Anomalies","🏷️ Categories","📋 Raw Data"])


with tab1:
    st.subheader("Price Trends Over Time")
    if df.empty: st.info("No data for selected filters.")
    else:
        df["month"] = df["created_at"].dt.to_period("M").astype(str)
        monthly = (df.groupby(["month","category"])["price"].median().reset_index()
                   .rename(columns={"price":"median_price"}))
        monthly["Category"] = monthly["category"].map(CAT_DISPLAY).fillna(monthly["category"])
        fig = px.line(monthly, x="month", y="median_price", color="Category",
            title="Monthly Median Price by Category",
            labels={"median_price":"Median Price (USD)","month":"Month"}, template=THEME)
        fig.update_layout(height=420, hovermode="x unified", legend=dict(orientation="h",y=-0.25))
        st.plotly_chart(fig, use_container_width=True)

        c1,c2 = st.columns(2)
        with c1:
            daily = df.groupby(["listing_date","category"]).size().reset_index(name="count")
            daily["category"] = daily["category"].map(CAT_DISPLAY).fillna(daily["category"])
            fig2 = px.bar(daily, x="listing_date", y="count", color="category",
                title="Daily Listing Volume", labels={"count":"Listings","listing_date":"Date","category":"Category"}, template=THEME)
            fig2.update_layout(height=320, legend=dict(orientation="h",y=-0.3))
            st.plotly_chart(fig2, use_container_width=True)
        with c2:
            box_df = df[df["price"] < df["price"].quantile(0.98)].copy()
            box_df["cat_label"] = box_df["category"].map(CAT_DISPLAY).fillna(box_df["category"])
            fig3 = px.box(box_df, x="cat_label", y="price",
                color="condition" if "condition" in box_df.columns else "cat_label",
                title="Price Distribution by Category & Condition",
                labels={"price":"Price (USD)","cat_label":"Category"}, template=THEME)
            fig3.update_layout(height=320, legend=dict(orientation="h",y=-0.3))
            st.plotly_chart(fig3, use_container_width=True)

        if "platform_fee" in df.columns:
            st.subheader("Platform Fee by Source")
            fee_df = (df.groupby("source").agg(avg_fee=("platform_fee","mean")).reset_index())
            fee_df["source_label"] = fee_df["source"].map(SOURCE_DISPLAY).fillna(fee_df["source"])
            fig4 = px.bar(fee_df.sort_values("avg_fee",ascending=False),
                x="source_label", y="avg_fee", title="Average Platform Fee (USD)",
                labels={"avg_fee":"Avg Fee ($)","source_label":"Source"},
                color="source_label",
                color_discrete_map={SOURCE_DISPLAY.get(k,k):v for k,v in SOURCE_COLORS.items()},
                template=THEME)
            fig4.update_layout(showlegend=False, height=280)
            st.plotly_chart(fig4, use_container_width=True)


with tab2:
    st.subheader("Source Coverage & Performance")
    if df.empty: st.info("No data for selected filters.")
    else:
        c1,c2 = st.columns(2)
        with c1:
            src_c = (df["source"].map(SOURCE_DISPLAY).fillna(df["source"]).value_counts().reset_index())
            src_c.columns = ["source","count"]
            fig = px.pie(src_c, values="count", names="source", title="Listings by Source",
                color="source", color_discrete_map={SOURCE_DISPLAY.get(k,k):v for k,v in SOURCE_COLORS.items()},
                template=THEME)
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            sp = (df.groupby("source")["price"].median().reset_index().sort_values("price",ascending=False))
            sp["source_label"] = sp["source"].map(SOURCE_DISPLAY).fillna(sp["source"])
            fig2 = px.bar(sp, x="source_label", y="price", title="Median Price by Source",
                labels={"price":"Median Price (USD)","source_label":"Source"},
                color="source_label",
                color_discrete_map={SOURCE_DISPLAY.get(k,k):v for k,v in SOURCE_COLORS.items()},
                template=THEME)
            fig2.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Source × Category Price Heatmap")
        heat = df.groupby(["source","category"])["price"].median().reset_index()
        heat["source_label"]   = heat["source"].map(SOURCE_DISPLAY).fillna(heat["source"])
        heat["category_label"] = heat["category"].map(CAT_DISPLAY).fillna(heat["category"])
        hp = heat.pivot(index="source_label", columns="category_label", values="price")
        fig3 = go.Figure(data=go.Heatmap(
            z=hp.values, x=hp.columns.tolist(), y=hp.index.tolist(),
            colorscale="Blues",
            text=[[f"${v:,.0f}" if v==v else "—" for v in row] for row in hp.values],
            texttemplate="%{text}", textfont={"size":10}, hoverongaps=False,
        ))
        fig3.update_layout(title="Median Price ($) by Source × Category", height=340, xaxis=dict(tickangle=30))
        st.plotly_chart(fig3, use_container_width=True)

        if "is_sold" in df.columns:
            sell = (df.groupby("source").agg(sell_through=("is_sold","mean")).reset_index())
            sell["sell_through_pct"] = (sell["sell_through"]*100).round(1)
            sell["source_label"] = sell["source"].map(SOURCE_DISPLAY).fillna(sell["source"])
            fig4 = px.bar(sell.sort_values("sell_through_pct",ascending=True),
                x="sell_through_pct", y="source_label", orientation="h",
                title="Sell-Through Rate by Source (%)",
                labels={"sell_through_pct":"Sell-Through %","source_label":"Source"},
                color="source_label",
                color_discrete_map={SOURCE_DISPLAY.get(k,k):v for k,v in SOURCE_COLORS.items()},
                template=THEME)
            fig4.update_layout(showlegend=False, height=280)
            st.plotly_chart(fig4, use_container_width=True)


with tab3:
    st.subheader("Price Anomaly Detection")
    thresh = st.slider("Anomaly threshold (% from median)", 10, 100, 30, 5)
    if df.empty: st.info("No data for selected filters.")
    else:
        df_a = flag_anomalies(df.copy(), threshold=thresh/100)
        anoms = df_a[df_a["is_anomaly"]]
        c1,c2,c3 = st.columns(3)
        c1.metric("Anomalous Listings", f"{len(anoms):,}")
        c2.metric("Anomaly Rate", f"{len(anoms)/len(df)*100:.1f}%" if len(df) else "—")
        c3.metric("Max Deviation", f"{df_a['price_vs_median_pct'].abs().max()*100:+.0f}%" if len(df_a) else "—")

        sample = df_a.sample(min(3000,len(df_a)), random_state=42)
        sample["label"] = sample["is_anomaly"].map({True:"Anomaly",False:"Normal"})
        sample["cat_label"] = sample["category"].map(CAT_DISPLAY).fillna(sample["category"])
        fig = px.scatter(sample, x="listing_date", y="price", color="label",
            color_discrete_map={"Anomaly":"#ef4444","Normal":"#94a3b8"},
            opacity=0.55, title="Price Anomalies Over Time",
            labels={"price":"Price (USD)","listing_date":"Date"}, template=THEME)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

        if not anoms.empty:
            st.subheader("Top Anomalous Listings")
            disp = ["category","source","brand","title","price","price_vs_median_pct","condition"]
            disp = [c for c in disp if c in anoms.columns]
            top = anoms.sort_values("price_vs_median_pct",key=abs,ascending=False).head(25)[disp].copy()
            top["category"] = top["category"].map(CAT_DISPLAY).fillna(top["category"])
            top["source"]   = top["source"].map(SOURCE_DISPLAY).fillna(top["source"])
            if "price_vs_median_pct" in top.columns:
                top["price_vs_median_pct"] = top["price_vs_median_pct"].apply(lambda x: f"{x*100:+.1f}%")
            if "price" in top.columns:
                top["price"] = top["price"].apply(lambda x: f"${x:,.2f}")
            st.dataframe(top, use_container_width=True)


with tab4:
    st.subheader("Category Deep Dive")
    if df.empty: st.info("No data for selected filters.")
    else:
        cat_sum = df.groupby("category").agg(
            listings=("listing_id","count"),
            median_price=("price","median"),
            avg_price=("price","mean"),
            min_price=("price","min"),
            max_price=("price","max"),
            sell_through=("is_sold","mean") if "is_sold" in df.columns else ("listing_id","count"),
            avg_fee=("platform_fee","mean") if "platform_fee" in df.columns else ("listing_id","count"),
        ).reset_index()
        cat_sum["category"] = cat_sum["category"].map(CAT_DISPLAY).fillna(cat_sum["category"])
        cat_sum["sell_through"] = (cat_sum["sell_through"]*100).round(1)
        cat_sum = cat_sum.rename(columns={
            "category":"Category","listings":"Listings",
            "median_price":"Median $","avg_price":"Avg $",
            "min_price":"Min $","max_price":"Max $",
            "sell_through":"Sell-Through %","avg_fee":"Avg Fee $",
        })
        for c in ["Median $","Avg $","Min $","Max $","Avg Fee $"]:
            if c in cat_sum.columns:
                cat_sum[c] = cat_sum[c].apply(lambda x: f"${x:,.0f}")
        st.dataframe(cat_sum.sort_values("Listings",ascending=False), use_container_width=True)

        st.subheader("Top Brands by Median Price")
        sel_cat = st.selectbox("Select category", df["category"].unique(),
            format_func=lambda c: CAT_DISPLAY.get(c, c))
        if "brand" in df.columns:
            bdf = (df[df["category"]==sel_cat].groupby("brand")
                   .agg(listings=("listing_id","count"), median_price=("price","median"))
                   .reset_index().query("listings>=5").sort_values("median_price",ascending=False).head(15))
            fig = px.bar(bdf, x="brand", y="median_price",
                title=f"Top Brands — {CAT_DISPLAY.get(sel_cat,sel_cat)}",
                labels={"median_price":"Median Price (USD)","brand":"Brand"}, template=THEME)
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

        if "condition" in df.columns:
            cdf = (df.groupby(["category","condition"])["price"].median().reset_index())
            cdf["category"] = cdf["category"].map(CAT_DISPLAY).fillna(cdf["category"])
            fig2 = px.bar(cdf, x="category", y="price", color="condition", barmode="group",
                title="Median Price by Category & Condition",
                labels={"price":"Median Price (USD)","category":"Category"}, template=THEME)
            fig2.update_layout(height=360, xaxis_tickangle=30, legend=dict(orientation="h",y=-0.3))
            st.plotly_chart(fig2, use_container_width=True)


with tab5:
    st.subheader("Raw Listings")
    n = st.slider("Sample size", 100, min(10000,len(df)), min(1000,len(df)))
    sdf = df.sample(min(n,len(df)), random_state=42).copy()
    if "category" in sdf.columns: sdf["category"] = sdf["category"].map(CAT_DISPLAY).fillna(sdf["category"])
    if "source"   in sdf.columns: sdf["source"]   = sdf["source"].map(SOURCE_DISPLAY).fillna(sdf["source"])
    drop = ["listing_url","image_url","updated_at","ingested_at","batch_id","source_display","category_display"]
    sdf = sdf.drop(columns=[c for c in drop if c in sdf.columns])
    st.dataframe(sdf, use_container_width=True)
    st.download_button("📥 Download filtered data (CSV)",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"price_intelligence_{date_range[0]}_{date_range[1]}.csv",
        mime="text/csv")


st.divider()
st.caption(
    f"Price Intelligence v2  ·  "
    f"Last refreshed: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}  ·  "
    f"6 sources · 10 categories · {len(df_raw):,} total listings"
)