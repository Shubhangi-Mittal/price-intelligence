"""
price_intelligence_pipeline.py
Airflow DAG — Marketplace Price Intelligence Pipeline v2

Schedule: Daily at 6 AM UTC

Sources:    ebay · amazon · stockx · facebook_marketplace · goat · mercari
Categories: sneakers · trading_cards · electronics · furniture · watches
            handbags · vintage_clothing · video_games · collectibles · cameras

Tasks:
  1.  extract_marketplace_data  — Pull from all 6 sources (eBay API + mock others)
  2.  upload_to_gcs             — Upload parquet files to GCS
  3.  load_to_bigquery          — Load GCS → BQ raw tables
  4.  dbt_run_staging           — stg_listings (6 sources, 10 categories)
  5.  dbt_test_staging          — Schema + business rule tests
  6.  dbt_run_intermediate      — int_listings_enriched (benchmarks, anomalies)
  7.  dbt_run_marts             — mart_price_trends + mart_price_anomalies
  8.  dbt_test_marts            — Mart validation tests
  9.  check_freshness           — Verify <2hr SLA
  10. send_anomaly_alerts        — Slack alert on critical price spikes
  11. notify_success/failure     — Pipeline status notifications
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum

# ── Config ────────────────────────────────────────────────────────────────────
default_args = {
    "owner":                    "data-engineering",
    "depends_on_past":          False,
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  2,
    "retry_delay":              timedelta(minutes=5),
    "retry_exponential_backoff":True,
    "max_retry_delay":          timedelta(minutes=30),
}

GCP_PROJECT       = "{{ var.value.gcp_project_id }}"
GCS_BUCKET        = "{{ var.value.gcs_bucket }}"
BQ_DATASET_RAW    = "price_intelligence_raw"
BQ_DATASET_MARTS  = "price_intelligence_marts"
DBT_DIR           = "/opt/airflow/dbt_project"
SLACK_CONN        = "slack_price_intel"

ALL_SOURCES = ["ebay", "amazon", "stockx", "facebook_marketplace", "goat", "mercari"]

ALL_CATEGORIES = [
    "sneakers", "trading_cards", "electronics", "furniture", "watches",
    "handbags", "vintage_clothing", "video_games", "collectibles", "cameras",
]


# ── Callables ─────────────────────────────────────────────────────────────────
def extract_marketplace_data(**context):
    """
    Extract listings from all 6 sources.
    - eBay: real API (falls back to mock if no key)
    - Others: mock feed generator (plug in real APIs as you get keys)
    """
    import sys
    sys.path.insert(0, "/opt/airflow/ingestion")
    from ingestion.ebay_extractor import EbayExtractor
    from ingestion.mock_feed_generator import generate_dataset

    run_date = context["ds"]
    summary  = {"sources": {}, "total_extracted": 0}

    # eBay — live or mock
    extractor = EbayExtractor(output_dir=f"/tmp/raw_data/{run_date}")
    ebay_summary = extractor.run(categories=ALL_CATEGORIES, max_pages=5)
    summary["sources"]["ebay"]   = ebay_summary
    summary["total_extracted"]  += ebay_summary.get("total_extracted", 0)

    # Other sources — mock feed (replace with real API clients as available)
    for source in ["amazon", "stockx", "facebook_marketplace", "goat", "mercari"]:
        generate_dataset(
            days=1,
            records_per_day=80,
            output_dir=f"/tmp/raw_data/{run_date}/{source}",
            inject_issues=False,
        )
        summary["sources"][source] = {"status": "mock_generated"}

    context["task_instance"].xcom_push(key="extraction_summary", value=summary)
    return summary


def upload_to_storage(**context):
    """Upload all source/category parquet files to GCS."""
    import sys
    sys.path.insert(0, "/opt/airflow/ingestion")
    from ingestion.gcs_uploader import get_uploader
    from pathlib import Path

    run_date = context["ds"]
    uploader = get_uploader()
    results  = []

    for source in ALL_SOURCES:
        for category in ALL_CATEGORIES:
            local_dir = Path(f"/tmp/raw_data/{run_date}/{source}/{category}")
            if local_dir.exists():
                r = uploader.upload_directory(local_dir, source, category, run_date)
                results.extend(r)

    uploaded = sum(1 for r in results if r.get("uploaded"))
    context["task_instance"].xcom_push(key="upload_count", value=uploaded)
    return {"uploaded": uploaded, "total": len(results)}


def check_freshness_sla(**context):
    """Verify mart data freshness < 2 hours."""
    from google.cloud import bigquery

    client = bigquery.Client()
    query  = f"""
        SELECT
            MAX(dbt_updated_at) as latest_update,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(dbt_updated_at), MINUTE) as minutes_stale
        FROM `{GCP_PROJECT}.{BQ_DATASET_MARTS}.mart_price_trends`
    """
    row = list(client.query(query).result())[0]
    SLA = 120
    if row.minutes_stale and row.minutes_stale > SLA:
        raise Exception(
            f"Freshness SLA breached: data is {row.minutes_stale:.0f} min stale (threshold: {SLA})"
        )
    print(f"✅ Freshness OK: {row.minutes_stale:.0f} min old")
    return {"minutes_stale": row.minutes_stale}


def send_anomaly_alerts(**context):
    """Query critical anomalies across all categories/sources and Slack-alert."""
    from google.cloud import bigquery

    client   = bigquery.Client()
    run_date = context["ds"]
    query    = f"""
        SELECT
            category_display,
            source_display,
            COUNT(*) as anomaly_count,
            ROUND(AVG(ABS(price_vs_median_pct)) * 100, 1) as avg_deviation_pct,
            ROUND(MAX(ABS(price_vs_median_pct)) * 100, 1) as max_deviation_pct,
            COUNTIF(anomaly_severity = 'critical') as critical_count
        FROM `{GCP_PROJECT}.{BQ_DATASET_MARTS}.mart_price_anomalies`
        WHERE listing_date = '{run_date}'
          AND anomaly_severity IN ('critical', 'high')
        GROUP BY 1, 2
        ORDER BY critical_count DESC, anomaly_count DESC
        LIMIT 15
    """
    results = list(client.query(query).result())
    if not results:
        print("No critical anomalies today.")
        return

    lines = [f"🚨 *Price Anomaly Alert — {run_date}*\n"]
    for r in results:
        lines.append(
            f"• *{r.category_display}* / {r.source_display}: "
            f"{r.anomaly_count} anomalies "
            f"(avg {r.avg_deviation_pct}%, max {r.max_deviation_pct}%)"
        )

    msg = "\n".join(lines)
    context["task_instance"].xcom_push(key="anomaly_message", value=msg)
    print(msg)
    return msg


def build_success_message(**context):
    run_date = context["ds"]
    summary  = context["task_instance"].xcom_pull(
        task_ids="extract_marketplace_data", key="extraction_summary"
    ) or {}
    total    = summary.get("total_extracted", "N/A")
    sources  = len(summary.get("sources", ALL_SOURCES))
    return (
        f"✅ *Price Intelligence Pipeline v2 Complete*\n"
        f"Date: {run_date} | Records: {total:,} | Sources: {sources} | "
        f"Categories: {len(ALL_CATEGORIES)}"
    )


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="price_intelligence_pipeline_v2",
    default_args=default_args,
    description="Marketplace Price Intelligence v2: 6 sources × 10 categories",
    schedule_interval="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["price-intelligence", "production", "v2"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. Ingest
    extract = PythonOperator(
        task_id="extract_marketplace_data",
        python_callable=extract_marketplace_data,
    )

    # 2. Upload
    upload = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_storage,
    )

    # 3. Load BQ
    load_bq = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=["raw/*/*/{{ ds }}/*.parquet"],   # covers all sources + categories
        destination_project_dataset_table=f"{BQ_DATASET_RAW}.listings",
        source_format="PARQUET",
        write_disposition="WRITE_APPEND",
        autodetect=True,
        time_partitioning={"type": "DAY", "field": "ingested_at"},
    )

    # 4-8. dbt
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging --target prod_bq",
    )
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_DIR} && dbt test --select staging --target prod_bq",
    )
    dbt_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"cd {DBT_DIR} && dbt run --select intermediate --target prod_bq",
    )
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select marts --target prod_bq",
    )
    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"cd {DBT_DIR} && dbt test --select marts --target prod_bq",
    )

    # 9-10. Quality
    freshness = PythonOperator(task_id="check_freshness_sla",  python_callable=check_freshness_sla)
    alerts    = PythonOperator(task_id="send_anomaly_alerts",  python_callable=send_anomaly_alerts)

    # 11. Notify
    build_msg       = PythonOperator(task_id="build_success_message", python_callable=build_success_message)
    notify_success  = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id=SLACK_CONN,
        message="{{ task_instance.xcom_pull(task_ids='build_success_message') }}",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    notify_failure  = SlackWebhookOperator(
        task_id="notify_failure",
        slack_webhook_conn_id=SLACK_CONN,
        message="❌ *Price Intelligence Pipeline FAILED* on {{ ds }}",
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # ── Dependencies ──────────────────────────────────────────────────────────
    (
        start >> extract >> upload >> load_bq
        >> dbt_staging >> dbt_test_staging
        >> dbt_intermediate
        >> dbt_marts >> dbt_test_marts
        >> freshness >> alerts >> build_msg >> notify_success >> end
    )
    [extract, upload, load_bq, dbt_staging, dbt_marts] >> notify_failure >> end
