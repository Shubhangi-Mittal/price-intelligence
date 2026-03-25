"""
monitor.py
Pipeline monitoring: freshness, latency, anomaly alerting.

Supports all 6 sources and 10 categories.

Usage:
    python monitoring/monitor.py --check all
    python monitoring/monitor.py --check freshness
    python monitoring/monitor.py --check anomalies --date 2024-01-15
    python monitoring/monitor.py --check coverage   # source × category coverage gaps
"""

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

ALL_SOURCES    = ["ebay", "amazon", "stockx", "facebook_marketplace", "goat", "mercari"]
ALL_CATEGORIES = [
    "sneakers", "trading_cards", "electronics", "furniture", "watches",
    "handbags", "vintage_clothing", "video_games", "collectibles", "cameras",
]


@dataclass
class FreshnessCheck:
    table:              str
    latest_update:      Optional[datetime]
    minutes_stale:      Optional[float]
    warn_threshold_min: int = 120
    error_threshold_min:int = 480
    status:             str = field(init=False)

    def __post_init__(self):
        if   self.minutes_stale is None:                             self.status = "unknown"
        elif self.minutes_stale > self.error_threshold_min:          self.status = "error"
        elif self.minutes_stale > self.warn_threshold_min:           self.status = "warn"
        else:                                                         self.status = "ok"


@dataclass
class AnomalySummary:
    date:               str
    category:           str
    source:             str
    critical_count:     int
    high_count:         int
    avg_deviation_pct:  float
    max_deviation_pct:  float


class PipelineMonitor:
    def __init__(self):
        self.bq           = self._init_bq()
        self.slack_on     = bool(SLACK_WEBHOOK_URL)

    def _init_bq(self):
        try:
            from google.cloud import bigquery
            return bigquery.Client()
        except Exception:
            logger.warning("BigQuery unavailable — mock mode.")
            return None

    def _query(self, sql: str) -> list:
        if not self.bq:
            logger.warning(f"[MOCK] Would run:\n{sql}")
            return []
        return [dict(row) for row in self.bq.query(sql).result()]

    # ── Freshness ─────────────────────────────────────────────────────────────
    def check_freshness(self, project: str, dataset: str, table: str) -> FreshnessCheck:
        rows = self._query(f"""
            SELECT MAX(dbt_updated_at) as latest_update,
                   TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(dbt_updated_at), MINUTE) as minutes_stale
            FROM `{project}.{dataset}.{table}`
        """)
        if rows and rows[0]["latest_update"]:
            r = rows[0]
            return FreshnessCheck(f"{dataset}.{table}", r["latest_update"], r["minutes_stale"])
        return FreshnessCheck(f"{dataset}.{table}", None, None)

    def run_freshness_checks(self, project: str) -> list[FreshnessCheck]:
        tables = [
            ("price_intelligence_marts", "mart_price_trends"),
            ("price_intelligence_marts", "mart_price_anomalies"),
            ("price_intelligence_raw",   "listings"),
        ]
        results = []
        for dataset, table in tables:
            c    = self.check_freshness(project, dataset, table)
            icon = {"ok":"✅","warn":"⚠️","error":"❌","unknown":"❓"}[c.status]
            msg  = f"{icon} {c.table}: {c.minutes_stale:.0f}min stale [{c.status.upper()}]" \
                   if c.minutes_stale else f"❓ {c.table}: unknown"
            logger.info(msg)
            results.append(c)
        return results

    # ── Source × Category coverage ────────────────────────────────────────────
    def check_coverage(self, project: str, date: str) -> dict:
        """Detect source × category gaps in today's data."""
        rows = self._query(f"""
            SELECT source, category, COUNT(*) as listing_count
            FROM `{project}.price_intelligence_raw.listings`
            WHERE DATE(ingested_at) = '{date}'
            GROUP BY 1, 2
        """)
        present = {(r["source"], r["category"]) for r in rows}
        expected = {(s, c) for s in ALL_SOURCES for c in ALL_CATEGORIES}
        missing  = sorted(expected - present)

        if missing:
            logger.warning(f"⚠️  {len(missing)} source×category gaps today:")
            for s, c in missing[:10]:
                logger.warning(f"   • {s} / {c}")
        else:
            logger.success("✅ All 60 source×category combinations present")

        return {"present": len(present), "missing": missing, "total_expected": 60}

    # ── Anomalies ─────────────────────────────────────────────────────────────
    def get_anomaly_summary(self, project: str, date: str) -> list[AnomalySummary]:
        rows = self._query(f"""
            SELECT listing_date, category, source,
                   COUNTIF(anomaly_severity = 'critical') as critical_count,
                   COUNTIF(anomaly_severity = 'high')     as high_count,
                   ROUND(AVG(ABS(price_vs_median_pct))*100,2) as avg_deviation_pct,
                   ROUND(MAX(ABS(price_vs_median_pct))*100,2) as max_deviation_pct
            FROM `{project}.price_intelligence_marts.mart_price_anomalies`
            WHERE DATE(listing_date) = '{date}'
            GROUP BY 1,2,3
            HAVING critical_count + high_count > 0
            ORDER BY critical_count DESC
        """)
        return [AnomalySummary(**{k: row[k] for k in AnomalySummary.__dataclass_fields__}) for row in rows]

    def get_pipeline_metrics(self, project: str, date: str) -> dict:
        rows = self._query(f"""
            SELECT
                SUM(total_listings)                   as total_listings,
                SUM(sold_listings)                    as total_sold,
                SUM(anomalous_listings)               as total_anomalies,
                COUNT(DISTINCT source)                as source_count,
                COUNT(DISTINCT category)              as category_count,
                ROUND(AVG(sell_through_rate)*100,2)  as avg_sell_through_pct,
                ROUND(AVG(avg_fee_pct),2)             as avg_platform_fee_pct
            FROM `{project}.price_intelligence_marts.mart_price_trends`
            WHERE listing_date = '{date}'
        """)
        return rows[0] if rows else {}

    # ── Slack ─────────────────────────────────────────────────────────────────
    def slack(self, message: str, level: str = "info") -> bool:
        if not self.slack_on:
            logger.info(f"[SLACK MOCK] {message}")
            return True
        import requests
        icons = {"info":"ℹ️","warning":"⚠️","error":"🚨","success":"✅"}
        try:
            requests.post(
                SLACK_WEBHOOK_URL,
                json={"text": f"{icons.get(level,'📊')} *Price Intelligence*\n{message}", "mrkdwn": True},
                timeout=10,
            ).raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Slack failed: {e}")
            return False

    def alert_freshness(self, checks: list[FreshnessCheck]):
        errors = [c for c in checks if c.status == "error"]
        warns  = [c for c in checks if c.status == "warn"]
        if errors:
            self.slack("🚨 *Freshness ERROR*\n" + "\n".join(f"  • `{c.table}`: {c.minutes_stale:.0f}min" for c in errors), "error")
        elif warns:
            self.slack("⚠️ *Freshness WARNING*\n" + "\n".join(f"  • `{c.table}`: {c.minutes_stale:.0f}min" for c in warns), "warning")

    def alert_anomalies(self, anomalies: list[AnomalySummary], date: str):
        critical = [a for a in anomalies if a.critical_count > 0]
        if not critical:
            return
        lines = [f"🚨 *Critical Anomalies — {date}*"]
        for a in critical[:8]:
            lines.append(f"  • *{a.category.title()}* / {a.source}: {a.critical_count} critical, max {a.max_deviation_pct:.0f}%")
        if len(critical) > 8:
            lines.append(f"  • ...and {len(critical)-8} more")
        self.slack("\n".join(lines), "error")

    def daily_summary(self, metrics: dict, coverage: dict, anomaly_count: int, date: str):
        gaps = len(coverage.get("missing", []))
        self.slack(
            f"*Daily Summary — {date}*\n"
            f"  • Listings: {metrics.get('total_listings',0):,}\n"
            f"  • Sources active: {metrics.get('source_count',0)} / {len(ALL_SOURCES)}\n"
            f"  • Categories active: {metrics.get('category_count',0)} / {len(ALL_CATEGORIES)}\n"
            f"  • Sell-through: {metrics.get('avg_sell_through_pct',0):.1f}%\n"
            f"  • Avg platform fee: {metrics.get('avg_platform_fee_pct',0):.1f}%\n"
            f"  • Anomalies: {anomaly_count} | Coverage gaps: {gaps}",
            "success",
        )

    # ── Main ──────────────────────────────────────────────────────────────────
    def run(self, project: str, date: str, checks: list[str] = None) -> dict:
        checks  = checks or ["freshness", "coverage", "anomalies", "metrics"]
        results = {"date": date, "project": project, "checks": {}}

        if "freshness" in checks:
            fc = self.run_freshness_checks(project)
            self.alert_freshness(fc)
            results["checks"]["freshness"] = [{"table": c.table, "status": c.status, "minutes_stale": c.minutes_stale} for c in fc]

        if "coverage" in checks:
            cov = self.check_coverage(project, date)
            results["checks"]["coverage"] = cov

        if "anomalies" in checks:
            anoms = self.get_anomaly_summary(project, date)
            self.alert_anomalies(anoms, date)
            results["checks"]["anomalies"] = len(anoms)

        if "metrics" in checks:
            metrics  = self.get_pipeline_metrics(project, date)
            coverage = results["checks"].get("coverage", {})
            self.daily_summary(metrics, coverage, results["checks"].get("anomalies", 0), date)
            results["checks"]["metrics"] = metrics

        logger.success("Monitoring run complete")
        return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT_ID", "dev-project"))
    parser.add_argument("--date",    default=str(datetime.utcnow().date()))
    parser.add_argument("--check",   nargs="+", default=["freshness","coverage","anomalies","metrics"])
    args = parser.parse_args()

    monitor = PipelineMonitor()
    results = monitor.run(project=args.project, date=args.date, checks=args.check)
    print(json.dumps(results, indent=2, default=str))
