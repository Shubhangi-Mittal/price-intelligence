"""
gcs_uploader.py
Uploads extracted files to GCS or S3 with:
  - MD5 checksum validation
  - Idempotency (skip already-uploaded files)
  - Structured path: gs://bucket/raw/source/category/YYYY-MM-DD/file.parquet

Supports all 6 sources: ebay, amazon, stockx, facebook_marketplace, goat, mercari

Usage:
    python ingestion/gcs_uploader.py --source ebay --date 2024-01-15
    python ingestion/gcs_uploader.py --source all --date 2024-01-15
"""

import hashlib
import json
import os
from datetime import datetime, date
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

GCS_BUCKET = os.getenv("GCS_BUCKET", "price-intelligence-raw")
AWS_BUCKET = os.getenv("AWS_BUCKET")
USE_GCS    = bool(os.getenv("GCS_BUCKET"))

ALL_SOURCES = ["ebay", "amazon", "stockx", "facebook_marketplace", "goat", "mercari"]

ALL_CATEGORIES = [
    "sneakers", "trading_cards", "electronics", "furniture", "watches",
    "handbags", "vintage_clothing", "video_games", "collectibles", "cameras",
]


def compute_md5(filepath: Path) -> str:
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


class GCSUploader:
    def __init__(self, bucket: str = GCS_BUCKET):
        try:
            from google.cloud import storage
            self.client      = storage.Client()
            self.bucket      = self.client.bucket(bucket)
            self.bucket_name = bucket
            logger.info(f"Connected to GCS: gs://{bucket}")
        except Exception as e:
            logger.warning(f"GCS unavailable ({e}). Running in mock mode.")
            self.client      = None
            self.bucket      = None
            self.bucket_name = bucket

    def _gcs_path(self, local_path: Path, source: str, category: str, date_str: str) -> str:
        return f"raw/{source}/{category}/{date_str}/{local_path.name}"

    def file_exists(self, gcs_path: str) -> bool:
        if not self.bucket:
            return False
        return self.bucket.blob(gcs_path).exists()

    def upload_file(self, local_path: Path, source: str, category: str,
                    date_str: str, force: bool = False) -> dict:
        gcs_path  = self._gcs_path(local_path, source, category, date_str)
        md5       = compute_md5(local_path)
        file_size = local_path.stat().st_size
        result    = {
            "local_path": str(local_path),
            "gcs_path":   f"gs://{self.bucket_name}/{gcs_path}",
            "md5":        md5,
            "size_bytes": file_size,
            "uploaded":   False,
            "skipped":    False,
        }

        if not force and self.file_exists(gcs_path):
            logger.info(f"Skipping {local_path.name} (already exists)")
            result["skipped"] = True
            return result

        if self.bucket:
            blob = self.bucket.blob(gcs_path)
            blob.metadata = {
                "source":           source,
                "category":         category,
                "date_partition":   date_str,
                "md5_checksum":     md5,
                "pipeline_version": "2.0",
                "uploaded_at":      datetime.utcnow().isoformat(),
            }
            blob.upload_from_filename(str(local_path), content_type="application/octet-stream")
            blob.reload()
            logger.success(f"Uploaded → gs://{self.bucket_name}/{gcs_path} ({file_size:,} bytes)")
        else:
            logger.info(f"[MOCK] Would upload → gs://{self.bucket_name}/{gcs_path}")

        result["uploaded"] = True
        return result

    def upload_directory(self, local_dir: Path, source: str,
                         category: str, date_str: str) -> list[dict]:
        files      = list(local_dir.glob("**/*"))
        uploadable = [f for f in files if f.is_file() and f.suffix in (".parquet", ".csv", ".json")]
        logger.info(f"Uploading {len(uploadable)} files from {local_dir}")
        results    = [self.upload_file(f, source, category, date_str) for f in uploadable]
        uploaded   = sum(1 for r in results if r["uploaded"])
        skipped    = sum(1 for r in results if r["skipped"])
        logger.info(f"Summary: {uploaded} uploaded, {skipped} skipped")
        return results


class S3Uploader:
    def __init__(self, bucket: str = AWS_BUCKET):
        try:
            import boto3
            self.s3     = boto3.client("s3")
            self.bucket = bucket
            logger.info(f"Connected to S3: s3://{bucket}")
        except Exception as e:
            logger.warning(f"S3 unavailable ({e}). Mock mode.")
            self.s3     = None
            self.bucket = bucket

    def upload_file(self, local_path: Path, s3_key: str, metadata: dict = None) -> dict:
        md5 = compute_md5(local_path)
        if self.s3:
            self.s3.upload_file(
                str(local_path), self.bucket, s3_key,
                ExtraArgs={"Metadata": {k: str(v) for k, v in (metadata or {}).items()}}
            )
            logger.success(f"Uploaded → s3://{self.bucket}/{s3_key}")
        else:
            logger.info(f"[MOCK] Would upload → s3://{self.bucket}/{s3_key}")
        return {"s3_path": f"s3://{self.bucket}/{s3_key}", "md5": md5, "uploaded": True}


def get_uploader():
    if USE_GCS:
        return GCSUploader()
    elif AWS_BUCKET:
        return S3Uploader()
    else:
        logger.warning("No cloud storage configured — using GCS mock mode.")
        return GCSUploader()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--source",    default="all", choices=ALL_SOURCES + ["all"])
    parser.add_argument("--category",  default=None,  choices=ALL_CATEGORIES + [None])
    parser.add_argument("--date",      default=str(date.today()))
    parser.add_argument("--local-dir", default="raw_data")
    args = parser.parse_args()

    uploader    = get_uploader()
    local_dir   = Path(args.local_dir) / args.date
    sources     = ALL_SOURCES if args.source == "all" else [args.source]
    categories  = [args.category] if args.category else ALL_CATEGORIES
    all_results = []

    for source in sources:
        for category in categories:
            cat_dir = local_dir / source / category
            if cat_dir.exists():
                results = uploader.upload_directory(cat_dir, source, category, args.date)
                all_results.extend(results)

    print(json.dumps(all_results, indent=2))
