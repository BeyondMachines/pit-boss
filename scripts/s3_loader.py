"""
S3DataLoader — Fetches PR-Bouncer reviews and decisions from S3.

Understands the PR-Bouncer S3 key layout:
    reviews/YYYY/MM/DD/repo__org__PR-N__sha.json
    reviews/YYYY/MM/DD/repo__org__PR-N__sha__summary.json
    decisions/YYYY/MM/DD/repo__org__PR-N__command__author.json
    decisions/YYYY/MM.csv

v2 changes:
    - Reviews now contain new/existing finding separation
    - Multiple reviews per PR are expected (re-scans after fixes)
    - Supports loading a date range (weekly mode) or full month
"""

import json
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import boto3


class S3DataLoader:
    def __init__(self, bucket: str, region: str = None):
        self.bucket = bucket
        kwargs = {}
        if region:
            kwargs["region_name"] = region
        self.s3 = boto3.client("s3", **kwargs)

    def _list_keys(self, prefix: str) -> List[str]:
        """List all S3 keys under a prefix, handling pagination."""
        keys = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def _get_json(self, key: str) -> Dict:
        """Download and parse a single JSON object."""
        try:
            body = self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
            return json.loads(body)
        except Exception as e:
            print(f"  ⚠️ Failed to read {key}: {e}")
            return {}

    def _parse_review_key(self, key: str) -> Dict[str, str]:
        """
        Extract metadata from the S3 key path.
        Key format: reviews/YYYY/MM/DD/org__repo__PR-N__sha[__summary].json
        """
        filename = key.rsplit("/", 1)[-1].replace(".json", "")
        parts = filename.split("__")
        meta = {"key": key, "is_summary": filename.endswith("__summary")}
        if len(parts) >= 3:
            meta["repo"] = f"{parts[0]}/{parts[1]}" if len(parts) >= 4 else parts[0]
            for p in parts:
                if p.startswith("PR-"):
                    meta["pr_number"] = p.replace("PR-", "")
                    break
            # Extract SHA (the part after PR-N that isn't 'summary')
            pr_idx = next((i for i, p in enumerate(parts) if p.startswith("PR-")), -1)
            if pr_idx >= 0 and pr_idx + 1 < len(parts):
                sha_candidate = parts[pr_idx + 1]
                if sha_candidate != "summary":
                    meta["sha"] = sha_candidate
        return meta

    def _parse_decision_key(self, key: str) -> Dict[str, str]:
        """
        Extract metadata from decision key path.
        Key format: decisions/YYYY/MM/DD/org__repo__PR-N__command__author.json
        """
        filename = key.rsplit("/", 1)[-1].replace(".json", "")
        parts = filename.split("__")
        meta = {"key": key}
        if len(parts) >= 4:
            meta["repo"] = f"{parts[0]}/{parts[1]}" if "/" not in parts[0] else parts[0]
            for p in parts:
                if p.startswith("PR-"):
                    meta["pr_number"] = p.replace("PR-", "")
                    break
            if len(parts) >= 2:
                meta["command"] = parts[-2] if parts[-2] in ("accept-risk", "false-positive") else ""
                meta["author"] = parts[-1]
        return meta

    def _date_range_prefixes(self, year: int, month: int,
                             start_day: Optional[int] = None,
                             end_day: Optional[int] = None) -> List[str]:
        """
        Generate day-level S3 prefixes for a date range within a month.
        If start_day/end_day not given, covers the full month.
        """
        if start_day and end_day:
            return [
                f"{year}/{month:02d}/{d:02d}/"
                for d in range(start_day, end_day + 1)
            ]
        # Full month — just use the month prefix and let S3 list everything
        return [f"{year}/{month:02d}/"]

    def load_reviews(self, year: int, month: int,
                     start_day: Optional[int] = None,
                     end_day: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Load all review JSONs for a given period.
        Supports full month or day range (for weekly mode).
        """
        prefixes = self._date_range_prefixes(year, month, start_day, end_day)
        all_keys = []
        for pfx in prefixes:
            all_keys.extend(self._list_keys(f"reviews/{pfx}"))

        json_keys = [k for k in all_keys if k.endswith(".json")]
        print(f"   Found {len(json_keys)} review files")

        reviews = []
        for key in json_keys:
            key_meta = self._parse_review_key(key)
            data = self._get_json(key)
            if not data:
                continue

            review = {
                "s3_key": key,
                "is_summary": key_meta.get("is_summary", False),
                "key_repo": key_meta.get("repo", ""),
                "key_pr": key_meta.get("pr_number", ""),
                "key_sha": key_meta.get("sha", ""),
            }

            if "metadata" in data:
                review["metadata"] = data["metadata"]
            if "review" in data:
                review["review"] = data["review"]
            elif "review_summary" in data:
                review["review"] = data["review_summary"]

            reviews.append(review)

        return reviews

    def load_decisions(self, year: int, month: int,
                       start_day: Optional[int] = None,
                       end_day: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Load all decision JSONs for a given period.
        Supports full month or day range (for weekly mode).
        """
        prefixes = self._date_range_prefixes(year, month, start_day, end_day)
        all_keys = []
        for pfx in prefixes:
            all_keys.extend(self._list_keys(f"decisions/{pfx}"))

        json_keys = [k for k in all_keys if k.endswith(".json")]
        print(f"   Found {len(json_keys)} decision files")

        decisions = []
        for key in json_keys:
            key_meta = self._parse_decision_key(key)
            data = self._get_json(key)
            if not data:
                continue

            decision = {
                "s3_key": key,
                "type": data.get("type", key_meta.get("command", "")),
                "repo": data.get("repo", key_meta.get("repo", "")),
                "pr_number": str(data.get("pr_number", key_meta.get("pr_number", ""))),
                "author": data.get("author", key_meta.get("author", "")),
                "reasoning": data.get("reasoning", ""),
                "timestamp": data.get("timestamp", ""),
            }
            decisions.append(decision)

        return decisions

    def load_weekly_snapshot(self, year: int, month: int,
                             snapshot_key: str) -> Optional[Dict]:
        """Load a previously saved weekly snapshot from S3."""
        key = f"pitboss-snapshots/{year}-{month:02d}/{snapshot_key}.json"
        try:
            body = self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
            return json.loads(body)
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"  ⚠️ Failed to load snapshot {key}: {e}")
            return None

    def save_weekly_snapshot(self, year: int, month: int,
                             snapshot_key: str, data: Dict) -> bool:
        """Save a weekly snapshot to S3 for later aggregation."""
        key = f"pitboss-snapshots/{year}-{month:02d}/{snapshot_key}.json"
        try:
            self.s3.put_object(
                Bucket=self.bucket, Key=key,
                Body=json.dumps(data, indent=2),
                ContentType="application/json",
            )
            print(f"   Snapshot saved: s3://{self.bucket}/{key}")
            return True
        except Exception as e:
            print(f"  ⚠️ Failed to save snapshot: {e}")
            return False

    def list_weekly_snapshots(self, year: int, month: int) -> List[str]:
        """List all weekly snapshots for a month."""
        prefix = f"pitboss-snapshots/{year}-{month:02d}/"
        keys = self._list_keys(prefix)
        return [k for k in keys if k.endswith(".json")]

    def load_all_weekly_snapshots(self, year: int, month: int) -> List[Dict]:
        """Load all weekly snapshots for aggregation into monthly report."""
        keys = self.list_weekly_snapshots(year, month)
        snapshots = []
        for key in sorted(keys):
            data = self._get_json(key)
            if data:
                snapshots.append(data)
        print(f"   Loaded {len(snapshots)} weekly snapshots for {year}-{month:02d}")
        return snapshots