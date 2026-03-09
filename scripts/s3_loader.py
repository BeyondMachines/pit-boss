"""
S3DataLoader — Fetches PR-Bouncer reviews and decisions from S3.

Understands the PR-Bouncer S3 key layout:
    reviews/YYYY/MM/DD/repo__org__PR-N__sha.json
    reviews/YYYY/MM/DD/repo__org__PR-N__sha__summary.json
    decisions/YYYY/MM/DD/repo__org__PR-N__command__author.json
    decisions/YYYY/MM.csv
"""

import json
import re
from typing import Dict, List, Any

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
            # Find the PR-N part
            for p in parts:
                if p.startswith("PR-"):
                    meta["pr_number"] = p.replace("PR-", "")
                    break
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
            # command is second to last, author is last
            if len(parts) >= 2:
                meta["command"] = parts[-2] if parts[-2] in ("accept-risk", "false-positive") else ""
                meta["author"] = parts[-1]
        return meta

    def load_reviews(self, year: int, month: int) -> List[Dict[str, Any]]:
        """
        Load all review JSONs for a given month.
        Iterates day prefixes (01-31) to get all reviews.
        """
        prefix = f"reviews/{year}/{month:02d}/"
        keys = self._list_keys(prefix)
        print(f"   Found {len(keys)} review files")

        reviews = []
        for key in keys:
            if not key.endswith(".json"):
                continue
            key_meta = self._parse_review_key(key)
            data = self._get_json(key)
            if not data:
                continue

            # Merge key-derived metadata with the JSON content
            review = {
                "s3_key": key,
                "is_summary": key_meta.get("is_summary", False),
                "key_repo": key_meta.get("repo", ""),
                "key_pr": key_meta.get("pr_number", ""),
            }

            # The JSON has metadata.* and review.* (full) or review_summary.* (summary)
            if "metadata" in data:
                review["metadata"] = data["metadata"]
            if "review" in data:
                review["review"] = data["review"]
            elif "review_summary" in data:
                review["review"] = data["review_summary"]

            reviews.append(review)

        return reviews

    def load_decisions(self, year: int, month: int) -> List[Dict[str, Any]]:
        """
        Load all decision JSONs for a given month.
        Also tries to load the decisions CSV as a fallback/supplement.
        """
        prefix = f"decisions/{year}/{month:02d}/"
        keys = [k for k in self._list_keys(prefix) if k.endswith(".json")]
        print(f"   Found {len(keys)} decision files")

        decisions = []
        for key in keys:
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