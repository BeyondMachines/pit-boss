#!/usr/bin/env python3
"""
Iterate over PB_PERIODS JSON and invoke pitboss.py once per period.

Environment:
    PB_PERIODS    JSON array of [year, month, start_day, end_day] tuples.
    PB_MODE       One of: monthly, weekly, monthly-aggregate.
    PB_THRESHOLD  Integer.
    PB_USE_LLM    "true" or "false".
    S3_BUCKET     S3 bucket name.
"""

import json
import os
import subprocess
import sys


def main() -> int:
    raw = os.environ.get("PB_PERIODS", "[]")
    try:
        periods = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"PB_PERIODS is not valid JSON: {e}", file=sys.stderr)
        return 1

    if not isinstance(periods, list) or not periods:
        print("PB_PERIODS must be a non-empty list", file=sys.stderr)
        return 1

    mode = os.environ["PB_MODE"]
    threshold = os.environ["PB_THRESHOLD"]
    use_llm = os.environ.get("PB_USE_LLM", "true")
    s3_bucket = os.environ["S3_BUCKET"]

    for p in periods:
        if not isinstance(p, list) or len(p) != 4:
            print(f"Invalid period entry: {p}", file=sys.stderr)
            return 1
        if not all(isinstance(v, int) and v >= 0 for v in p):
            print(f"Period values must be non-negative integers: {p}",
                  file=sys.stderr)
            return 1

        year, month, start_day, end_day = p
        print()
        print(f"Running pit-boss for {year}-{month:02d} "
              f"days {start_day}-{end_day}")

        args = [
            "python", "pitboss.py",
            "--year", str(year),
            "--month", str(month),
            "--shakedown-threshold", str(threshold),
            "--output-dir", "../../pitboss-output",
            "--s3-bucket", s3_bucket,
            "--mode", mode,
        ]

        if use_llm == "false":
            args.append("--no-llm")

        if mode == "weekly":
            args.extend(["--start-day", str(start_day),
                         "--end-day", str(end_day)])

        result = subprocess.run(args, check=False)
        if result.returncode != 0:
            print(f"pit-boss exited {result.returncode} for period {p}",
                  file=sys.stderr)
            return result.returncode

    return 0


if __name__ == "__main__":
    sys.exit(main())