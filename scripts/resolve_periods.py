#!/usr/bin/env python3
"""
Resolve pit-boss analysis periods from workflow inputs.

Reads INPUT_* environment variables set by the calling GHA step.
Writes period data to GITHUB_OUTPUT in the form:
    periods=[[year, month, start_day, end_day], ...]
    count=<N>
    first_year=<year>
    first_month=<month>

For weekly mode with auto-resolution, splits cross-month weeks into two
periods so each snapshot lands in its correct month folder and no
PR-Bouncer reviews are dropped.

For monthly and monthly-aggregate modes, emits a single period.
"""

import calendar
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import List


def resolve_periods(
    mode: str,
    input_year: int,
    input_month: int,
    input_start_day: int,
    input_end_day: int,
    now: datetime,
) -> List[List[int]]:
    """
    Return a list of [year, month, start_day, end_day] tuples to analyze.

    Same-month weekly and monthly modes produce a single period.
    Cross-month weekly (auto-resolved) produces two periods.
    """
    periods: List[List[int]] = []

    if mode == "weekly":
        if input_start_day > 0 and input_end_day > 0:
            # Manual override — trust the user, single period
            year = input_year if input_year > 0 else now.year
            month = input_month if input_month > 0 else now.month
            periods.append([year, month, input_start_day, input_end_day])
        else:
            # Auto: previous full Monday-Sunday week, regardless of which
            # weekday this runs on. now.weekday() returns 0 for Monday.
            days_since_monday = now.weekday()
            prev_monday = now - timedelta(days=days_since_monday + 7)
            prev_sunday = prev_monday + timedelta(days=6)

            if prev_monday.month == prev_sunday.month:
                periods.append([
                    prev_monday.year, prev_monday.month,
                    prev_monday.day, prev_sunday.day,
                ])
            else:
                last_day = calendar.monthrange(
                    prev_monday.year, prev_monday.month
                )[1]
                periods.append([
                    prev_monday.year, prev_monday.month,
                    prev_monday.day, last_day,
                ])
                periods.append([
                    prev_sunday.year, prev_sunday.month,
                    1, prev_sunday.day,
                ])
    else:
        # Monthly / monthly-aggregate
        if input_year > 0 and input_month > 0:
            year, month = input_year, input_month
        elif now.month > 1:
            year, month = now.year, now.month - 1
        else:
            year, month = now.year - 1, 12
        periods.append([year, month, 0, 0])

    return periods


def _read_env_int(name: str, default: int = 0) -> int:
    """Read an env var as int. Empty or unset returns default."""
    raw = os.environ.get(name, "")
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"Invalid integer value for {name}: {raw!r}", file=sys.stderr)
        sys.exit(1)


def main() -> int:
    mode = os.environ.get("INPUT_MODE", "monthly")
    if mode not in ("monthly", "weekly", "monthly-aggregate"):
        print(f"Invalid mode: {mode!r}", file=sys.stderr)
        return 1

    periods = resolve_periods(
        mode=mode,
        input_year=_read_env_int("INPUT_YEAR"),
        input_month=_read_env_int("INPUT_MONTH"),
        input_start_day=_read_env_int("INPUT_START_DAY"),
        input_end_day=_read_env_int("INPUT_END_DAY"),
        now=datetime.now(timezone.utc),
    )

    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        # When run outside GHA, print to stdout for debugging
        print(json.dumps(periods, indent=2))
        return 0

    with open(output_path, "a") as fh:
        fh.write(f"periods={json.dumps(periods)}\n")
        fh.write(f"count={len(periods)}\n")
        fh.write(f"first_year={periods[0][0]}\n")
        fh.write(f"first_month={periods[0][1]}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())