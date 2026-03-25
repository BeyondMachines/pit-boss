#!/usr/bin/env python3
"""
pit-boss — Reviews PR-Bouncer's output from S3.

Reads reviews and decisions, correlates findings with accept-risk/false-positive
overrides, produces:
  1. A meeting report (Markdown) with stats, trends, and discussion points
  2. A shakedown candidates list (JSON) for repo-shakedown

Supports two modes:
  - Weekly: Analyzes a date range, saves a compact snapshot to S3 (or locally).
    Designed to limit token usage and spread work across the month.
  - Monthly: Either analyzes the full month directly, or aggregates previously
    saved weekly snapshots into a combined report.

Deterministic correlation + optional Gemini-powered narrative analysis.

Usage:
    # Monthly — analyze full month (default)
    python pitboss.py
    python pitboss.py --year 2026 --month 2

    # Weekly — analyze a specific week
    python pitboss.py --mode weekly --start-day 1 --end-day 7

    # Monthly — aggregate from weekly snapshots
    python pitboss.py --mode monthly-aggregate

    # Local only — skip S3 upload
    python pitboss.py --local

    # No LLM — deterministic only
    python pitboss.py --no-llm

Required env vars:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    S3_BUCKET (default: bm-pr-reviews)

Optional:
    AWS_DEFAULT_REGION (default: us-east-1)
    GEMINI_API_KEY (enables AI narrative, override eval, shakedown reasoning)
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict


from dotenv import load_dotenv

# Load .env from repo root (local dev)
_env_file = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_env_file)

from s3_loader import S3DataLoader
from correlator import ReviewCorrelator
from report_generator import ReportGenerator
from shakedown_candidates import ShakedownCandidateBuilder


def parse_args():
    p = argparse.ArgumentParser(description="pit-boss: PR-Bouncer analysis")

    p.add_argument("--year", type=int, default=int(os.environ.get("PB_YEAR", 0)))
    p.add_argument("--month", type=int, default=int(os.environ.get("PB_MONTH", 0)))
    p.add_argument("--output-dir", type=str, default="./pitboss-output")
    p.add_argument("--shakedown-threshold", type=int,
                   default=int(os.environ.get("PB_THRESHOLD", 7)),
                   help="Minimum risk score for shakedown candidates (default: 7)")
    p.add_argument("--s3-bucket", type=str,
                   default=os.environ.get("S3_BUCKET", "bm-pr-reviews"))
    p.add_argument("--no-llm", action="store_true",
                   help="Skip Gemini analysis, deterministic report only")
    p.add_argument("--local", action="store_true",
                   help="Local only — skip S3 uploads, write files to output-dir")

    # Mode selection
    p.add_argument("--mode", type=str, default="monthly",
                   choices=["monthly", "weekly", "monthly-aggregate"],
                   help="Run mode: monthly (full month), weekly (date range), "
                        "monthly-aggregate (combine weekly snapshots)")
    p.add_argument("--start-day", type=int, default=None,
                   help="Start day for weekly mode (1-31)")
    p.add_argument("--end-day", type=int, default=None,
                   help="End day for weekly mode (1-31)")

    return p.parse_args()


def run_weekly(args, loader: S3DataLoader, output_dir: Path) -> int:
    """Run weekly analysis on a date range. Saves a snapshot."""
    if not args.start_day or not args.end_day:
        print("❌ Weekly mode requires --start-day and --end-day")
        return 1

    week_label = f"{args.year}-{args.month:02d}-d{args.start_day:02d}-{args.end_day:02d}"
    print(f"\n📅 Weekly mode: days {args.start_day}-{args.end_day}")

    # Load data for date range
    print(f"\n📥 Loading data for {week_label} ...")
    reviews = loader.load_reviews(args.year, args.month, args.start_day, args.end_day)
    decisions = loader.load_decisions(args.year, args.month, args.start_day, args.end_day)

    print(f"   Reviews loaded:  {len(reviews)}")
    print(f"   Decisions loaded: {len(decisions)}")

    if not reviews:
        print("\n⚠️  No reviews found for this period.")
        return 0

    # Correlate
    print("\n🔗 Correlating ...")
    correlator = ReviewCorrelator(reviews, decisions)
    analysis = correlator.analyze()
    snapshot = correlator.to_snapshot()
    snapshot["week_label"] = week_label

    _print_summary(analysis)

    # LLM analysis (optional — may skip for weekly to save tokens)
    llm_results = {}
    if not args.no_llm:
        llm_results = _run_llm_if_available(analysis, args)

    # Generate report
    print("\n📝 Generating weekly report ...")
    report_gen = ReportGenerator(analysis, week_label, llm_results=llm_results)
    report_md = report_gen.generate()

    report_path = output_dir / f"pitboss-weekly-{week_label}.md"
    report_path.write_text(report_md)
    print(f"   Report: {report_path}")

    # Build shakedown candidates
    print("\n🎯 Building shakedown candidates ...")
    candidate_builder = ShakedownCandidateBuilder(analysis, threshold=args.shakedown_threshold)
    candidates = candidate_builder.build()
    _enrich_candidates_with_llm(candidates, llm_results)

    candidates_path = output_dir / f"shakedown-candidates-{week_label}.json"
    candidates_path.write_text(json.dumps(candidates, indent=2))
    print(f"   Candidates: {len(candidates['repos'])} repos")

    # Save snapshot
    snapshot_path = output_dir / f"snapshot-{week_label}.json"
    snapshot_path.write_text(json.dumps(snapshot, indent=2))
    print(f"   Snapshot: {snapshot_path}")

    if not args.local:
        _upload_to_s3(args, loader, week_label, report_md, candidates, snapshot)

    return 0


def run_monthly(args, loader: S3DataLoader, output_dir: Path) -> int:
    """Run monthly analysis on the full month."""
    month_label = f"{args.year}-{args.month:02d}"

    print(f"\n📥 Loading data from s3://{args.s3_bucket} ...")
    reviews = loader.load_reviews(args.year, args.month)
    decisions = loader.load_decisions(args.year, args.month)

    print(f"   Reviews loaded:  {len(reviews)}")
    print(f"   Decisions loaded: {len(decisions)}")

    if not reviews:
        print("\n⚠️  No reviews found for this period.")
        return 0

    print("\n🔗 Correlating reviews with decisions ...")
    correlator = ReviewCorrelator(reviews, decisions)
    analysis = correlator.analyze()

    _print_summary(analysis)

    llm_results = {}
    if not args.no_llm:
        llm_results = _run_llm_if_available(analysis, args)

    return _generate_outputs(args, output_dir, month_label, analysis, llm_results, loader)


def run_monthly_aggregate(args, loader: S3DataLoader, output_dir: Path) -> int:
    """Aggregate weekly snapshots into a monthly report."""
    month_label = f"{args.year}-{args.month:02d}"
    print(f"\n📥 Loading weekly snapshots for {month_label} ...")

    # Try S3 first, fall back to local files
    snapshots = []
    if not args.local:
        snapshots = loader.load_all_weekly_snapshots(args.year, args.month)

    if not snapshots:
        # Try local snapshot files
        local_pattern = output_dir / f"snapshot-{args.year}-{args.month:02d}-*.json"
        import glob
        local_files = sorted(glob.glob(str(local_pattern)))
        for f in local_files:
            with open(f) as fh:
                snapshots.append(json.load(fh))
        print(f"   Loaded {len(snapshots)} local snapshots")

    if not snapshots:
        print("\n⚠️  No snapshots found. Run weekly analyses first, or use --mode monthly.")
        return 1

    print(f"\n🔗 Merging {len(snapshots)} weekly snapshots ...")
    analysis = ReviewCorrelator.merge_snapshots(snapshots)

    _print_summary(analysis)

    llm_results = {}
    if not args.no_llm:
        llm_results = _run_llm_if_available(analysis, args)

    return _generate_outputs(args, output_dir, month_label, analysis, llm_results, loader)


# ── Shared helpers ───────────────────────────────────────────────

def _print_summary(analysis: Dict):
    s = analysis["summary"]
    print(f"   Total PRs (deduplicated):  {s['total_prs']}")
    print(f"   Total scans:               {s.get('total_scans', s['total_prs'])}")
    print(f"   PRs blocked:               {s['prs_blocked']}")
    print(f"   Avg NEW risk:              {s['avg_risk_score']}/10")
    print(f"   Avg EXISTING risk:         {s.get('avg_existing_risk_score', 0)}/10")
    print(f"   Risks accepted:            {s['risks_accepted']}")
    print(f"   False positives:           {s['false_positives']}")
    if s.get("multi_scan_prs", 0) > 0:
        print(f"   Multi-scan PRs:            {s['multi_scan_prs']} "
              f"({s.get('improving_prs', 0)} improving, {s.get('worsening_prs', 0)} worsening)")


def _run_llm_if_available(analysis, args) -> Dict:
    gemini_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_key:
        print("\n⚠️  GEMINI_API_KEY not set — skipping LLM analysis")
        return {}

    print("\n🧠 Running Gemini analysis ...")
    from llm_analyzer import LLMAnalyzer
    try:
        llm = LLMAnalyzer(gemini_key)
        candidate_builder = ShakedownCandidateBuilder(analysis, threshold=args.shakedown_threshold)
        candidates = candidate_builder.build()
        return llm.run_all(analysis, candidates)
    except Exception as e:
        print(f"  ⚠️ LLM analysis failed: {e}")
        print("  Report will continue without AI sections.")
        return {}


def _enrich_candidates_with_llm(candidates: Dict, llm_results: Dict):
    """Merge LLM reasoning into candidate entries."""
    shakedown_reasoning = llm_results.get("shakedown_reasoning")
    if not shakedown_reasoning:
        return

    llm_candidates = {
        c["repo"]: c
        for c in shakedown_reasoning.get("candidates", [])
    }
    for repo in candidates["repos"]:
        llm_data = llm_candidates.get(repo["repo"])
        if llm_data:
            repo["llm_urgency"] = llm_data.get("urgency", "")
            repo["llm_narrative"] = llm_data.get("narrative", "")
            repo["llm_focus_areas"] = llm_data.get("focus_areas", [])
            repo["llm_priority_files"] = llm_data.get("priority_files", [])
            repo["llm_scan_instructions"] = llm_data.get("scan_instructions", "")
            repo["llm_existing_debt_notes"] = llm_data.get("existing_debt_notes", "")
            repo["llm_risk_if_ignored"] = llm_data.get("risk_if_ignored", "")


def _generate_outputs(args, output_dir: Path, label: str, analysis: Dict,
                      llm_results: Dict, loader: S3DataLoader) -> int:
    """Generate report and candidates, write locally, optionally upload."""

    print("\n📝 Generating meeting report ...")
    report_gen = ReportGenerator(analysis, label, llm_results=llm_results)
    report_md = report_gen.generate()

    report_path = output_dir / f"pitboss-report-{label}.md"
    report_path.write_text(report_md)
    print(f"   Report: {report_path}")

    print("\n🎯 Building shakedown candidates ...")
    candidate_builder = ShakedownCandidateBuilder(analysis, threshold=args.shakedown_threshold)
    candidates = candidate_builder.build()
    _enrich_candidates_with_llm(candidates, llm_results)

    candidates_path = output_dir / f"shakedown-candidates-{label}.json"
    candidates_path.write_text(json.dumps(candidates, indent=2))
    print(f"   Candidates: {len(candidates['repos'])} repos")
    print(f"   File: {candidates_path}")

    if not args.local:
        _upload_to_s3_monthly(args, loader, label, report_md, candidates)

    print(f"\n{'=' * 60}")
    print(f"  Done. Outputs in {output_dir}/")
    print(f"{'=' * 60}")
    return 0


def _upload_to_s3(args, loader, label, report_md, candidates, snapshot=None):
    """Upload weekly outputs to S3."""
    print("\n📤 Uploading to S3 ...")
    try:
        import boto3
        s3 = boto3.client("s3")
        bucket = args.s3_bucket

        report_key = f"pitboss-reports/{label}/pitboss-report-{label}.md"
        s3.put_object(
            Bucket=bucket, Key=report_key,
            Body=report_md, ContentType="text/markdown",
        )
        print(f"   Report:      s3://{bucket}/{report_key}")

        candidates_key = f"shakedown/{label}/candidates.json"
        s3.put_object(
            Bucket=bucket, Key=candidates_key,
            Body=json.dumps(candidates, indent=2),
            ContentType="application/json",
        )
        print(f"   Candidates:  s3://{bucket}/{candidates_key}")

        if snapshot:
            snapshot_key = f"pitboss-snapshots/{args.year}-{args.month:02d}/{label}.json"
            s3.put_object(
                Bucket=bucket, Key=snapshot_key,
                Body=json.dumps(snapshot, indent=2),
                ContentType="application/json",
            )
            print(f"   Snapshot:    s3://{bucket}/{snapshot_key}")

    except Exception as e:
        print(f"   ⚠️ S3 upload failed: {e}")
        print("   Local files are still available.")


def _upload_to_s3_monthly(args, loader, label, report_md, candidates):
    """Upload monthly outputs to S3."""
    print("\n📤 Uploading to S3 ...")
    try:
        import boto3
        s3 = boto3.client("s3")
        bucket = args.s3_bucket

        report_key = f"pitboss-reports/{label}/pitboss-report-{label}.md"
        s3.put_object(
            Bucket=bucket, Key=report_key,
            Body=report_md, ContentType="text/markdown",
        )
        print(f"   Report:      s3://{bucket}/{report_key}")

        candidates_key = f"shakedown/{label}/candidates.json"
        s3.put_object(
            Bucket=bucket, Key=candidates_key,
            Body=json.dumps(candidates, indent=2),
            ContentType="application/json",
        )
        print(f"   Candidates:  s3://{bucket}/{candidates_key}")

    except Exception as e:
        print(f"   ⚠️ S3 upload failed: {e}")
        print("   Local files are still available.")



def main() -> int:
    args = parse_args()

    # Fill year/month defaults based on mode:
    # weekly → current month (analysis is in-progress)
    # monthly/monthly-aggregate → previous month (analysis is after month ends)
    if args.year == 0 or args.month == 0:
        now = datetime.now(timezone.utc)
        if args.mode == "weekly":
            args.year, args.month = now.year, now.month
        elif now.month == 1:
            args.year, args.month = now.year - 1, 12
        else:
            args.year, args.month = now.year, now.month - 1

    month_label = f"{args.year}-{args.month:02d}"

    print("=" * 60)
    print(f"  pit-boss — Security Analysis")
    print(f"  Period: {month_label} | Mode: {args.mode}")
    if args.local:
        print(f"  Running in LOCAL mode (no S3 uploads)")
    print("=" * 60)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.local:
        # In local mode, we still need S3 to READ data (unless aggregating local snapshots)
        loader = None
        if args.mode != "monthly-aggregate":
            try:
                loader = S3DataLoader(bucket=args.s3_bucket)
            except Exception as e:
                print(f"⚠️ Cannot connect to S3: {e}")
                if args.mode == "monthly-aggregate":
                    pass  # Will use local files
                else:
                    print("S3 access is required to read PR-Bouncer data.")
                    return 1
    else:
        loader = S3DataLoader(bucket=args.s3_bucket)

    if args.mode == "weekly":
        return run_weekly(args, loader, output_dir)
    elif args.mode == "monthly-aggregate":
        return run_monthly_aggregate(args, loader, output_dir)
    else:
        return run_monthly(args, loader, output_dir)


if __name__ == "__main__":
    sys.exit(main())