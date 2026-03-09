#!/usr/bin/env python3
"""
pit-boss — Reviews PR-Bouncer's monthly output from S3.

Reads reviews and decisions, correlates findings with accept-risk/false-positive
overrides, produces:
  1. A meeting report (Markdown) with stats, trends, and discussion points
  2. A shakedown candidates list (JSON) for repo-shakedown

Deterministic correlation + optional Gemini-powered narrative analysis.

Usage:
    python pitboss.py                          # defaults to previous month
    python pitboss.py --year 2026 --month 2    # specific month
    python pitboss.py --output-dir ./reports    # custom output dir
    python pitboss.py --no-llm                 # skip Gemini, deterministic only

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

from dotenv import load_dotenv

# Load .env from repo root (local dev)
_env_file = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_env_file)

from s3_loader import S3DataLoader
from correlator import ReviewCorrelator
from report_generator import ReportGenerator
from shakedown_candidates import ShakedownCandidateBuilder


def parse_args():
    p = argparse.ArgumentParser(description="pit-boss: PR-Bouncer monthly analysis")
    now = datetime.now(timezone.utc)
    # Default to previous month
    if now.month == 1:
        def_year, def_month = now.year - 1, 12
    else:
        def_year, def_month = now.year, now.month - 1

    p.add_argument("--year", type=int, default=def_year)
    p.add_argument("--month", type=int, default=def_month)
    p.add_argument("--output-dir", type=str, default="./pitboss-output")
    p.add_argument("--shakedown-threshold", type=int, default=7,
                   help="Minimum risk score to be a shakedown candidate (default: 7)")
    p.add_argument("--s3-bucket", type=str,
                   default=os.environ.get("S3_BUCKET", "bm-pr-reviews"))
    p.add_argument("--no-llm", action="store_true",
                   help="Skip Gemini analysis, deterministic report only")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    month_label = f"{args.year}-{args.month:02d}"

    print("=" * 60)
    print(f"  pit-boss — Monthly Security Analysis")
    print(f"  Period: {month_label}")
    print("=" * 60)

    # ── 1. Load data from S3 ─────────────────────────────────────
    print(f"\n📥 Loading data from s3://{args.s3_bucket} ...")
    loader = S3DataLoader(bucket=args.s3_bucket)

    reviews = loader.load_reviews(args.year, args.month)
    decisions = loader.load_decisions(args.year, args.month)

    print(f"   Reviews loaded:  {len(reviews)}")
    print(f"   Decisions loaded: {len(decisions)}")

    if not reviews:
        print("\n⚠️  No reviews found for this period. Nothing to analyze.")
        return 0

    # ── 2. Correlate reviews with decisions ──────────────────────
    print("\n🔗 Correlating reviews with decisions ...")
    correlator = ReviewCorrelator(reviews, decisions)
    analysis = correlator.analyze()

    print(f"   Total PRs reviewed:    {analysis['summary']['total_prs']}")
    print(f"   PRs blocked:           {analysis['summary']['prs_blocked']}")
    print(f"   Risks accepted:        {analysis['summary']['risks_accepted']}")
    print(f"   False positives:       {analysis['summary']['false_positives']}")

    # ── 3. LLM analysis (optional) ──────────────────────────────
    llm_results = {}
    if not args.no_llm:
        gemini_key = os.environ.get("GEMINI_API_KEY")
        if gemini_key:
            print("\n🧠 Running Gemini analysis ...")
            from llm_analyzer import LLMAnalyzer
            llm = LLMAnalyzer(gemini_key)

            # Build candidates first so LLM can reason about them
            candidate_builder = ShakedownCandidateBuilder(
                analysis, threshold=args.shakedown_threshold
            )
            candidates = candidate_builder.build()

            llm_results = llm.run_all(analysis, candidates)
        else:
            print("\n⚠️  GEMINI_API_KEY not set — skipping LLM analysis")
            print("   Set it in .env or pass --no-llm to suppress this warning")
    else:
        print("\n⏭️  LLM analysis skipped (--no-llm)")

    # ── 4. Generate meeting report ───────────────────────────────
    print("\n📝 Generating meeting report ...")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    report_gen = ReportGenerator(analysis, month_label, llm_results=llm_results)
    report_md = report_gen.generate()

    report_path = output_dir / f"pitboss-report-{month_label}.md"
    report_path.write_text(report_md)
    print(f"   Report: {report_path}")

    # ── 5. Build shakedown candidate list ────────────────────────
    print("\n🎯 Building shakedown candidates ...")
    if "candidates" not in dir():
        candidate_builder = ShakedownCandidateBuilder(
            analysis, threshold=args.shakedown_threshold
        )
        candidates = candidate_builder.build()

    # Enrich with LLM reasoning if available
    if llm_results.get("shakedown_reasoning"):
        llm_candidates = {
            c["repo"]: c
            for c in llm_results["shakedown_reasoning"].get("candidates", [])
        }
        for repo in candidates["repos"]:
            llm_data = llm_candidates.get(repo["repo"])
            if llm_data:
                repo["llm_urgency"] = llm_data.get("urgency", "")
                repo["llm_narrative"] = llm_data.get("narrative", "")
                repo["llm_focus_areas"] = llm_data.get("focus_areas", [])
                repo["llm_risk_if_ignored"] = llm_data.get("risk_if_ignored", "")

    candidates_path = output_dir / f"shakedown-candidates-{month_label}.json"
    candidates_path.write_text(json.dumps(candidates, indent=2))
    print(f"   Candidates: {len(candidates['repos'])} repos")
    print(f"   File: {candidates_path}")

    # ── 6. Upload results to S3 ──────────────────────────────────
    print("\n📤 Uploading results to S3 ...")
    try:
        import boto3
        s3 = boto3.client("s3")
        bucket = args.s3_bucket
        prefix = f"pitboss/{month_label}"

        # Meeting report
        report_key = f"pitboss-reports/{month_label}/pitboss-report-{month_label}.md"
        s3.put_object(
            Bucket=bucket, Key=report_key,
            Body=report_md, ContentType="text/markdown",
        )
        print(f"   Report:      s3://{bucket}/{report_key}")

        # Shakedown candidates
        candidates_key = f"shakedown/{month_label}/candidates.json"
        s3.put_object(
            Bucket=bucket, Key=candidates_key,
            Body=json.dumps(candidates, indent=2),
            ContentType="application/json",
        )
        print(f"   Candidates:  s3://{bucket}/{candidates_key}")

    except Exception as e:
        print(f"   ⚠️ S3 upload failed: {e}")
        print("   Local files are still available in the output directory.")

    # ── Summary ──────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  Done. Outputs in {output_dir}/")
    print(f"{'=' * 60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())