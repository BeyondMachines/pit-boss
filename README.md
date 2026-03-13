# pit-boss

The floor manager of the Bada Bing security suite. Pit Boss reviews PR-Bouncer data, correlates findings with team decisions, and flags what needs attention — then hands off the worst offenders to [repo-shakedown](https://github.com/BeyondMachines/repo-shakedown) for deep scanning.

**Part of the [Bada Bing](https://github.com/BeyondMachines) security pipeline:**

| Tool | Role |
|------|------|
| [pr-bouncer](https://github.com/BeyondMachines/pr-bouncer) | Checks every PR at the door — static scans + AI review |
| **pit-boss** | Reviews the floor activity, flags trouble, escalates |
| [repo-shakedown](https://github.com/BeyondMachines/repo-shakedown) | Takes flagged repos to the back room — deep AI pentesting with Strix |

---

## What It Does

Pit Boss reads PR-Bouncer's review data and team decisions from S3, then produces two outputs:

**1. Meeting Report** (`pitboss-report-YYYY-MM.md`)

A structured Markdown report designed to drive a security meeting:

- Executive summary — PRs reviewed, block rate, override rate, both NEW and EXISTING risk scores
- Fix trend tracking — which PRs improved after re-scans, fix velocity across the org
- Risk score distribution split by NEW (introduced by PRs) and EXISTING (technical debt)
- Top risky repos ranked by new risk, existing risk, and critical count
- Most common vulnerability patterns separated by NEW vs EXISTING
- Technical debt section — repos carrying the most pre-existing security risk
- Override analysis with reasoning quality evaluation
- Auto-generated discussion points and action items

With Gemini LLM enabled (`use_llm: true`), the report also includes:

- AI-generated executive narrative with cross-repo pattern detection
- Override reasoning quality evaluation (ADEQUATE / WEAK / INSUFFICIENT / SUSPICIOUS)
- Team behavior observations and specific meeting talking points
- Technical debt assessment
- Detailed shakedown reasoning with specific file paths and scan instructions

All AI-generated sections are labeled with 🧠.

**2. Shakedown Candidates** (`shakedown-candidates-YYYY-MM.json`)

A prioritized list of repos that need deep security scanning, with:

- Priority score based on both NEW and EXISTING risk severity
- Specific file paths and rule IDs for the scanner to focus on
- Structured scan guidance so Strix doesn't waste tokens on unrelated code
- When LLM is enabled: concrete scan instructions, priority files, and existing debt notes

---

## How It Works

```
S3 (PR-Bouncer data)
  ├── reviews/YYYY/MM/DD/*.json
  ├── decisions/YYYY/MM/DD/*.json
  └── decisions/YYYY/MM.csv
          │
     s3_loader.py            Fetches and parses data (full month or date range)
          │
     correlator.py           Groups multiple reviews per PR → deduplicates
          │                   Tracks fix trends across re-scans
          │                   Separates NEW vs EXISTING findings
          │                   Computes per-repo stats with tool-level detail
          │
     llm_analyzer.py         (optional) Three Gemini calls with rate limiting:
          │                   1. Meeting narrative + cross-repo patterns
          │                   2. Override reasoning evaluation
          │                   3. Shakedown targeting with file-level detail
          │
     ┌────┴─────────────────┐
     │                      │
report_generator.py    shakedown_candidates.py
  (Markdown)              (JSON with scan guidance)
```

### Key Design Decisions

**Deduplication:** A PR may have multiple reviews (re-scans after pushes). Pit Boss groups reviews by `repo + pr_number`, keeps the latest state as the canonical record, and tracks the trend (improving/worsening/stable) across scans. Counting is deduplicated — a PR with 3 scans counts as 1 PR, not 3.

**NEW vs EXISTING separation:** PR-Bouncer v2 classifies each finding as NEW (introduced by the PR) or EXISTING (pre-existing in changed files). Pit Boss carries this distinction through the entire pipeline — separate risk distributions, separate issue type rankings, separate critical counts per repo.

**Rate limiting:** All Gemini calls use exponential backoff with jitter (tenacity). If the LLM is unavailable, each section degrades gracefully — the deterministic report still generates, just without the AI narrative sections.

**Weekly/Monthly modes:** For large orgs, running a full-month analysis may hit token limits or take too long. Weekly mode analyzes a date range and saves a compact snapshot. Monthly-aggregate mode combines weekly snapshots into a full report.

---

## Quick Start — Run Locally

```bash
# 1. Clone
git clone https://github.com/BeyondMachines/pit-boss.git
cd pit-boss

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure
cp .env.example .env
# Edit .env with your AWS credentials, S3 bucket, and optionally GEMINI_API_KEY

# 4. Run for previous month
cd scripts
python pitboss.py

# Specify a month
python pitboss.py --year 2026 --month 2

# Local only — skip S3 uploads
python pitboss.py --local

# Without LLM analysis (faster, no Gemini key needed)
python pitboss.py --no-llm

# Weekly mode — analyze a specific date range
python pitboss.py --mode weekly --start-day 1 --end-day 7

# Monthly aggregate — combine weekly snapshots
python pitboss.py --mode monthly-aggregate

# Custom output directory and threshold
python pitboss.py --output-dir ../reports --shakedown-threshold 5
```

Outputs land in `./pitboss-output/` (or your custom dir):
- `pitboss-report-YYYY-MM.md` — meeting report
- `shakedown-candidates-YYYY-MM.json` — prioritized scan list with guidance

### CLI Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--year` | Previous month's year | Year to analyze |
| `--month` | Previous month | Month to analyze (1-12) |
| `--output-dir` | `./pitboss-output` | Where to write reports |
| `--shakedown-threshold` | `7` | Min risk score for shakedown candidates |
| `--s3-bucket` | `$S3_BUCKET` or `bm-pr-reviews` | S3 bucket to read from |
| `--no-llm` | off | Skip Gemini analysis, deterministic report only |
| `--local` | off | Skip S3 uploads, write files locally only |
| `--mode` | `monthly` | `monthly`, `weekly`, or `monthly-aggregate` |
| `--start-day` | — | Start day for weekly mode (1-31) |
| `--end-day` | — | End day for weekly mode (1-31) |

---

## Weekly + Monthly Workflow

For orgs with high PR volume, a weekly cadence keeps each analysis pass small:

```
Week 1:  pitboss.py --mode weekly --start-day 1 --end-day 7      → snapshot saved
Week 2:  pitboss.py --mode weekly --start-day 8 --end-day 14     → snapshot saved
Week 3:  pitboss.py --mode weekly --start-day 15 --end-day 21    → snapshot saved
Week 4:  pitboss.py --mode weekly --start-day 22 --end-day 31    → snapshot saved
EOM:     pitboss.py --mode monthly-aggregate                     → full monthly report
```

Each weekly run produces its own report and saves a compact snapshot to S3 (under `pitboss-snapshots/YYYY-MM/`). The monthly-aggregate mode reads all snapshots for the month, deduplicates PRs that span multiple weeks, and produces the combined report.

Weekly reports are useful for standups. The monthly aggregate is for the formal security meeting.

### Example Caller Workflows

**Weekly (runs every Monday):**
```yaml
name: Weekly Security Summary

on:
  schedule:
    - cron: "0 8 * * 1"
  workflow_dispatch:

jobs:
  weekly-report:
    uses: BeyondMachines/pit-boss/.github/workflows/monthly-report.yml@v1
    with:
      mode: weekly
      start_day: 0   # auto-computed: previous 7 days
      end_day: 0
      use_llm: false  # save tokens for monthly
    secrets:
      AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
      AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
      AWS_REGION: ${{ secrets.AWS_REGION }}
```

**Monthly (1st of each month):**
```yaml
name: Monthly Security Report

on:
  schedule:
    - cron: "0 8 1 * *"
  workflow_dispatch:

jobs:
  monthly-report:
    uses: BeyondMachines/pit-boss/.github/workflows/monthly-report.yml@v1
    with:
      mode: monthly-aggregate
      use_llm: true
    secrets:
      GEMINI_API_KEY: ${{ secrets.GEMINI_API_KEY }}
      AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
      AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
      AWS_REGION: ${{ secrets.AWS_REGION }}
```

---

## GitHub Action — Reusable Workflow

### Workflow Inputs

| Input | Type | Default | Description |
|-------|------|---------|-------------|
| `s3_bucket` | string | `bm-pr-reviews` | S3 bucket where PR-Bouncer stores data |
| `shakedown_threshold` | number | `7` | Min risk score for shakedown candidates |
| `use_llm` | boolean | `true` | Enable Gemini AI analysis |
| `year` | number | previous month | Year to analyze |
| `month` | number | previous month | Month to analyze (1-12) |
| `mode` | string | `monthly` | `monthly`, `weekly`, or `monthly-aggregate` |
| `start_day` | number | 0 | Start day for weekly mode |
| `end_day` | number | 0 | End day for weekly mode |

### Required Secrets

| Secret | Required | Description |
|--------|----------|-------------|
| `AWS_ACCESS_KEY_ID` | Yes | AWS credentials with S3 read/write access |
| `AWS_SECRET_ACCESS_KEY` | Yes | AWS credentials |
| `AWS_REGION` | No | e.g. `us-east-1` |
| `GEMINI_API_KEY` | No | Required only if `use_llm: true` |

---

## Understanding the Report

### NEW vs EXISTING Findings

PR-Bouncer v2 classifies each finding:

- **NEW** — on lines added or modified by the PR. These block the merge gate and represent current development quality.
- **EXISTING** — on lines that existed before the PR. These are pre-existing technical debt surfaced because the PR touched nearby code.

Pit Boss carries this split throughout: separate risk distributions, separate issue type rankings, separate critical counts. The shakedown candidates consider both — a repo with low NEW risk but high EXISTING risk still needs attention.

### Fix Trends

When a PR is re-scanned after pushes, Pit Boss tracks:

- **Improving** — risk score went down between first and latest scan
- **Worsening** — risk score went up
- **Issues fixed** — specific critical issues that disappeared between scans
- **Issues persisted** — specific issues that remained despite re-pushes

This tells you whether teams are actually fixing issues when blocked, or just overriding.

### Shakedown Candidate Scoring

Repos are scored considering both new and existing risk:

```
score = (max_new_risk × 3)
      + (new_critical_count × 5)
      + (avg_risk × 2)
      + (max_existing_risk × 2)
      + (existing_critical_count × 3)
      + (overridden_high_risk × 4)
      + (persisted_issues × 2)
      - (fixed_issues × 1)
```

Each candidate includes `scan_guidance` with priority file paths, rule IDs, and tool-level breakdowns — designed to be consumed directly by repo-shakedown so Strix knows exactly where to look.

---

## S3 Data Layout

```
s3://bm-pr-reviews/
│
│  PR-Bouncer writes:
├── reviews/YYYY/MM/DD/*.json
├── decisions/YYYY/MM/DD/*.json
├── tokens/YYYY/MM.csv
│
│  Pit Boss writes:
├── pitboss-reports/YYYY-MM/
│   └── pitboss-report-YYYY-MM.md
├── pitboss-snapshots/YYYY-MM/         ← weekly snapshots for aggregation
│   ├── YYYY-MM-d01-07.json
│   ├── YYYY-MM-d08-14.json
│   └── ...
└── shakedown/YYYY-MM/
    └── candidates.json                ← repo-shakedown reads this
```

---

## Repository Structure

```
pit-boss/
├── .github/
│   └── workflows/
│       └── monthly-report.yml      ← Reusable workflow (supports weekly/monthly/aggregate)
├── scripts/
│   ├── pitboss.py                  ← CLI entrypoint with mode selection
│   ├── s3_loader.py                ← S3 data fetching (date range support, snapshots)
│   ├── correlator.py               ← Deterministic analysis (dedup, trends, new/existing)
│   ├── llm_analyzer.py             ← Optional Gemini layer (rate limited, graceful degradation)
│   ├── report_generator.py         ← Markdown report (new/existing sections, fix trends)
│   └── shakedown_candidates.py     ← JSON candidates with scan guidance
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Rate Limiting

Pit Boss handles rate limits at two levels:

**Gemini API:** All LLM calls use exponential backoff with jitter (4 attempts, 5s initial, 120s max). If all retries fail, that section is skipped and the report continues without it. Each of the three LLM passes (meeting insights, override evaluation, shakedown reasoning) is independent — if one fails, the others still run.

**S3 API:** Standard boto3 retry configuration handles transient S3 errors.

**Token budget control:** Weekly mode is designed for orgs that want to spread token usage across the month. Run weekly analyses with `--no-llm` (deterministic only, zero tokens), then use `--mode monthly-aggregate` with `--use-llm` for a single LLM pass on the combined data.

---

## License

MIT