# pit-boss

The floor manager of the Bada Bing security suite. Pit Boss reviews a month's worth of PR-Bouncer data, correlates findings with team decisions, and flags what needs attention — then hands off the worst offenders to [repo-shakedown](https://github.com/BeyondMachines/repo-shakedown) for deep scanning.

**Part of the [Bada Bing](https://github.com/BeyondMachines) security pipeline:**

| Tool | Role |
|------|------|
| [pr-bouncer](https://github.com/BeyondMachines/pr-bouncer) | Checks every PR at the door — static scans + AI review |
| **pit-boss** | Reviews the month's floor activity, flags trouble, escalates |
| [repo-shakedown](https://github.com/BeyondMachines/repo-shakedown) | Takes flagged repos to the back room — deep AI pentesting with Strix |

---

## What It Does

Pit Boss reads PR-Bouncer's review data and team decisions from S3, then produces two outputs:

**1. Meeting Report** (`pitboss-report-YYYY-MM.md`)

A structured Markdown report designed to drive an engineering security meeting:

- Executive summary — PRs reviewed, block rate, override rate
- Risk score distribution across all PRs
- Top risky repos ranked by severity, with recurring issue types
- Most common vulnerability patterns across the org
- Override analysis — who accepted risk, who flagged false positives, and whether they explained why
- Flagged overrides — high-risk PRs that were pushed through, especially without reasoning
- Auto-generated discussion points and action items

With Gemini LLM enabled (`use_llm: true`), the report also includes:

- AI-generated executive narrative with cross-repo pattern detection
- Override reasoning quality evaluation (ADEQUATE / WEAK / INSUFFICIENT / SUSPICIOUS)
- Team behavior observations and specific meeting talking points
- Detailed shakedown reasoning per candidate repo

All AI-generated sections are clearly labeled with 🧠 so readers know what's deterministic data vs AI interpretation.

**2. Shakedown Candidates** (`shakedown-candidates-YYYY-MM.json`)

A prioritized list of repos that need deep security scanning, with:

- Priority score based on risk severity, critical issue count, and override patterns
- Suggested scan mode (`quick`, `default`, or `deep`)
- Human-readable reasons for each repo's inclusion
- When LLM is enabled: urgency rating, focus areas for the scanner, and risk-if-ignored narrative
- Format ready to feed directly into repo-shakedown's dispatch trigger

Both outputs are saved locally and uploaded to S3.

---

## How It Works

```
S3 (PR-Bouncer data)
  ├── reviews/YYYY/MM/DD/*.json        ← PR review results
  ├── decisions/YYYY/MM/DD/*.json      ← /accept-risk & /false-positive commands
  └── decisions/YYYY/MM.csv            ← decision log
          │
     s3_loader.py            Fetches and parses all data for the month
          │
     correlator.py           Pairs reviews ↔ decisions by repo + PR number
          │                   Computes per-repo stats, finds patterns
          │                   100% deterministic — no AI
          │
     llm_analyzer.py         (optional) Three Gemini calls:
          │                   1. Meeting narrative + cross-repo patterns
          │                   2. Override reasoning evaluation
          │                   3. Shakedown candidate reasoning
          │
     ┌────┴─────────────────┐
     │                      │
report_generator.py    shakedown_candidates.py
  (Markdown)              (JSON)
          │
     Uploaded to S3
  pitboss-reports/YYYY-MM/    ← report
  shakedown/YYYY-MM/          ← candidates for repo-shakedown
```

The correlator joins reviews to decisions using `repo + pr_number` as the key. It then computes aggregates per repo (total risk, critical count, override rate) and org-wide (most common issues, severity distribution, override patterns). The deterministic layer does all the heavy lifting — the optional LLM pass reads the structured analysis and writes smarter narrative on top.

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

# Or specify a month
python pitboss.py --year 2026 --month 2

# Without LLM analysis (faster, no Gemini key needed)
python pitboss.py --no-llm

# Custom output directory
python pitboss.py --output-dir ../reports

# Adjust shakedown threshold (default: 7)
python pitboss.py --shakedown-threshold 8
```

Outputs land in `./pitboss-output/` (or your custom dir):
- `pitboss-report-2026-02.md` — meeting report with stats, discussion points, and (if LLM enabled) AI narrative
- `shakedown-candidates-2026-02.json` — prioritized list for repo-shakedown

### CLI Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--year` | Previous month's year | Year to analyze |
| `--month` | Previous month | Month to analyze (1-12) |
| `--output-dir` | `./pitboss-output` | Where to write reports |
| `--shakedown-threshold` | `7` | Minimum risk score for shakedown candidates |
| `--s3-bucket` | `$S3_BUCKET` or `bm-pr-reviews` | S3 bucket to read from |
| `--no-llm` | off | Skip Gemini analysis, deterministic report only |

---

## GitHub Action — Reusable Workflow

Pit Boss is designed to be called as a reusable workflow from any repo or a dedicated ops repo.

### Example Caller Workflow

Create this in your ops repo (e.g. `.github/workflows/monthly-security-report.yml`):

```yaml
name: Pit-Boss Monthly Security Report

on:
  schedule:
    - cron: "0 8 1 * *"
  workflow_dispatch:

jobs:
  monthly-report:
    uses: BeyondMachines/pit-boss/.github/workflows/monthly-report.yml@v1
    with:
      s3_bucket: "pr-bouncer-code-reviews"
      shakedown_threshold: 7
      use_llm: true
    secrets:
      GEMINI_API_KEY: ${{ secrets.PR_BOUNCER_GEMINI_API_KEY }}
      AWS_ACCESS_KEY_ID: ${{ secrets.PR_BOUNCER_AWS_ACCESS_KEY_ID }}
      AWS_SECRET_ACCESS_KEY: ${{ secrets.PR_BOUNCER_AWS_SECRET_ACCESS_KEY }}
      AWS_REGION: ${{ secrets.PR_BOUNCER_VATBOX_AWS_REGION }}
```

### Workflow Inputs

| Input | Type | Default | Description |
|-------|------|---------|-------------|
| `s3_bucket` | string | `bm-pr-reviews` | S3 bucket where PR-Bouncer stores data |
| `shakedown_threshold` | number | `7` | Min risk score for shakedown candidates |
| `use_llm` | boolean | `true` | Enable Gemini AI analysis |
| `year` | number | previous month | Year to analyze |
| `month` | number | previous month | Month to analyze (1-12) |

### Required Secrets

| Secret | Required | Description |
|--------|----------|-------------|
| `AWS_ACCESS_KEY_ID` | Yes | AWS credentials with S3 read/write access |
| `AWS_SECRET_ACCESS_KEY` | Yes | AWS credentials |
| `AWS_REGION` | No | e.g. `us-east-1` |
| `GEMINI_API_KEY` | No | Required only if `use_llm: true` |

---

## Understanding the Report

### Discussion Points

Pit Boss auto-generates discussion points based on patterns it detects:

- **High override rate** — If more than 50% of blocked PRs are overridden, it flags whether the threshold is too aggressive or teams are bypassing too freely
- **Empty reasoning** — Overrides without explanations undermine the audit trail
- **Repeat offender repos** — Repos with 3+ critical issues across multiple PRs
- **High false positive rate** — Suggests tuning Semgrep rules or the risk threshold

### Override Evaluation (LLM)

When Gemini is enabled, each `/accept-risk` and `/false-positive` override is evaluated:

- **ADEQUATE** — Reasoning specifically addresses the findings and is proportional to risk
- **WEAK** — Reasoning exists but is vague or doesn't address key findings
- **INSUFFICIENT** — No meaningful reasoning, or reasoning ignores critical issues
- **SUSPICIOUS** — Pattern suggests systematic bypassing without genuine review

### Shakedown Candidate Scoring

Repos are scored for shakedown priority based on:

```
score = (max_risk × 3) + (critical_count × 5) + (avg_risk × 2) + (overridden_high_risk × 4)
```

This weights individual severe incidents highest, then volume of criticals, sustained risk, and suspicious override patterns.

---

## S3 Data Layout

Pit Boss reads from PR-Bouncer's structure and writes its own outputs:

```
s3://bm-pr-reviews/
│
│  PR-Bouncer writes here:
├── reviews/
│   └── YYYY/MM/DD/
│       ├── org__repo__PR-42__abc123.json          ← full review (risk >= 5)
│       └── org__repo__PR-43__def456__summary.json ← summary only (risk < 5)
├── decisions/
│   └── YYYY/MM/DD/
│       └── org__repo__PR-42__accept-risk__alice.json
├── tokens/
│   └── YYYY/MM.csv
│
│  Pit Boss writes here:
├── pitboss-reports/
│   └── YYYY-MM/
│       └── pitboss-report-YYYY-MM.md
│
│  Repo Shakedown reads from here:
└── shakedown/
    └── YYYY-MM/
        └── candidates.json
```

---

## Repository Structure

```
pit-boss/
├── .github/
│   └── workflows/
│       └── monthly-report.yml      ← Reusable workflow
├── scripts/
│   ├── pitboss.py                  ← CLI entrypoint
│   ├── s3_loader.py                ← S3 data fetching and parsing
│   ├── correlator.py               ← Deterministic review ↔ decision analysis
│   ├── llm_analyzer.py             ← Optional Gemini analysis layer
│   ├── report_generator.py         ← Markdown meeting report
│   └── shakedown_candidates.py     ← JSON candidate list for repo-shakedown
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Closing the Loop

The full Bada Bing security pipeline:

1. **pr-bouncer** runs on every PR — scans code, posts AI review, blocks high-risk merges
2. Engineers respond with `/accept-risk` or `/false-positive` — decisions are logged to S3
3. **pit-boss** runs monthly — correlates everything, produces a meeting report and shakedown list
4. **repo-shakedown** runs deep Strix AI scans on the flagged repos
5. Repeat — the meeting report tracks whether issues are actually getting fixed over time

---

## License

MIT