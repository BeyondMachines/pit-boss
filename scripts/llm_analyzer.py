"""
LLMAnalyzer — Gemini-powered analysis layer for pit-boss.

Runs AFTER the deterministic correlator. Takes the structured analysis
and produces smarter narrative insights. Three responsibilities:

1. Meeting report narrative — patterns, trends, cross-repo insights
2. Shakedown reasoning — why each candidate needs deep scanning
3. Override evaluation — assess quality of accept-risk/false-positive reasoning

All LLM outputs are clearly labeled in the final report so readers know
what's deterministic data vs AI-generated insight.
"""

import json
import os
from typing import Any, Dict, List, Optional

from google import genai
from google.genai import types


# ── Response schemas for structured output ───────────────────────

MEETING_INSIGHTS_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "executive_narrative": {
            "type": "STRING",
            "description": "2-3 paragraph executive summary of the month's security posture, trends, and concerns. Written for a VP of Engineering audience."
        },
        "cross_repo_patterns": {
            "type": "ARRAY",
            "description": "Patterns that span multiple repos — shared vulnerabilities, common anti-patterns, likely shared libraries.",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "pattern": {"type": "STRING", "description": "Description of the pattern"},
                    "affected_repos": {"type": "ARRAY", "items": {"type": "STRING"}},
                    "recommendation": {"type": "STRING"},
                },
            },
        },
        "team_observations": {
            "type": "ARRAY",
            "description": "Observations about team behavior — override patterns, recurring blind spots, training gaps.",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "observation": {"type": "STRING"},
                    "evidence": {"type": "STRING", "description": "Specific data points supporting this"},
                    "suggested_action": {"type": "STRING"},
                },
            },
        },
        "meeting_talking_points": {
            "type": "ARRAY",
            "description": "3-5 specific talking points for the engineering meeting, ordered by importance.",
            "items": {"type": "STRING"},
        },
    },
}

OVERRIDE_EVAL_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "evaluations": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "repo": {"type": "STRING"},
                    "pr_number": {"type": "STRING"},
                    "command": {"type": "STRING", "description": "accept-risk or false-positive"},
                    "reasoning_provided": {"type": "STRING", "description": "The original reasoning from the engineer"},
                    "risk_score": {"type": "INTEGER"},
                    "verdict": {
                        "type": "STRING",
                        "enum": ["ADEQUATE", "WEAK", "INSUFFICIENT", "SUSPICIOUS"],
                        "description": "Quality of the reasoning given the risk level"
                    },
                    "explanation": {"type": "STRING", "description": "Why this reasoning is or isn't adequate"},
                    "follow_up_needed": {"type": "BOOLEAN"},
                },
            },
        },
        "summary": {
            "type": "STRING",
            "description": "Overall assessment of override reasoning quality across the org"
        },
    },
}

SHAKEDOWN_REASONING_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "candidates": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "repo": {"type": "STRING"},
                    "urgency": {"type": "STRING", "enum": ["CRITICAL", "HIGH", "MEDIUM"]},
                    "narrative": {"type": "STRING", "description": "2-3 sentence explanation of why this repo needs deep scanning, referencing specific findings"},
                    "focus_areas": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                        "description": "Specific vulnerability types or code areas Strix should focus on"
                    },
                    "risk_if_ignored": {"type": "STRING", "description": "What could happen if this repo is NOT scanned"},
                },
            },
        },
    },
}


class LLMAnalyzer:
    def __init__(self, gemini_api_key: str = None):
        key = gemini_api_key or os.environ.get("GEMINI_API_KEY")
        if not key:
            raise ValueError(
                "GEMINI_API_KEY required for LLM analysis. "
                "Set it in .env or pass --no-llm to skip."
            )
        self.client = genai.Client(api_key=key)
        self.model = "gemini-3.1-pro-preview"

    def _call_gemini(self, prompt: str, schema: Dict) -> Optional[Dict]:
        """Call Gemini with structured JSON output."""
        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=[{"role": "user", "parts": [{"text": prompt}]}],
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=schema,
                    temperature=0.2,
                    max_output_tokens=8192,
                ),
            )
            return json.loads(response.text)
        except Exception as e:
            print(f"  ⚠️ Gemini call failed: {e}")
            return None

    # ── 1. Meeting Insights ──────────────────────────────────────

    def analyze_meeting_insights(self, analysis: Dict[str, Any]) -> Optional[Dict]:
        """Generate narrative insights for the meeting report."""
        print("  🧠 Generating meeting insights ...")

        s = analysis["summary"]
        repo_risk = analysis["repo_risk"]

        # Build a condensed data summary for the prompt
        top_repos = sorted(
            repo_risk.items(),
            key=lambda x: x[1]["max_risk"],
            reverse=True,
        )[:15]

        repo_summary = ""
        for repo, data in top_repos:
            top_issues = ", ".join(r for r, _ in data.get("top_issues", [])[:3]) or "none"
            repo_summary += (
                f"- {repo}: {data['total_prs']} PRs, avg risk {data['avg_risk']}, "
                f"max risk {data['max_risk']}, {data['critical_count']} criticals, "
                f"{data['override_count']} overrides, top issues: {top_issues}\n"
            )

        issue_types = "\n".join(
            f"- {rule}: {count} occurrences"
            for rule, count in analysis["global_issue_types"][:15]
        )

        override_authors = "\n".join(
            f"- @{author}: {count} overrides"
            for author, count in analysis["override_authors"][:10]
        )

        prompt = f"""You are a senior security architect reviewing a month of PR security review data.
Analyze the following statistics and produce insights for an engineering leadership meeting.

## Monthly Summary
- Total PRs reviewed: {s['total_prs']}
- PRs blocked (risk >= 7): {s['prs_blocked']}
- Average risk score: {s['avg_risk_score']}/10
- Risks accepted: {s['risks_accepted']}
- False positives flagged: {s['false_positives']}
- High-risk PRs overridden: {s['high_risk_overridden']}
- Overrides with no reasoning: {s['empty_reasoning_overrides']}

## Risk Distribution
{json.dumps(analysis['risk_distribution'], indent=2)}

## Severity Distribution of Confirmed Findings
{json.dumps(analysis['severity_distribution'], indent=2)}

## Top Repos by Risk
{repo_summary}

## Most Common Issue Types (confirmed by AI review)
{issue_types}

## Override Activity by Author
{override_authors}

Focus on:
1. Cross-repo patterns — are multiple repos hitting the same issue types? Could this indicate a shared library or common anti-pattern?
2. Team behavior — are overrides concentrated with specific people? Are they justified?
3. Trends that need management attention
4. Specific, actionable talking points for the meeting (not generic advice)

Be direct and specific. Reference actual repo names and issue types from the data."""

        return self._call_gemini(prompt, MEETING_INSIGHTS_SCHEMA)

    # ── 2. Override Evaluation ───────────────────────────────────

    def evaluate_overrides(self, analysis: Dict[str, Any]) -> Optional[Dict]:
        """Assess quality of accept-risk and false-positive reasoning."""
        print("  🧠 Evaluating override reasoning ...")

        overridden_prs = [
            p for p in analysis["pr_records"]
            if p["was_overridden"]
        ]

        if not overridden_prs:
            return {"evaluations": [], "summary": "No overrides to evaluate."}

        # Build override details for the prompt — cap at 20 to stay in token budget
        override_details = []
        for p in overridden_prs[:20]:
            decisions = p["accept_risks"] + p["false_positives"]
            criticals = [
                {"title": c.get("title", ""), "description": c.get("description", "")[:200]}
                for c in p["critical_issues"][:3]
            ]
            confirmed = [
                {"rule": f.get("rule", ""), "severity": f.get("ai_severity", ""), "file": f.get("file", "")}
                for f in p["confirmed_findings"][:5]
            ]

            for d in decisions:
                override_details.append({
                    "repo": p["repo"],
                    "pr_number": p["pr_number"],
                    "risk_score": p["risk_score"],
                    "command": d["type"],
                    "author": d["author"],
                    "reasoning": d.get("reasoning", ""),
                    "critical_issues": criticals,
                    "confirmed_findings": confirmed,
                })

        prompt = f"""You are a security governance reviewer. Evaluate whether each override
(/accept-risk or /false-positive) has adequate reasoning given the risk level and findings.

For each override, assess:
- Does the reasoning address the SPECIFIC findings that triggered the block?
- Is the reasoning proportional to the risk score?
- Would an auditor find this reasoning acceptable?
- Is there any sign of "rubber stamping" (generic reasoning applied to different issues)?

Verdict guide:
- ADEQUATE: Reasoning specifically addresses the findings and is proportional to risk
- WEAK: Reasoning exists but is vague, generic, or doesn't address key findings
- INSUFFICIENT: No meaningful reasoning provided, or reasoning ignores critical issues
- SUSPICIOUS: Pattern suggests systematic bypassing without genuine review

## Overrides to Evaluate

{json.dumps(override_details, indent=2)}

Be fair but firm. A risk score of 3 with "/accept-risk this is a test file" is ADEQUATE.
A risk score of 9 with "/accept-risk will fix later" is INSUFFICIENT."""

        return self._call_gemini(prompt, OVERRIDE_EVAL_SCHEMA)

    # ── 3. Shakedown Reasoning ───────────────────────────────────

    def analyze_shakedown_candidates(
        self, analysis: Dict[str, Any], candidates: Dict[str, Any]
    ) -> Optional[Dict]:
        """Generate detailed reasoning for each shakedown candidate."""
        print("  🧠 Generating shakedown reasoning ...")

        if not candidates.get("repos"):
            return {"candidates": []}

        # Enrich candidates with their finding details
        repo_risk = analysis["repo_risk"]
        candidate_details = []

        for c in candidates["repos"][:10]:
            repo = c["repo"]
            data = repo_risk.get(repo, {})

            # Collect specific critical issue details
            criticals = []
            for pr in data.get("high_risk_prs", []):
                for issue in pr.get("critical_issues", []):
                    criticals.append({
                        "title": issue.get("title", ""),
                        "file": issue.get("file", ""),
                        "description": issue.get("description", "")[:200],
                    })

            candidate_details.append({
                "repo": repo,
                "priority_score": c["priority_score"],
                "max_risk": c["max_risk_score"],
                "critical_count": c["critical_issue_count"],
                "override_count": c["override_count"],
                "top_issues": data.get("top_issues", [])[:5],
                "critical_issue_details": criticals[:8],
                "reasons": c["reasons"],
            })

        prompt = f"""You are a penetration testing lead deciding which repositories need
deep security scanning (using an AI-powered tool called Strix that does autonomous pentesting).

For each candidate repo, write:
1. A specific narrative explaining WHY this repo needs scanning — reference actual findings
2. Focus areas that the scanner should prioritize (specific vuln types, code areas)
3. What could go wrong if this repo is NOT scanned (realistic risk assessment)

## Candidates

{json.dumps(candidate_details, indent=2)}

Be specific to each repo's actual findings. Don't give generic security advice.
Reference the actual issue types and critical findings from the data."""

        return self._call_gemini(prompt, SHAKEDOWN_REASONING_SCHEMA)

    # ── Public API ───────────────────────────────────────────────

    def run_all(
        self, analysis: Dict[str, Any], candidates: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Run all three analysis passes. Returns combined results."""
        results = {}

        insights = self.analyze_meeting_insights(analysis)
        if insights:
            results["meeting_insights"] = insights
            print(f"    Meeting insights: {len(insights.get('meeting_talking_points', []))} talking points")

        overrides = self.evaluate_overrides(analysis)
        if overrides:
            results["override_evaluations"] = overrides
            evals = overrides.get("evaluations", [])
            verdicts = [e.get("verdict", "") for e in evals]
            print(f"    Override evals: {len(evals)} reviewed — "
                  f"{verdicts.count('ADEQUATE')} adequate, "
                  f"{verdicts.count('WEAK')} weak, "
                  f"{verdicts.count('INSUFFICIENT')} insufficient, "
                  f"{verdicts.count('SUSPICIOUS')} suspicious")

        shakedown = self.analyze_shakedown_candidates(analysis, candidates)
        if shakedown:
            results["shakedown_reasoning"] = shakedown
            print(f"    Shakedown reasoning: {len(shakedown.get('candidates', []))} repos analyzed")

        return results