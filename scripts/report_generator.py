"""
ReportGenerator — Produces a Markdown meeting report from correlated analysis.

Designed to be printed, shared in Slack, or attached to a meeting invite.
"""

from typing import Any, Dict, List


class ReportGenerator:
    def __init__(self, analysis: Dict[str, Any], month_label: str, llm_results: Dict[str, Any] = None):
        self.a = analysis
        self.month = month_label
        self.llm = llm_results or {}

    def generate(self) -> str:
        s = self.a["summary"]
        sections = [
            self._header(s),
            self._executive_summary(s),
            self._llm_executive_narrative(),
            self._risk_distribution(),
            self._top_risky_repos(),
            self._most_common_issues(),
            self._llm_cross_repo_patterns(),
            self._override_analysis(s),
            self._high_risk_overrides(),
            self._llm_override_evaluations(),
            self._empty_reasoning_overrides(),
            self._override_leaderboard(),
            self._llm_team_observations(),
            self._discussion_points(s),
            self._llm_talking_points(),
            self._action_items(s),
            self._shakedown_preview(),
            self._llm_shakedown_reasoning(),
        ]
        return "\n\n".join(s for s in sections if s)

    # ── Sections ─────────────────────────────────────────────────

    def _header(self, s: Dict) -> str:
        return (
            f"# Pit Boss — Monthly Security Report\n"
            f"**Period:** {self.month}\n"
            f"**Generated:** deterministic analysis of PR-Bouncer data\n\n"
            f"---"
        )

    def _executive_summary(self, s: Dict) -> str:
        override_rate = (
            round(s["prs_overridden"] / max(s["prs_blocked"], 1) * 100)
            if s["prs_blocked"] > 0 else 0
        )
        return (
            f"## Executive Summary\n\n"
            f"| Metric | Value |\n"
            f"|--------|-------|\n"
            f"| Total PRs reviewed | {s['total_prs']} |\n"
            f"| PRs blocked (risk >= 7) | {s['prs_blocked']} |\n"
            f"| Average risk score | {s['avg_risk_score']}/10 |\n"
            f"| Risks accepted (`/accept-risk`) | {s['risks_accepted']} |\n"
            f"| False positives (`/false-positive`) | {s['false_positives']} |\n"
            f"| High-risk PRs overridden | {s['high_risk_overridden']} |\n"
            f"| Override rate (of blocked PRs) | {override_rate}% |\n"
            f"| Overrides with no reasoning | {s['empty_reasoning_overrides']} |"
        )

    def _risk_distribution(self) -> str:
        dist = self.a["risk_distribution"]
        rows = ""
        for bucket in ["1-3 (low)", "4-6 (medium)", "7-9 (high)", "10 (critical)"]:
            count = dist.get(bucket, 0)
            bar = "█" * min(count, 40)
            rows += f"| {bucket} | {count} | {bar} |\n"
        return (
            f"## Risk Score Distribution\n\n"
            f"| Range | Count | |\n"
            f"|-------|-------|-|\n"
            f"{rows}"
        )

    def _top_risky_repos(self) -> str:
        repos = self.a["repo_risk"]
        # Sort by max_risk desc, then total_risk desc
        sorted_repos = sorted(
            repos.items(),
            key=lambda x: (x[1]["max_risk"], x[1]["total_risk"]),
            reverse=True,
        )[:10]

        if not sorted_repos:
            return "## Top Risky Repos\n\nNo data."

        rows = ""
        for repo, data in sorted_repos:
            top_issues = ", ".join(rule for rule, _ in data["top_issues"][:3]) or "—"
            rows += (
                f"| `{repo}` | {data['total_prs']} | {data['avg_risk']}"
                f" | {data['max_risk']} | {data['critical_count']}"
                f" | {data['override_count']} | {top_issues} |\n"
            )

        return (
            f"## Top 10 Risky Repos\n\n"
            f"| Repo | PRs | Avg Risk | Max Risk | Criticals | Overrides | Top Issues |\n"
            f"|------|-----|----------|----------|-----------|-----------|------------|\n"
            f"{rows}"
        )

    def _most_common_issues(self) -> str:
        issues = self.a["global_issue_types"]
        if not issues:
            return "## Most Common Issue Types\n\nNo confirmed findings."

        rows = ""
        for rule, count in issues[:10]:
            rows += f"| `{rule}` | {count} |\n"

        sev = self.a["severity_distribution"]
        sev_line = ", ".join(f"{k}: {v}" for k, v in sorted(sev.items())) if sev else "—"

        return (
            f"## Most Common Issue Types\n\n"
            f"**Severity distribution of confirmed findings:** {sev_line}\n\n"
            f"| Rule | Occurrences |\n"
            f"|------|-------------|\n"
            f"{rows}"
        )

    def _override_analysis(self, s: Dict) -> str:
        if s["prs_overridden"] == 0:
            return "## Override Analysis\n\nNo overrides this period. 🎉"

        return (
            f"## Override Analysis\n\n"
            f"**{s['risks_accepted']}** PRs had their risk accepted, "
            f"**{s['false_positives']}** were marked as false positives.\n\n"
            f"Of the **{s['prs_blocked']}** PRs that were blocked by the security gate, "
            f"**{s['high_risk_overridden']}** were overridden."
        )

    def _high_risk_overrides(self) -> str:
        items = self.a["high_risk_overridden"]
        if not items:
            return ""

        rows = ""
        for p in items[:15]:
            decisions = p["accept_risks"] + p["false_positives"]
            cmd = decisions[0]["type"] if decisions else "?"
            author = decisions[0]["author"] if decisions else "?"
            reasoning = decisions[0].get("reasoning", "")[:80] if decisions else ""
            if not reasoning:
                reasoning = "⚠️ *No reasoning provided*"

            criticals = "; ".join(
                c.get("title", "?")[:50] for c in p["critical_issues"][:3]
            ) or "—"

            rows += (
                f"| `{p['repo']}` | #{p['pr_number']} | {p['risk_score']}"
                f" | `/{cmd}` | @{author} | {reasoning} | {criticals} |\n"
            )

        return (
            f"### ⚠️ High-Risk PRs That Were Overridden\n\n"
            f"These PRs scored 7+ but were pushed through. Each warrants discussion.\n\n"
            f"| Repo | PR | Risk | Action | By | Reasoning | Critical Issues |\n"
            f"|------|----|------|--------|----|-----------|----------------|\n"
            f"{rows}"
        )

    def _empty_reasoning_overrides(self) -> str:
        items = self.a["empty_reasoning_overrides"]
        if not items:
            return ""

        rows = ""
        for p in items[:10]:
            decisions = p["accept_risks"] + p["false_positives"]
            cmd = decisions[0]["type"] if decisions else "?"
            author = decisions[0]["author"] if decisions else "?"
            rows += (
                f"| `{p['repo']}` | #{p['pr_number']} | {p['risk_score']}"
                f" | `/{cmd}` | @{author} |\n"
            )

        return (
            f"### 🚩 Overrides Without Reasoning\n\n"
            f"These overrides had no explanation. This undermines audit trail.\n\n"
            f"| Repo | PR | Risk | Action | By |\n"
            f"|------|----|------|--------|----|  \n"
            f"{rows}"
        )

    def _override_leaderboard(self) -> str:
        authors = self.a["override_authors"]
        if not authors:
            return ""

        rows = ""
        for author, count in authors[:10]:
            rows += f"| @{author} | {count} |\n"

        return (
            f"### Override Frequency by Author\n\n"
            f"| Author | Overrides |\n"
            f"|--------|-----------|\n"
            f"{rows}"
        )

    def _discussion_points(self, s: Dict) -> str:
        points = []

        # High override rate
        if s["prs_blocked"] > 0:
            rate = s["prs_overridden"] / s["prs_blocked"]
            if rate > 0.5:
                points.append(
                    f"**Override rate is {rate:.0%}.** More than half of blocked PRs "
                    f"are being overridden. Is the risk threshold too aggressive, or "
                    f"are teams bypassing the gate too freely?"
                )

        # Lots of empty reasoning
        if s["empty_reasoning_overrides"] > 3:
            points.append(
                f"**{s['empty_reasoning_overrides']} overrides have no reasoning.** "
                f"Consider requiring a reason for all overrides to maintain an audit trail."
            )

        # High-risk overrides
        if s["high_risk_overridden"] > 0:
            points.append(
                f"**{s['high_risk_overridden']} high-risk PRs were overridden.** "
                f"Review the table above — are these genuinely false positives "
                f"or is critical risk being silently accepted?"
            )

        # Repeat offender repos
        repo_risk = self.a["repo_risk"]
        repeat_offenders = [
            repo for repo, data in repo_risk.items()
            if data["critical_count"] >= 3
        ]
        if repeat_offenders:
            repos_str = ", ".join(f"`{r}`" for r in repeat_offenders[:5])
            points.append(
                f"**Repeat offender repos with 3+ critical issues:** {repos_str}. "
                f"These repos may need a deep security review (repo-shakedown) "
                f"or architectural remediation."
            )

        # False positive rate
        if s["false_positives"] > s["total_prs"] * 0.3 and s["total_prs"] >= 10:
            points.append(
                f"**False positive rate is high ({s['false_positives']}/{s['total_prs']}).** "
                f"Consider tuning Semgrep rules or the risk threshold to reduce noise."
            )

        if not points:
            points.append("No major concerns this period. Keep it up.")

        numbered = "\n".join(f"{i+1}. {p}" for i, p in enumerate(points))
        return f"## Discussion Points for Engineering Meeting\n\n{numbered}"

    def _action_items(self, s: Dict) -> str:
        actions = []

        if s["empty_reasoning_overrides"] > 0:
            actions.append(
                "Enforce mandatory reasoning on `/accept-risk` and `/false-positive` commands."
            )

        if s["high_risk_overridden"] > 2:
            actions.append(
                "Review all high-risk overrides listed above. Escalate any that lack justification."
            )

        risky_repos = sorted(
            self.a["repo_risk"].items(),
            key=lambda x: x[1]["max_risk"],
            reverse=True,
        )[:3]
        if risky_repos and risky_repos[0][1]["max_risk"] >= 7:
            repos_str = ", ".join(f"`{r}`" for r, _ in risky_repos)
            actions.append(
                f"Run repo-shakedown on: {repos_str} (see shakedown candidates file)."
            )

        common_issues = self.a["global_issue_types"][:3]
        if common_issues:
            issues_str = ", ".join(f"`{rule}`" for rule, _ in common_issues)
            actions.append(
                f"Create training/awareness materials for top recurring issues: {issues_str}."
            )

        if not actions:
            actions.append("No immediate action items. Review trends next month.")

        numbered = "\n".join(f"{i+1}. {a}" for i, a in enumerate(actions))
        return f"## Action Items\n\n{numbered}"

    def _shakedown_preview(self) -> str:
        repos = self.a["repo_risk"]
        candidates = [
            (repo, data) for repo, data in repos.items()
            if data["max_risk"] >= 7 or data["critical_count"] >= 2
        ]
        candidates.sort(key=lambda x: x[1]["max_risk"], reverse=True)

        if not candidates:
            return (
                "## Repo Shakedown Candidates\n\n"
                "No repos qualify for deep scanning this period."
            )

        rows = ""
        for repo, data in candidates[:10]:
            reason = []
            if data["max_risk"] >= 9:
                reason.append(f"max risk {data['max_risk']}")
            elif data["max_risk"] >= 7:
                reason.append(f"high risk ({data['max_risk']})")
            if data["critical_count"] >= 2:
                reason.append(f"{data['critical_count']} criticals")
            if data["override_count"] > data["total_prs"] * 0.5 and data["total_prs"] >= 2:
                reason.append("high override rate")
            rows += f"| `{repo}` | {data['max_risk']} | {data['critical_count']} | {'; '.join(reason)} |\n"

        return (
            f"## Repo Shakedown Candidates\n\n"
            f"These repos are recommended for deep security scanning with repo-shakedown.\n"
            f"Full list exported to `shakedown-candidates-{self.month}.json`.\n\n"
            f"| Repo | Max Risk | Criticals | Reason |\n"
            f"|------|----------|-----------|--------|\n"
            f"{rows}"
        )

    # ── LLM-generated sections ───────────────────────────────────
    # Each is clearly labeled as AI-generated in the output.

    def _llm_executive_narrative(self) -> str:
        insights = self.llm.get("meeting_insights")
        if not insights:
            return ""
        narrative = insights.get("executive_narrative", "")
        if not narrative:
            return ""
        return (
            f"### 🧠 AI Analysis\n\n"
            f"*The following narrative was generated by Gemini based on the data above.*\n\n"
            f"{narrative}"
        )

    def _llm_cross_repo_patterns(self) -> str:
        insights = self.llm.get("meeting_insights")
        if not insights:
            return ""
        patterns = insights.get("cross_repo_patterns", [])
        if not patterns:
            return ""

        md = (
            "## 🧠 Cross-Repo Patterns (AI-detected)\n\n"
            "*Patterns spanning multiple repos that may indicate shared libraries or anti-patterns.*\n\n"
        )
        for p in patterns:
            repos_str = ", ".join(f"`{r}`" for r in p.get("affected_repos", []))
            md += f"### {p.get('pattern', 'Pattern')}\n"
            md += f"**Repos:** {repos_str}\n\n"
            md += f"**Recommendation:** {p.get('recommendation', '')}\n\n"
        return md

    def _llm_team_observations(self) -> str:
        insights = self.llm.get("meeting_insights")
        if not insights:
            return ""
        observations = insights.get("team_observations", [])
        if not observations:
            return ""

        md = (
            "## 🧠 Team Behavior Observations (AI-detected)\n\n"
            "*Patterns in how teams interact with security reviews.*\n\n"
        )
        for i, obs in enumerate(observations, 1):
            md += f"**{i}. {obs.get('observation', '')}**\n\n"
            md += f"Evidence: {obs.get('evidence', '')}\n\n"
            md += f"Suggested action: {obs.get('suggested_action', '')}\n\n"
        return md

    def _llm_talking_points(self) -> str:
        insights = self.llm.get("meeting_insights")
        if not insights:
            return ""
        points = insights.get("meeting_talking_points", [])
        if not points:
            return ""

        numbered = "\n".join(f"{i+1}. {p}" for i, p in enumerate(points))
        return (
            f"### 🧠 AI-Suggested Talking Points\n\n"
            f"{numbered}"
        )

    def _llm_override_evaluations(self) -> str:
        evals_data = self.llm.get("override_evaluations")
        if not evals_data:
            return ""
        evals = evals_data.get("evaluations", [])
        summary = evals_data.get("summary", "")
        if not evals:
            return ""

        md = (
            "### 🧠 Override Reasoning Quality (AI-evaluated)\n\n"
            "*Each override was assessed for whether the reasoning adequately addresses the findings.*\n\n"
        )

        if summary:
            md += f"**Overall:** {summary}\n\n"

        # Group by verdict
        for verdict, icon in [
            ("SUSPICIOUS", "🚨"), ("INSUFFICIENT", "❌"),
            ("WEAK", "⚠️"), ("ADEQUATE", "✅")
        ]:
            group = [e for e in evals if e.get("verdict") == verdict]
            if not group:
                continue
            md += f"#### {icon} {verdict} ({len(group)})\n\n"
            for e in group:
                reasoning = e.get("reasoning_provided", "").strip() or "*empty*"
                md += (
                    f"- `{e.get('repo', '?')}` PR #{e.get('pr_number', '?')} "
                    f"(risk {e.get('risk_score', '?')}) — `/{e.get('command', '?')}`\n"
                    f"  - Reasoning: \"{reasoning[:100]}\"\n"
                    f"  - Assessment: {e.get('explanation', '')}\n"
                )
                if e.get("follow_up_needed"):
                    md += f"  - **⚡ Follow-up needed**\n"
                md += "\n"

        return md

    def _llm_shakedown_reasoning(self) -> str:
        shakedown = self.llm.get("shakedown_reasoning")
        if not shakedown:
            return ""
        candidates = shakedown.get("candidates", [])
        if not candidates:
            return ""

        md = (
            "### 🧠 Deep Scan Reasoning (AI-generated)\n\n"
            "*Detailed rationale for each repo-shakedown candidate.*\n\n"
        )
        for c in candidates:
            urgency = c.get("urgency", "MEDIUM")
            icon = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡"}.get(urgency, "⚪")
            md += f"#### {icon} `{c.get('repo', '?')}` — {urgency}\n\n"
            md += f"{c.get('narrative', '')}\n\n"
            focus = c.get("focus_areas", [])
            if focus:
                md += "**Focus areas for scanner:**\n"
                md += "".join(f"- {f}\n" for f in focus)
                md += "\n"
            risk = c.get("risk_if_ignored", "")
            if risk:
                md += f"**Risk if not scanned:** {risk}\n\n"

        return md