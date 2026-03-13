"""
ReportGenerator — Produces a Markdown meeting report from correlated analysis.

Designed to be printed, shared in Slack, or attached to a meeting invite.

v2 changes:
    - NEW vs EXISTING finding separation throughout
    - Fix trend reporting (improving/worsening PRs, fix velocity)
    - Technical debt section
    - Richer shakedown preview with scan guidance
    - Deduplication awareness (shows scan count vs PR count)
"""

from typing import Any, Dict, List


class ReportGenerator:
    def __init__(self, analysis: Dict[str, Any], month_label: str, llm_results: Dict[str, Any] = None):
        self.a = analysis
        self.month = month_label
        self.llm = llm_results or {}

    @staticmethod
    def _md_safe(text: str, max_len: int = 200) -> str:
        """Escape Markdown control characters in untrusted strings."""
        if not isinstance(text, str):
            return str(text)[:max_len]
        s = text.replace("\r", " ").replace("\n", " ")
        s = s.replace("|", "\\|")
        s = s.replace("<", "&lt;").replace(">", "&gt;")
        s = s.replace("[", "\\[").replace("]", "\\]")
        return s[:max_len]

    def generate(self) -> str:
        s = self.a["summary"]
        sections = [
            self._header(s),
            self._executive_summary(s),
            self._llm_executive_narrative(),
            self._fix_trends(s),
            self._risk_distribution(),
            self._top_risky_repos(),
            self._most_common_issues(),
            self._technical_debt(),
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
        aggregated = s.get("aggregated_from_snapshots")
        source_note = (
            f"**Source:** aggregated from {aggregated} weekly snapshots"
            if aggregated
            else "**Source:** direct analysis of PR-Bouncer data"
        )
        return (
            f"# Pit Boss — Monthly Security Report\n"
            f"**Period:** {self.month}\n"
            f"{source_note}\n\n"
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
            f"| Total PRs reviewed | {s['total_prs']} ({s.get('total_scans', s['total_prs'])} scans) |\n"
            f"| PRs blocked (new risk >= 7) | {s['prs_blocked']} |\n"
            f"| Average NEW risk score | {s['avg_risk_score']}/10 |\n"
            f"| Average EXISTING risk score | {s.get('avg_existing_risk_score', 0)}/10 |\n"
            f"| Risks accepted (`/accept-risk`) | {s['risks_accepted']} |\n"
            f"| False positives (`/false-positive`) | {s['false_positives']} |\n"
            f"| High-risk PRs overridden | {s['high_risk_overridden']} |\n"
            f"| Override rate (of blocked PRs) | {override_rate}% |\n"
            f"| Overrides with no reasoning | {s['empty_reasoning_overrides']} |"
        )

    def _fix_trends(self, s: Dict) -> str:
        multi = s.get("multi_scan_prs", 0)
        if multi == 0:
            return ""

        improving = s.get("improving_prs", 0)
        worsening = s.get("worsening_prs", 0)
        fixed = s.get("total_issues_fixed", 0)
        persisted = s.get("total_issues_persisted", 0)

        return (
            f"## Fix Trends\n\n"
            f"**{multi}** PRs were re-scanned after pushes (out of {s['total_prs']} total).\n\n"
            f"| Metric | Value |\n"
            f"|--------|-------|\n"
            f"| PRs improving (risk went down) | {improving} |\n"
            f"| PRs worsening (risk went up) | {worsening} |\n"
            f"| PRs stable | {multi - improving - worsening} |\n"
            f"| Issues fixed across re-scans | {fixed} |\n"
            f"| Issues persisted across re-scans | {persisted} |"
        )

    def _risk_distribution(self) -> str:
        new_dist = self.a.get("risk_distribution", {})
        existing_dist = self.a.get("existing_risk_distribution", {})

        rows = ""
        for bucket in ["1-3 (low)", "4-6 (medium)", "7-9 (high)", "10 (critical)"]:
            new_count = new_dist.get(bucket, 0)
            existing_count = existing_dist.get(bucket, 0)
            new_bar = "█" * min(new_count, 30)
            existing_bar = "░" * min(existing_count, 30)
            rows += f"| {bucket} | {new_count} | {new_bar} | {existing_count} | {existing_bar} |\n"
        return (
            f"## Risk Score Distribution\n\n"
            f"| Range | NEW | | EXISTING | |\n"
            f"|-------|-----|---|----------|---|\n"
            f"{rows}"
        )

    def _top_risky_repos(self) -> str:
        repos = self.a["repo_risk"]
        sorted_repos = sorted(
            repos.items(),
            key=lambda x: (x[1].get("max_risk", 0), x[1].get("max_existing_risk", 0)),
            reverse=True,
        )[:10]

        if not sorted_repos:
            return "## Top Risky Repos\n\nNo data."

        rows = ""
        for repo, data in sorted_repos:
            top_issues = ", ".join(
                self._md_safe(rule, 40) for rule, _ in data.get("top_issues", [])[:3]
            ) or "—"
            rows += (
                f"| `{repo}` | {data['total_prs']}"
                f" | {data['avg_risk']} | {data['max_risk']}"
                f" | {data.get('max_existing_risk', 0)}"
                f" | {data.get('new_critical_count', 0)}"
                f" | {data.get('existing_critical_count', 0)}"
                f" | {data['override_count']}"
                f" | {top_issues} |\n"
            )

        return (
            f"## Top 10 Risky Repos\n\n"
            f"| Repo | PRs | Avg Risk | Max New | Max Existing | New Criticals | Existing Criticals | Overrides | Top Issues |\n"
            f"|------|-----|----------|---------|--------------|---------------|--------------------|-----------|-----------|\n"
            f"{rows}"
        )

    def _most_common_issues(self) -> str:
        new_issues = self.a.get("global_new_issue_types", [])
        existing_issues = self.a.get("global_existing_issue_types", [])

        if not new_issues and not existing_issues:
            return "## Most Common Issue Types\n\nNo confirmed findings."

        md = "## Most Common Issue Types\n\n"

        new_sev = self.a.get("new_severity_distribution", {})
        existing_sev = self.a.get("existing_severity_distribution", {})

        if new_sev:
            sev_line = ", ".join(f"{k}: {v}" for k, v in sorted(new_sev.items()))
            md += f"**NEW finding severity:** {sev_line}\n\n"
        if existing_sev:
            sev_line = ", ".join(f"{k}: {v}" for k, v in sorted(existing_sev.items()))
            md += f"**EXISTING finding severity:** {sev_line}\n\n"

        if new_issues:
            md += "### NEW Issues (introduced by PRs)\n\n"
            md += "| Rule | Occurrences |\n|------|-------------|\n"
            for rule, count in new_issues[:10]:
                md += f"| `{rule}` | {count} |\n"
            md += "\n"

        if existing_issues:
            md += "### EXISTING Issues (pre-existing debt)\n\n"
            md += "| Rule | Occurrences |\n|------|-------------|\n"
            for rule, count in existing_issues[:10]:
                md += f"| `{rule}` | {count} |\n"

        return md

    def _technical_debt(self) -> str:
        """Section highlighting pre-existing security debt across repos."""
        repos = self.a["repo_risk"]
        debt_repos = [
            (repo, data) for repo, data in repos.items()
            if data.get("max_existing_risk", 0) >= 5 or data.get("existing_critical_count", 0) >= 1
        ]
        debt_repos.sort(key=lambda x: x[1].get("max_existing_risk", 0), reverse=True)

        if not debt_repos:
            return ""

        md = (
            "## Technical Debt — Pre-existing Security Issues\n\n"
            "These repos have significant pre-existing security issues detected in code "
            "that was not changed by the PRs themselves. This represents accumulated debt.\n\n"
            "| Repo | Max Existing Risk | Existing Criticals | AI-Found Issues | Top Existing Rules |\n"
            "|------|-------------------|--------------------|-----------------|--------------------|\n"
        )
        for repo, data in debt_repos[:10]:
            top_existing = ", ".join(
                self._md_safe(rule, 40) for rule, _ in data.get("top_existing_issues", [])[:3]
            ) or "—"
            ai_found = len(data.get("existing_code_issues", []))
            md += (
                f"| `{repo}` | {data.get('max_existing_risk', 0)}"
                f" | {data.get('existing_critical_count', 0)}"
                f" | {ai_found}"
                f" | {top_existing} |\n"
            )

        # LLM technical debt summary
        insights = self.llm.get("meeting_insights")
        if insights and insights.get("technical_debt_summary"):
            md += (
                f"\n### 🧠 AI Technical Debt Assessment\n\n"
                f"{insights['technical_debt_summary']}"
            )

        return md

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
            reasoning = self._md_safe(decisions[0].get("reasoning", ""), 80) if decisions else ""
            if not reasoning:
                reasoning = "⚠️ *No reasoning provided*"

            new_crits = p.get("new_critical_count", 0)
            existing_crits = p.get("existing_critical_count", 0)
            trend = p.get("trend", "—") or "—"

            rows += (
                f"| `{p['repo']}` | #{p['pr_number']} | {p['risk_score']}"
                f" | {new_crits}N/{existing_crits}E | {trend}"
                f" | `/{cmd}` | @{author} | {reasoning} |\n"
            )

        return (
            f"### ⚠️ High-Risk PRs That Were Overridden\n\n"
            f"These PRs scored 7+ on NEW findings but were pushed through.\n\n"
            f"| Repo | PR | New Risk | Criticals | Trend | Action | By | Reasoning |\n"
            f"|------|----|----------|-----------|-------|--------|----|----------|\n"
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
            author = self._md_safe(decisions[0]["author"], 50) if decisions else "?"
            rows += (
                f"| `{p['repo']}` | #{p['pr_number']} | {p['risk_score']}"
                f" | `/{cmd}` | @{author} |\n"
            )

        return (
            f"### 🚩 Overrides Without Reasoning\n\n"
            f"| Repo | PR | Risk | Action | By |\n"
            f"|------|----|------|--------|----|  \n"
            f"{rows}"
        )

    def _override_leaderboard(self) -> str:
        authors = self.a.get("override_authors", [])
        if not authors:
            return ""

        # Handle both Counter and list-of-tuples
        if isinstance(authors, dict):
            items = sorted(authors.items(), key=lambda x: x[1], reverse=True)[:10]
        else:
            items = authors[:10]

        if not items:
            return ""

        rows = ""
        for author, count in items:
            rows += f"| @{self._md_safe(str(author), 50)} | {count} |\n"

        return (
            f"### Override Frequency by Author\n\n"
            f"| Author | Overrides |\n"
            f"|--------|-----------|\n"
            f"{rows}"
        )

    def _discussion_points(self, s: Dict) -> str:
        points = []

        if s["prs_blocked"] > 0:
            rate = s["prs_overridden"] / s["prs_blocked"]
            if rate > 0.5:
                points.append(
                    f"**Override rate is {rate:.0%}.** More than half of blocked PRs "
                    f"are being overridden."
                )

        if s["empty_reasoning_overrides"] > 3:
            points.append(
                f"**{s['empty_reasoning_overrides']} overrides have no reasoning.** "
                f"Consider requiring a reason for all overrides."
            )

        if s["high_risk_overridden"] > 0:
            points.append(
                f"**{s['high_risk_overridden']} high-risk PRs were overridden.** "
                f"Review the table above."
            )

        # Repeat offender repos (by new criticals)
        repo_risk = self.a["repo_risk"]
        repeat_offenders = [
            repo for repo, data in repo_risk.items()
            if data.get("new_critical_count", data.get("critical_count", 0)) >= 3
        ]
        if repeat_offenders:
            repos_str = ", ".join(f"`{r}`" for r in repeat_offenders[:5])
            points.append(
                f"**Repeat offender repos (3+ NEW criticals):** {repos_str}."
            )

        # Technical debt flagging
        debt_repos = [
            repo for repo, data in repo_risk.items()
            if data.get("max_existing_risk", 0) >= 7
        ]
        if debt_repos:
            repos_str = ", ".join(f"`{r}`" for r in debt_repos[:5])
            points.append(
                f"**High existing security debt:** {repos_str} have pre-existing "
                f"risk scores >= 7. Consider dedicated remediation sprints."
            )

        # Fix velocity
        if s.get("multi_scan_prs", 0) > 0:
            fix_rate = s.get("improving_prs", 0) / s["multi_scan_prs"]
            if fix_rate < 0.5 and s["multi_scan_prs"] >= 3:
                points.append(
                    f"**Low fix velocity:** only {s.get('improving_prs', 0)}/{s['multi_scan_prs']} "
                    f"re-scanned PRs showed improvement."
                )

        if not points:
            points.append("No major concerns this period.")

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
                "Review all high-risk overrides. Escalate any that lack justification."
            )

        risky_repos = sorted(
            self.a["repo_risk"].items(),
            key=lambda x: x[1].get("max_risk", 0),
            reverse=True,
        )[:3]
        if risky_repos and risky_repos[0][1].get("max_risk", 0) >= 7:
            repos_str = ", ".join(f"`{r}`" for r, _ in risky_repos)
            actions.append(
                f"Run repo-shakedown on: {repos_str} (see shakedown candidates file)."
            )

        # Technical debt action items
        debt_repos = sorted(
            self.a["repo_risk"].items(),
            key=lambda x: x[1].get("max_existing_risk", 0),
            reverse=True,
        )[:3]
        if debt_repos and debt_repos[0][1].get("max_existing_risk", 0) >= 7:
            repos_str = ", ".join(f"`{r}`" for r, _ in debt_repos)
            actions.append(
                f"Schedule technical debt remediation for: {repos_str} (high existing risk)."
            )

        common_issues = self.a.get("global_new_issue_types", self.a.get("global_issue_types", []))[:3]
        if common_issues:
            issues_str = ", ".join(f"`{rule}`" for rule, _ in common_issues)
            actions.append(
                f"Create training/awareness materials for top recurring NEW issues: {issues_str}."
            )

        if not actions:
            actions.append("No immediate action items. Review trends next month.")

        numbered = "\n".join(f"{i+1}. {a}" for i, a in enumerate(actions))
        return f"## Action Items\n\n{numbered}"

    def _shakedown_preview(self) -> str:
        repos = self.a["repo_risk"]
        candidates = [
            (repo, data) for repo, data in repos.items()
            if (
                data.get("max_risk", 0) >= 7
                or data.get("max_existing_risk", 0) >= 7
                or (data.get("new_critical_count", 0) + data.get("existing_critical_count", 0)) >= 2
            )
        ]
        candidates.sort(
            key=lambda x: max(x[1].get("max_risk", 0), x[1].get("max_existing_risk", 0)),
            reverse=True,
        )

        if not candidates:
            return (
                "## Repo Shakedown Candidates\n\n"
                "No repos qualify for deep scanning this period."
            )

        rows = ""
        for repo, data in candidates[:10]:
            reason = []
            if data.get("max_risk", 0) >= 7:
                reason.append(f"new risk {data['max_risk']}")
            if data.get("max_existing_risk", 0) >= 7:
                reason.append(f"existing risk {data['max_existing_risk']}")
            total_crits = data.get("new_critical_count", 0) + data.get("existing_critical_count", 0)
            if total_crits >= 2:
                reason.append(f"{total_crits} criticals")
            if data.get("persist_count", 0) > 0:
                reason.append(f"{data['persist_count']} unfixed")
            rows += (
                f"| `{repo}` | {data.get('max_risk', 0)}"
                f" | {data.get('max_existing_risk', 0)}"
                f" | {data.get('new_critical_count', 0)}N/{data.get('existing_critical_count', 0)}E"
                f" | {'; '.join(reason)} |\n"
            )

        return (
            f"## Repo Shakedown Candidates\n\n"
            f"These repos are recommended for deep security scanning with repo-shakedown.\n"
            f"Full list with scan guidance exported to `shakedown-candidates-{self.month}.json`.\n\n"
            f"| Repo | Max New | Max Existing | Criticals | Reason |\n"
            f"|------|---------|--------------|-----------|--------|\n"
            f"{rows}"
        )

    # ── LLM-generated sections ───────────────────────────────────

    def _llm_executive_narrative(self) -> str:
        insights = self.llm.get("meeting_insights")
        if not insights:
            return ""
        narrative = insights.get("executive_narrative", "")
        if not narrative:
            return ""
        return (
            f"### 🧠 AI Analysis\n\n"
            f"*Generated by Gemini based on the data above.*\n\n"
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
            "*Patterns spanning multiple repos.*\n\n"
        )
        for p in patterns:
            repos_str = ", ".join(f"`{r}`" for r in p.get("affected_repos", []))
            scope = p.get("scope", "BOTH")
            scope_tag = f" [{scope}]" if scope != "BOTH" else ""
            md += f"### {p.get('pattern', 'Pattern')}{scope_tag}\n"
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

        md = "## 🧠 Team Behavior Observations (AI-detected)\n\n"
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
        return f"### 🧠 AI-Suggested Talking Points\n\n{numbered}"

    def _llm_override_evaluations(self) -> str:
        evals_data = self.llm.get("override_evaluations")
        if not evals_data:
            return ""
        evals = evals_data.get("evaluations", [])
        summary = evals_data.get("summary", "")
        if not evals:
            return ""

        md = "### 🧠 Override Reasoning Quality (AI-evaluated)\n\n"
        if summary:
            md += f"**Overall:** {summary}\n\n"

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
                    f"  - Reasoning: \"{self._md_safe(reasoning, 100)}\"\n"
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
            "*Detailed rationale and scan instructions for each candidate.*\n\n"
        )
        for c in candidates:
            urgency = c.get("urgency", "MEDIUM")
            icon = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡"}.get(urgency, "⚪")
            md += f"#### {icon} `{c.get('repo', '?')}` — {urgency}\n\n"
            md += f"{c.get('narrative', '')}\n\n"

            # Priority files
            pfiles = c.get("priority_files", [])
            if pfiles:
                md += "**Priority files for scanning:**\n"
                md += "".join(f"- `{f}`\n" for f in pfiles[:10])
                md += "\n"

            # Focus areas
            focus = c.get("focus_areas", [])
            if focus:
                md += "**Focus areas:**\n"
                md += "".join(f"- {f}\n" for f in focus)
                md += "\n"

            # Scan instructions
            instructions = c.get("scan_instructions", "")
            if instructions:
                md += f"**Scan instructions for Strix:**\n{instructions}\n\n"

            # Existing debt
            debt = c.get("existing_debt_notes", "")
            if debt:
                md += f"**Pre-existing debt to verify:** {debt}\n\n"

            risk = c.get("risk_if_ignored", "")
            if risk:
                md += f"**Risk if not scanned:** {risk}\n\n"

        return md