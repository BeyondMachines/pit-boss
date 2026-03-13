"""
ShakedownCandidateBuilder — Produces a JSON list of repos that should be
deep-scanned by repo-shakedown.

Selection criteria (any match qualifies):
  - Max NEW risk score >= threshold (default 7)
  - Max EXISTING risk score >= threshold (accumulated debt)
  - 2+ critical issues in the period (new or existing)
  - High override rate with high risk scores
  - Issues that persisted across multiple re-scans (unfixed)

Output includes:
  - Priority score incorporating both new and existing risk
  - Suggested scan mode (quick/default/deep)
  - Specific file paths and rules for the scanner to focus on
  - Structured scan_guidance for Strix to avoid wasting tokens

v2: Now considers both NEW and EXISTING findings for candidate selection
    and produces much richer targeting data for repo-shakedown.
"""

from collections import Counter
from typing import Any, Dict, List


class ShakedownCandidateBuilder:
    def __init__(self, analysis: Dict[str, Any], threshold: int = 7):
        self.a = analysis
        self.threshold = threshold

    def _score_repo(self, repo: str, data: Dict) -> int:
        """
        Compute a priority score for shakedown ordering.
        Higher = more urgent. Considers both new and existing risk.
        """
        score = 0
        # New findings (primary signal — active development risk)
        score += data["max_risk"] * 3
        score += data.get("new_critical_count", 0) * 5
        score += data["avg_risk"] * 2

        # Existing findings (secondary signal — accumulated debt)
        score += data.get("max_existing_risk", 0) * 2
        score += data.get("existing_critical_count", 0) * 3

        # Overrides on high-risk PRs are suspicious
        if data.get("high_risk_prs"):
            overridden_high = sum(
                1 for p in data["high_risk_prs"] if p.get("was_overridden")
            )
            score += overridden_high * 4

        # Unfixed issues across re-scans penalize further
        score += data.get("persist_count", 0) * 2

        # Credit for fixing issues
        score -= data.get("fix_count", 0)

        return max(0, round(score))

    def _determine_scan_mode(self, data: Dict) -> str:
        """Suggest scan mode based on severity and volume."""
        max_combined = max(data["max_risk"], data.get("max_existing_risk", 0))
        total_criticals = data.get("new_critical_count", 0) + data.get("existing_critical_count", 0)

        if max_combined >= 9 or total_criticals >= 5:
            return "deep"
        if max_combined >= 7 or total_criticals >= 2:
            return "default"
        return "quick"

    def _build_reasons(self, repo: str, data: Dict) -> List[str]:
        """Human-readable reasons why this repo is flagged."""
        reasons = []

        # New risk
        if data["max_risk"] >= 9:
            reasons.append(f"Critical NEW risk score ({data['max_risk']}/10)")
        elif data["max_risk"] >= 7:
            reasons.append(f"High NEW risk score ({data['max_risk']}/10)")

        # Existing risk
        existing_max = data.get("max_existing_risk", 0)
        if existing_max >= 7:
            reasons.append(f"High EXISTING risk score ({existing_max}/10) — pre-existing debt")

        # Critical counts
        new_crits = data.get("new_critical_count", 0)
        existing_crits = data.get("existing_critical_count", 0)
        if new_crits >= 2:
            reasons.append(f"{new_crits} NEW critical issues found")
        if existing_crits >= 2:
            reasons.append(f"{existing_crits} EXISTING critical issues (technical debt)")

        # Overrides
        if data["override_count"] > 0 and data.get("high_risk_prs"):
            overridden = sum(
                1 for p in data["high_risk_prs"] if p.get("was_overridden")
            )
            if overridden > 0:
                reasons.append(f"{overridden} high-risk PRs were overridden")

        # Unfixed issues
        if data.get("persist_count", 0) > 0:
            reasons.append(f"{data['persist_count']} issues persisted across re-scans")

        # Recurring issue types
        top_issues = data.get("top_issues", [])
        if top_issues:
            top = [rule for rule, _ in top_issues[:3]]
            reasons.append(f"Recurring issues: {', '.join(top)}")

        return reasons

    def _build_scan_guidance(self, repo: str, data: Dict) -> Dict[str, Any]:
        """
        Build structured scan guidance for Strix.
        Tells the scanner exactly where to look and what to test.
        """
        # Collect all files with findings
        priority_files = set()
        focus_rules = Counter()

        for tool, tf in data.get("tool_findings", {}).items():
            for f in tf.get("files", []):
                priority_files.add(f)
            for rule, count in tf.get("rules", {}).items():
                focus_rules[rule] += count

        # Files from critical issues
        for pr in data.get("high_risk_prs", []):
            for issue in pr.get("critical_issues", []):
                f = issue.get("file", "")
                if f:
                    priority_files.add(f)

        # Files from existing code issues
        for issue in data.get("existing_code_issues", []):
            f = issue.get("file", "")
            if f:
                priority_files.add(f)

        # Collect critical issue titles for context
        new_critical_titles = []
        existing_critical_titles = []
        for pr in data.get("high_risk_prs", []):
            for issue in pr.get("critical_issues", []):
                title = issue.get("title", "")
                scope = issue.get("scope", "NEW")
                if title:
                    if scope == "NEW" and title not in new_critical_titles:
                        new_critical_titles.append(title)
                    elif scope == "EXISTING" and title not in existing_critical_titles:
                        existing_critical_titles.append(title)

        # Existing code issues (AI-found)
        existing_ai_issues = [
            {
                "title": i.get("title", ""),
                "file": i.get("file", ""),
                "severity": i.get("severity", "MEDIUM"),
            }
            for i in data.get("existing_code_issues", [])[:5]
        ]

        return {
            "priority_files": sorted(list(priority_files))[:25],
            "focus_rules": focus_rules.most_common(10),
            "new_critical_titles": new_critical_titles[:10],
            "existing_critical_titles": existing_critical_titles[:10],
            "existing_ai_issues": existing_ai_issues,
            "tool_severities": {
                tool: tf.get("severities", {})
                for tool, tf in data.get("tool_findings", {}).items()
            },
        }

    def _resolve_repo_url(self, repo: str) -> str:
        if repo.startswith("https://github.com/"):
            return repo
        if repo.startswith("http://github.com"):
            return repo.replace("http://", "https://", 1)
        return f"https://github.com/{repo}"

    def build(self) -> Dict[str, Any]:
        """Build the shakedown candidate list."""
        repos = self.a["repo_risk"]
        candidates = []

        for repo, data in repos.items():
            new_qualifies = data["max_risk"] >= self.threshold
            existing_qualifies = data.get("max_existing_risk", 0) >= self.threshold
            critical_qualifies = (
                data.get("new_critical_count", 0) + data.get("existing_critical_count", 0)
            ) >= 2
            persist_qualifies = data.get("persist_count", 0) >= 3

            if not (new_qualifies or existing_qualifies or critical_qualifies or persist_qualifies):
                continue

            priority = self._score_repo(repo, data)
            scan_mode = self._determine_scan_mode(data)
            reasons = self._build_reasons(repo, data)
            scan_guidance = self._build_scan_guidance(repo, data)

            # Collect critical titles
            all_critical_titles = (
                scan_guidance["new_critical_titles"]
                + scan_guidance["existing_critical_titles"]
            )

            candidates.append({
                "repo": repo,
                "repo_url": self._resolve_repo_url(repo),
                "priority_score": priority,
                "suggested_scan_mode": scan_mode,
                "max_risk_score": data["max_risk"],
                "max_existing_risk_score": data.get("max_existing_risk", 0),
                "new_critical_count": data.get("new_critical_count", 0),
                "existing_critical_count": data.get("existing_critical_count", 0),
                "critical_issue_count": data.get("new_critical_count", 0) + data.get("existing_critical_count", 0),
                "total_prs_reviewed": data["total_prs"],
                "override_count": data["override_count"],
                "fix_count": data.get("fix_count", 0),
                "persist_count": data.get("persist_count", 0),
                "reasons": reasons,
                "critical_issue_titles": all_critical_titles[:10],
                "scan_guidance": scan_guidance,
                # Qualification flags
                "qualified_by": {
                    "new_risk": new_qualifies,
                    "existing_risk": existing_qualifies,
                    "critical_count": critical_qualifies,
                    "unfixed_issues": persist_qualifies,
                },
            })

        candidates.sort(key=lambda x: x["priority_score"], reverse=True)

        return {
            "generated_by": "pit-boss",
            "period": self.a.get("month_label", ""),
            "threshold": self.threshold,
            "total_candidates": len(candidates),
            "repos": candidates,
        }