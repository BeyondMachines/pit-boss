"""
ShakedownCandidateBuilder — Produces a JSON list of repos that should be
deep-scanned by repo-shakedown.

Selection criteria (any match qualifies):
  - Max risk score >= threshold (default 7)
  - 2+ critical issues in the period
  - High override rate with high risk scores

Output format is designed to be consumed directly by repo-shakedown's
workflow_dispatch or repository_dispatch trigger.
"""

from typing import Any, Dict, List


class ShakedownCandidateBuilder:
    def __init__(self, analysis: Dict[str, Any], threshold: int = 7):
        self.a = analysis
        self.threshold = threshold

    def _score_repo(self, repo: str, data: Dict) -> int:
        """
        Compute a priority score for shakedown ordering.
        Higher = more urgent.
        """
        score = 0
        score += data["max_risk"] * 3          # Max risk is strongest signal
        score += data["critical_count"] * 5     # Each critical adds weight
        score += data["avg_risk"] * 2           # Sustained risk matters
        # Overrides on high-risk PRs are suspicious
        if data["high_risk_prs"]:
            overridden_high = sum(
                1 for p in data["high_risk_prs"] if p.get("was_overridden")
            )
            score += overridden_high * 4
        return round(score)

    def _determine_scan_mode(self, data: Dict) -> str:
        """Suggest scan mode based on severity."""
        if data["max_risk"] >= 9 or data["critical_count"] >= 5:
            return "deep"
        if data["max_risk"] >= 7 or data["critical_count"] >= 2:
            return "default"
        return "quick"

    def _build_reasons(self, repo: str, data: Dict) -> List[str]:
        """Human-readable reasons why this repo is flagged."""
        reasons = []
        if data["max_risk"] >= 9:
            reasons.append(f"Critical risk score ({data['max_risk']}/10)")
        elif data["max_risk"] >= 7:
            reasons.append(f"High risk score ({data['max_risk']}/10)")

        if data["critical_count"] >= 2:
            reasons.append(f"{data['critical_count']} critical issues found")

        if data["override_count"] > 0 and data["high_risk_prs"]:
            overridden = sum(
                1 for p in data["high_risk_prs"] if p.get("was_overridden")
            )
            if overridden > 0:
                reasons.append(f"{overridden} high-risk PRs were overridden")

        if data["accept_risk_count"] > data["total_prs"] * 0.5 and data["total_prs"] >= 3:
            reasons.append("Majority of PRs had risk accepted")

        if data.get("top_issues"):
            top = [rule for rule, _ in data["top_issues"][:3]]
            reasons.append(f"Recurring issues: {', '.join(top)}")

        return reasons

    def _resolve_repo_url(self, repo: str) -> str:
        """
        Convert repo identifier to a GitHub URL.
        PR-Bouncer stores repos as 'org/repo' format.
        """
        if repo.startswith("http"):
            return repo
        return f"https://github.com/{repo}"

    def build(self) -> Dict[str, Any]:
        """Build the shakedown candidate list."""
        repos = self.a["repo_risk"]
        candidates = []

        for repo, data in repos.items():
            qualifies = (
                data["max_risk"] >= self.threshold
                or data["critical_count"] >= 2
            )
            if not qualifies:
                continue

            priority = self._score_repo(repo, data)
            scan_mode = self._determine_scan_mode(data)
            reasons = self._build_reasons(repo, data)

            # Collect the specific critical issues for context
            critical_titles = []
            for pr in data["high_risk_prs"]:
                for issue in pr.get("critical_issues", []):
                    title = issue.get("title", "")
                    if title and title not in critical_titles:
                        critical_titles.append(title)

            candidates.append({
                "repo": repo,
                "repo_url": self._resolve_repo_url(repo),
                "priority_score": priority,
                "suggested_scan_mode": scan_mode,
                "max_risk_score": data["max_risk"],
                "critical_issue_count": data["critical_count"],
                "total_prs_reviewed": data["total_prs"],
                "override_count": data["override_count"],
                "reasons": reasons,
                "critical_issue_titles": critical_titles[:10],
            })

        # Sort by priority descending
        candidates.sort(key=lambda x: x["priority_score"], reverse=True)

        return {
            "generated_by": "pit-boss",
            "period": self.a.get("month_label", ""),
            "threshold": self.threshold,
            "total_candidates": len(candidates),
            "repos": candidates,
        }