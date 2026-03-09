"""
ReviewCorrelator — Deterministic correlation of reviews and decisions.

No LLM involved. Pure data pairing and statistical analysis.
Produces a structured analysis dict consumed by ReportGenerator and
ShakedownCandidateBuilder.
"""

from collections import Counter, defaultdict
from typing import Any, Dict, List


class ReviewCorrelator:
    def __init__(self, reviews: List[Dict], decisions: List[Dict]):
        self.reviews = reviews
        self.decisions = decisions
        # Index decisions by (repo, pr_number) for fast lookup
        self.decision_index = self._build_decision_index()

    def _build_decision_index(self) -> Dict[str, List[Dict]]:
        """Key: 'repo::pr_number' → list of decisions for that PR."""
        idx = defaultdict(list)
        for d in self.decisions:
            key = f"{d['repo']}::{d['pr_number']}"
            idx[key].append(d)
        return idx

    def _get_decisions_for_pr(self, repo: str, pr: str) -> List[Dict]:
        return self.decision_index.get(f"{repo}::{pr}", [])

    def _extract_risk_score(self, review: Dict) -> int:
        r = review.get("review", {})
        return r.get("risk_score", r.get("risk_score", 0))

    def _extract_critical_issues(self, review: Dict) -> List[Dict]:
        r = review.get("review", {})
        return r.get("critical_issues", [])

    def _extract_finding_evaluations(self, review: Dict) -> List[Dict]:
        r = review.get("review", {})
        return r.get("finding_evaluations", [])

    def analyze(self) -> Dict[str, Any]:
        """Run the full deterministic analysis. Returns a structured dict."""

        # ── Per-PR correlation ───────────────────────────────────
        pr_records = []
        for rev in self.reviews:
            meta = rev.get("metadata", {})
            repo = meta.get("repo", rev.get("key_repo", "unknown"))
            pr_num = str(meta.get("pr_number", rev.get("key_pr", "0")))
            risk = self._extract_risk_score(rev)
            criticals = self._extract_critical_issues(rev)
            evals = self._extract_finding_evaluations(rev)
            decisions = self._get_decisions_for_pr(repo, pr_num)

            accept_risks = [d for d in decisions if d["type"] == "accept-risk"]
            false_positives = [d for d in decisions if d["type"] == "false-positive"]

            pr_records.append({
                "repo": repo,
                "pr_number": pr_num,
                "author": meta.get("author", ""),
                "branch": meta.get("branch", ""),
                "timestamp": meta.get("timestamp", ""),
                "risk_score": risk,
                "critical_issues": criticals,
                "critical_count": len(criticals),
                "finding_evaluations": evals,
                "confirmed_findings": [
                    e for e in evals
                    if e.get("ai_verdict") in ("CONFIRMED", "LIKELY")
                ],
                "false_positive_findings": [
                    e for e in evals
                    if e.get("ai_verdict") in ("UNLIKELY", "FALSE_POSITIVE")
                ],
                "is_summary_only": rev.get("is_summary", False),
                "accept_risks": accept_risks,
                "false_positives": false_positives,
                "was_overridden": len(accept_risks) + len(false_positives) > 0,
                "was_risk_accepted": len(accept_risks) > 0,
                "was_false_positive": len(false_positives) > 0,
            })

        # ── Aggregate stats ──────────────────────────────────────
        total_prs = len(pr_records)
        blocked = [p for p in pr_records if p["risk_score"] >= 7]
        overridden = [p for p in pr_records if p["was_overridden"]]
        risk_accepted = [p for p in pr_records if p["was_risk_accepted"]]
        false_positived = [p for p in pr_records if p["was_false_positive"]]

        # High severity PRs that were overridden (discussion-worthy)
        high_risk_overridden = [
            p for p in pr_records
            if p["risk_score"] >= 7 and p["was_overridden"]
        ]

        # Repos sorted by cumulative risk
        repo_risk = defaultdict(lambda: {
            "total_prs": 0, "total_risk": 0, "max_risk": 0,
            "critical_count": 0, "override_count": 0,
            "accept_risk_count": 0, "false_positive_count": 0,
            "high_risk_prs": [], "issue_types": Counter(),
        })
        for p in pr_records:
            r = repo_risk[p["repo"]]
            r["total_prs"] += 1
            r["total_risk"] += p["risk_score"]
            r["max_risk"] = max(r["max_risk"], p["risk_score"])
            r["critical_count"] += p["critical_count"]
            r["override_count"] += 1 if p["was_overridden"] else 0
            r["accept_risk_count"] += 1 if p["was_risk_accepted"] else 0
            r["false_positive_count"] += 1 if p["was_false_positive"] else 0
            if p["risk_score"] >= 7:
                r["high_risk_prs"].append(p)
            # Count issue types from confirmed findings
            for finding in p["confirmed_findings"]:
                rule = finding.get("rule", "unknown")
                r["issue_types"][rule] += 1

        # Most common issue types across all repos
        global_issue_types = Counter()
        for p in pr_records:
            for f in p["confirmed_findings"]:
                global_issue_types[f.get("rule", "unknown")] += 1

        # Most common AI severities on confirmed findings
        severity_dist = Counter()
        for p in pr_records:
            for f in p["confirmed_findings"]:
                severity_dist[f.get("ai_severity", "UNKNOWN")] += 1

        # Authors who override most
        override_authors = Counter()
        for p in pr_records:
            for d in p["accept_risks"] + p["false_positives"]:
                override_authors[d["author"]] += 1

        # Overrides with no reasoning
        empty_reasoning = [
            p for p in pr_records
            if p["was_overridden"]
            and all(
                not d.get("reasoning", "").strip()
                for d in p["accept_risks"] + p["false_positives"]
            )
        ]

        # Risk score distribution
        risk_distribution = Counter()
        for p in pr_records:
            bucket = "1-3 (low)" if p["risk_score"] <= 3 \
                else "4-6 (medium)" if p["risk_score"] <= 6 \
                else "7-9 (high)" if p["risk_score"] <= 9 \
                else "10 (critical)"
            risk_distribution[bucket] += 1

        return {
            "summary": {
                "total_prs": total_prs,
                "prs_blocked": len(blocked),
                "prs_overridden": len(overridden),
                "risks_accepted": len(risk_accepted),
                "false_positives": len(false_positived),
                "high_risk_overridden": len(high_risk_overridden),
                "empty_reasoning_overrides": len(empty_reasoning),
                "avg_risk_score": round(
                    sum(p["risk_score"] for p in pr_records) / max(total_prs, 1), 1
                ),
            },
            "risk_distribution": dict(risk_distribution),
            "repo_risk": {
                repo: {
                    "total_prs": data["total_prs"],
                    "total_risk": data["total_risk"],
                    "avg_risk": round(data["total_risk"] / max(data["total_prs"], 1), 1),
                    "max_risk": data["max_risk"],
                    "critical_count": data["critical_count"],
                    "override_count": data["override_count"],
                    "accept_risk_count": data["accept_risk_count"],
                    "false_positive_count": data["false_positive_count"],
                    "high_risk_prs": data["high_risk_prs"],
                    "top_issues": data["issue_types"].most_common(5),
                }
                for repo, data in repo_risk.items()
            },
            "global_issue_types": global_issue_types.most_common(15),
            "severity_distribution": dict(severity_dist),
            "override_authors": override_authors.most_common(10),
            "high_risk_overridden": high_risk_overridden,
            "empty_reasoning_overrides": empty_reasoning,
            "pr_records": pr_records,
        }