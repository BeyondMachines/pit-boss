"""
ReviewCorrelator — Deterministic correlation of reviews and decisions.

No LLM involved. Pure data pairing and statistical analysis.
Produces a structured analysis dict consumed by ReportGenerator and
ShakedownCandidateBuilder.

v2 changes:
    - Handles new/existing finding separation from pr-bouncer v2
    - Deduplicates across multiple reviews per PR (re-scans after fixes)
    - Tracks fix trends: which issues got resolved between pushes
    - Produces richer per-repo breakdowns for shakedown targeting
    - Extracts raw tool finding summaries for shakedown focus areas
"""

from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple


class ReviewCorrelator:
    def __init__(self, reviews: List[Dict], decisions: List[Dict]):
        self.reviews = reviews
        self.decisions = decisions
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

    # ── v2 score extraction ──────────────────────────────────────

    def _extract_risk_score(self, review: Dict) -> int:
        """Extract the NEW findings risk score (gates the PR)."""
        r = review.get("review", {})
        return r.get("risk_score", 0)

    def _extract_existing_risk_score(self, review: Dict) -> int:
        """Extract the EXISTING findings risk score (informational)."""
        r = review.get("review", {})
        return r.get("existing_risk_score", 0)

    def _extract_critical_issues(self, review: Dict) -> List[Dict]:
        r = review.get("review", {})
        return r.get("critical_issues", [])

    def _extract_finding_evaluations(self, review: Dict) -> List[Dict]:
        r = review.get("review", {})
        return r.get("finding_evaluations", [])

    def _extract_existing_code_issues(self, review: Dict) -> List[Dict]:
        """AI-found issues in existing code that tools missed."""
        r = review.get("review", {})
        return r.get("existing_code_issues", [])

    def _extract_summary_text(self, review: Dict) -> str:
        r = review.get("review", {})
        return r.get("summary", "")

    def _extract_recommendations(self, review: Dict) -> List[Dict]:
        r = review.get("review", {})
        return r.get("recommendations", [])

    def _extract_breaking_changes(self, review: Dict) -> List[str]:
        r = review.get("review", {})
        return r.get("breaking_changes", [])

    # ── PR deduplication & trend tracking ────────────────────────

    def _group_reviews_by_pr(self) -> Dict[str, List[Dict]]:
        """
        Group reviews by (repo, pr_number). A PR may have multiple reviews
        from re-scans after pushes.
        Returns dict keyed by 'repo::pr_number' → list of reviews sorted by timestamp.
        """
        groups = defaultdict(list)
        for rev in self.reviews:
            meta = rev.get("metadata", {})
            repo = meta.get("repo", rev.get("key_repo", "unknown"))
            pr_num = str(meta.get("pr_number", rev.get("key_pr", "0")))
            key = f"{repo}::{pr_num}"
            groups[key].append(rev)

        # Sort each group by timestamp
        for key in groups:
            groups[key].sort(
                key=lambda r: r.get("metadata", {}).get("timestamp", "")
            )
        return groups

    def _deduplicate_pr_reviews(self, reviews: List[Dict]) -> Dict[str, Any]:
        """
        Given multiple reviews for the same PR, produce a deduplicated record
        that captures:
        - The LATEST state (final risk score, final findings)
        - The trend (did risk go up or down between pushes?)
        - Which issues were fixed vs which persisted
        - The combined set of unique findings across all scans

        Returns a single merged PR record dict.
        """
        if not reviews:
            return {}

        latest = reviews[-1]
        first = reviews[0]
        scan_count = len(reviews)

        # Latest state
        meta = latest.get("metadata", {})
        repo = meta.get("repo", latest.get("key_repo", "unknown"))
        pr_num = str(meta.get("pr_number", latest.get("key_pr", "0")))

        latest_risk = self._extract_risk_score(latest)
        latest_existing_risk = self._extract_existing_risk_score(latest)
        latest_criticals = self._extract_critical_issues(latest)
        latest_evals = self._extract_finding_evaluations(latest)
        latest_existing_code = self._extract_existing_code_issues(latest)
        latest_recommendations = self._extract_recommendations(latest)
        latest_breaking = self._extract_breaking_changes(latest)

        # Separate findings by scope
        new_evals = [e for e in latest_evals if e.get("scope", "NEW") == "NEW"]
        existing_evals = [e for e in latest_evals if e.get("scope") == "EXISTING"]

        new_criticals = [c for c in latest_criticals if c.get("scope", "NEW") == "NEW"]
        existing_criticals = [c for c in latest_criticals if c.get("scope") == "EXISTING"]

        # Confirmed findings by scope
        confirmed_new = [
            e for e in new_evals
            if e.get("ai_verdict") in ("CONFIRMED", "LIKELY")
        ]
        confirmed_existing = [
            e for e in existing_evals
            if e.get("ai_verdict") in ("CONFIRMED", "LIKELY")
        ]
        false_positive_findings = [
            e for e in latest_evals
            if e.get("ai_verdict") in ("UNLIKELY", "FALSE_POSITIVE")
        ]

        # ── Trend analysis across scans ──────────────────────────
        trend = None
        issues_fixed = []
        issues_persisted = []
        issues_introduced = []

        if scan_count > 1:
            first_risk = self._extract_risk_score(first)
            first_criticals = self._extract_critical_issues(first)

            if latest_risk < first_risk:
                trend = "improving"
            elif latest_risk > first_risk:
                trend = "worsening"
            else:
                trend = "stable"

            # Track which critical issues were fixed vs persisted
            first_critical_keys = set()
            for c in first_criticals:
                k = self._finding_key(c)
                first_critical_keys.add(k)

            latest_critical_keys = set()
            for c in latest_criticals:
                k = self._finding_key(c)
                latest_critical_keys.add(k)

            issues_fixed = list(first_critical_keys - latest_critical_keys)
            issues_persisted = list(first_critical_keys & latest_critical_keys)
            issues_introduced = list(latest_critical_keys - first_critical_keys)

        # ── Deduplicated unique findings across all scans ────────
        all_unique_findings = {}
        all_unique_tool_findings = {}
        for rev in reviews:
            for e in self._extract_finding_evaluations(rev):
                k = self._finding_key(e)
                # Keep the latest evaluation for each unique finding
                all_unique_findings[k] = e
            for e in self._extract_critical_issues(rev):
                k = self._finding_key(e)
                all_unique_findings[k] = e

        # Collect raw tool finding info for shakedown targeting
        tool_findings_summary = self._collect_tool_findings(reviews)

        decisions = self._get_decisions_for_pr(repo, pr_num)
        accept_risks = [d for d in decisions if d["type"] == "accept-risk"]
        false_positives = [d for d in decisions if d["type"] == "false-positive"]

        return {
            "repo": repo,
            "pr_number": pr_num,
            "author": meta.get("author", ""),
            "branch": meta.get("branch", ""),
            "timestamp": meta.get("timestamp", ""),
            "scan_count": scan_count,
            # Latest state — new findings (gate-relevant)
            "risk_score": latest_risk,
            "existing_risk_score": latest_existing_risk,
            "critical_issues": latest_criticals,
            "critical_count": len(latest_criticals),
            "new_critical_count": len(new_criticals),
            "existing_critical_count": len(existing_criticals),
            "finding_evaluations": latest_evals,
            "new_finding_evaluations": new_evals,
            "existing_finding_evaluations": existing_evals,
            "existing_code_issues": latest_existing_code,
            "confirmed_new": confirmed_new,
            "confirmed_existing": confirmed_existing,
            "confirmed_findings": confirmed_new + confirmed_existing,
            "false_positive_findings": false_positive_findings,
            "recommendations": latest_recommendations,
            "breaking_changes": latest_breaking,
            "summary_text": self._extract_summary_text(latest),
            "is_summary_only": latest.get("is_summary", False),
            # Trend data (across re-scans)
            "trend": trend,
            "issues_fixed": issues_fixed,
            "issues_persisted": issues_persisted,
            "issues_introduced": issues_introduced,
            "risk_score_history": [
                self._extract_risk_score(r) for r in reviews
            ],
            # Deduplicated totals
            "unique_finding_count": len(all_unique_findings),
            "tool_findings_summary": tool_findings_summary,
            # Decisions
            "accept_risks": accept_risks,
            "false_positives": false_positives,
            "was_overridden": len(accept_risks) + len(false_positives) > 0,
            "was_risk_accepted": len(accept_risks) > 0,
            "was_false_positive": len(false_positives) > 0,
        }

    @staticmethod
    def _finding_key(finding: Dict) -> str:
        """
        Create a deduplication key for a finding.
        Uses file + line + rule/title to identify the same issue across scans.
        """
        f = finding.get("file", finding.get("file", ""))
        line = finding.get("line", 0)
        rule = finding.get("rule", finding.get("title", ""))
        return f"{f}::{line}::{rule}"

    @staticmethod
    def _collect_tool_findings(reviews: List[Dict]) -> Dict[str, Any]:
        """
        Collect raw tool finding info across all scans for a PR.
        Groups by tool and tracks which rules fired.
        Returns a summary suitable for shakedown targeting.
        """
        tool_rules = defaultdict(Counter)       # tool → {rule: count}
        tool_files = defaultdict(set)            # tool → {files}
        tool_severities = defaultdict(Counter)   # tool → {severity: count}

        for rev in reviews:
            for e in rev.get("review", {}).get("finding_evaluations", []):
                tool = e.get("tool", "unknown")
                rule = e.get("rule", "unknown")
                f = e.get("file", "")
                sev = e.get("tool_severity", e.get("ai_severity", "MEDIUM"))
                scope = e.get("scope", "NEW")

                tool_rules[tool][rule] += 1
                if f:
                    tool_files[tool].add(f)
                tool_severities[tool][sev] += 1

        return {
            tool: {
                "rules": dict(counter.most_common(10)),
                "files": sorted(list(files))[:20],
                "severities": dict(tool_severities[tool]),
            }
            for tool, counter in tool_rules.items()
            for files in [tool_files[tool]]
        }

    # ── Main analysis ────────────────────────────────────────────

    def analyze(self) -> Dict[str, Any]:
        """Run the full deterministic analysis. Returns a structured dict."""

        # ── Deduplicate: group by PR, merge multiple scans ───────
        pr_groups = self._group_reviews_by_pr()
        pr_records = []
        for key, reviews in pr_groups.items():
            record = self._deduplicate_pr_reviews(reviews)
            if record:
                pr_records.append(record)

        # ── Aggregate stats ──────────────────────────────────────
        total_prs = len(pr_records)
        total_scans = sum(p.get("scan_count", 1) for p in pr_records)
        blocked = [p for p in pr_records if p["risk_score"] >= 7]
        overridden = [p for p in pr_records if p["was_overridden"]]
        risk_accepted = [p for p in pr_records if p["was_risk_accepted"]]
        false_positived = [p for p in pr_records if p["was_false_positive"]]

        high_risk_overridden = [
            p for p in pr_records
            if p["risk_score"] >= 7 and p["was_overridden"]
        ]

        # PRs with multiple scans (re-pushes)
        multi_scan_prs = [p for p in pr_records if p.get("scan_count", 1) > 1]
        improving_prs = [p for p in multi_scan_prs if p.get("trend") == "improving"]
        worsening_prs = [p for p in multi_scan_prs if p.get("trend") == "worsening"]

        # ── Per-repo aggregation ─────────────────────────────────
        repo_risk = defaultdict(lambda: {
            "total_prs": 0, "total_scans": 0,
            "total_risk": 0, "max_risk": 0,
            "total_existing_risk": 0, "max_existing_risk": 0,
            "new_critical_count": 0, "existing_critical_count": 0,
            "critical_count": 0,
            "override_count": 0,
            "accept_risk_count": 0, "false_positive_count": 0,
            "high_risk_prs": [],
            "new_issue_types": Counter(),
            "existing_issue_types": Counter(),
            "all_issue_types": Counter(),
            "tool_findings": defaultdict(lambda: {"rules": Counter(), "files": set(), "severities": Counter()}),
            "existing_code_issues": [],
            "fix_count": 0,
            "persist_count": 0,
            "recommendations": [],
            "breaking_changes": [],
        })

        for p in pr_records:
            r = repo_risk[p["repo"]]
            r["total_prs"] += 1
            r["total_scans"] += p.get("scan_count", 1)
            r["total_risk"] += p["risk_score"]
            r["max_risk"] = max(r["max_risk"], p["risk_score"])
            r["total_existing_risk"] += p.get("existing_risk_score", 0)
            r["max_existing_risk"] = max(r["max_existing_risk"], p.get("existing_risk_score", 0))
            r["new_critical_count"] += p.get("new_critical_count", 0)
            r["existing_critical_count"] += p.get("existing_critical_count", 0)
            r["critical_count"] += p["critical_count"]
            r["override_count"] += 1 if p["was_overridden"] else 0
            r["accept_risk_count"] += 1 if p["was_risk_accepted"] else 0
            r["false_positive_count"] += 1 if p["was_false_positive"] else 0
            r["fix_count"] += len(p.get("issues_fixed", []))
            r["persist_count"] += len(p.get("issues_persisted", []))

            if p["risk_score"] >= 7:
                r["high_risk_prs"].append(p)

            # Count issue types from confirmed findings, split by scope
            for finding in p.get("confirmed_new", []):
                rule = finding.get("rule", "unknown")
                r["new_issue_types"][rule] += 1
                r["all_issue_types"][rule] += 1
            for finding in p.get("confirmed_existing", []):
                rule = finding.get("rule", "unknown")
                r["existing_issue_types"][rule] += 1
                r["all_issue_types"][rule] += 1

            # Aggregate tool findings for shakedown targeting
            for tool, data in p.get("tool_findings_summary", {}).items():
                t = r["tool_findings"][tool]
                for rule, count in data.get("rules", {}).items():
                    t["rules"][rule] += count
                for f in data.get("files", []):
                    t["files"].add(f)
                for sev, count in data.get("severities", {}).items():
                    t["severities"][sev] += count

            # Collect existing code issues (AI-found, tools missed)
            for issue in p.get("existing_code_issues", []):
                r["existing_code_issues"].append({
                    "pr": p["pr_number"],
                    **issue,
                })

            # Collect recommendations and breaking changes
            for rec in p.get("recommendations", []):
                r["recommendations"].append({
                    "pr": p["pr_number"],
                    "risk_score": p["risk_score"],
                    **rec,
                })
            for bc in p.get("breaking_changes", []):
                r["breaking_changes"].append({
                    "pr": p["pr_number"],
                    "change": bc,
                })

        # ── Global aggregations ──────────────────────────────────
        global_new_issue_types = Counter()
        global_existing_issue_types = Counter()
        global_all_issue_types = Counter()
        for p in pr_records:
            for f in p.get("confirmed_new", []):
                global_new_issue_types[f.get("rule", "unknown")] += 1
                global_all_issue_types[f.get("rule", "unknown")] += 1
            for f in p.get("confirmed_existing", []):
                global_existing_issue_types[f.get("rule", "unknown")] += 1
                global_all_issue_types[f.get("rule", "unknown")] += 1

        # Severity distribution split by scope
        new_severity_dist = Counter()
        existing_severity_dist = Counter()
        for p in pr_records:
            for f in p.get("confirmed_new", []):
                new_severity_dist[f.get("ai_severity", "UNKNOWN")] += 1
            for f in p.get("confirmed_existing", []):
                existing_severity_dist[f.get("ai_severity", "UNKNOWN")] += 1

        # Override authors
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

        # Risk score distribution (based on new-findings score)
        risk_distribution = Counter()
        for p in pr_records:
            bucket = (
                "1-3 (low)" if p["risk_score"] <= 3
                else "4-6 (medium)" if p["risk_score"] <= 6
                else "7-9 (high)" if p["risk_score"] <= 9
                else "10 (critical)"
            )
            risk_distribution[bucket] += 1

        # Existing risk distribution
        existing_risk_distribution = Counter()
        for p in pr_records:
            ex_score = p.get("existing_risk_score", 0)
            if ex_score > 0:
                bucket = (
                    "1-3 (low)" if ex_score <= 3
                    else "4-6 (medium)" if ex_score <= 6
                    else "7-9 (high)" if ex_score <= 9
                    else "10 (critical)"
                )
                existing_risk_distribution[bucket] += 1

        return {
            "summary": {
                "total_prs": total_prs,
                "total_scans": total_scans,
                "prs_blocked": len(blocked),
                "prs_overridden": len(overridden),
                "risks_accepted": len(risk_accepted),
                "false_positives": len(false_positived),
                "high_risk_overridden": len(high_risk_overridden),
                "empty_reasoning_overrides": len(empty_reasoning),
                "avg_risk_score": round(
                    sum(p["risk_score"] for p in pr_records) / max(total_prs, 1), 1
                ),
                "avg_existing_risk_score": round(
                    sum(p.get("existing_risk_score", 0) for p in pr_records) / max(total_prs, 1), 1
                ),
                # Trend stats
                "multi_scan_prs": len(multi_scan_prs),
                "improving_prs": len(improving_prs),
                "worsening_prs": len(worsening_prs),
                "total_issues_fixed": sum(len(p.get("issues_fixed", [])) for p in pr_records),
                "total_issues_persisted": sum(len(p.get("issues_persisted", [])) for p in pr_records),
            },
            "risk_distribution": dict(risk_distribution),
            "existing_risk_distribution": dict(existing_risk_distribution),
            "new_severity_distribution": dict(new_severity_dist),
            "existing_severity_distribution": dict(existing_severity_dist),
            "repo_risk": {
                repo: {
                    "total_prs": data["total_prs"],
                    "total_scans": data["total_scans"],
                    "total_risk": data["total_risk"],
                    "avg_risk": round(data["total_risk"] / max(data["total_prs"], 1), 1),
                    "max_risk": data["max_risk"],
                    "avg_existing_risk": round(
                        data["total_existing_risk"] / max(data["total_prs"], 1), 1
                    ),
                    "max_existing_risk": data["max_existing_risk"],
                    "critical_count": data["critical_count"],
                    "new_critical_count": data["new_critical_count"],
                    "existing_critical_count": data["existing_critical_count"],
                    "override_count": data["override_count"],
                    "accept_risk_count": data["accept_risk_count"],
                    "false_positive_count": data["false_positive_count"],
                    "high_risk_prs": data["high_risk_prs"],
                    "top_new_issues": data["new_issue_types"].most_common(5),
                    "top_existing_issues": data["existing_issue_types"].most_common(5),
                    "top_issues": data["all_issue_types"].most_common(5),
                    "tool_findings": {
                        tool: {
                            "rules": dict(tf["rules"].most_common(10)),
                            "files": sorted(list(tf["files"]))[:20],
                            "severities": dict(tf["severities"]),
                        }
                        for tool, tf in data["tool_findings"].items()
                    },
                    "existing_code_issues": data["existing_code_issues"][:10],
                    "fix_count": data["fix_count"],
                    "persist_count": data["persist_count"],
                    "recommendations": data["recommendations"][:10],
                    "breaking_changes": data["breaking_changes"][:10],
                }
                for repo, data in repo_risk.items()
            },
            "global_new_issue_types": global_new_issue_types.most_common(15),
            "global_existing_issue_types": global_existing_issue_types.most_common(15),
            "global_issue_types": global_all_issue_types.most_common(15),
            "new_severity_distribution": dict(new_severity_dist),
            "existing_severity_distribution": dict(existing_severity_dist),
            "override_authors": override_authors.most_common(10),
            "high_risk_overridden": high_risk_overridden,
            "empty_reasoning_overrides": empty_reasoning,
            "pr_records": pr_records,
        }

    # ── Weekly snapshot support ───────────────────────────────────

    def to_snapshot(self) -> Dict[str, Any]:
        """
        Produce a compact snapshot of this week's analysis that can be
        saved to S3 and later aggregated into a monthly report.
        Strips the full pr_records to save space/tokens.
        """
        analysis = self.analyze()
        # Keep everything except the heavy pr_records detail
        snapshot = {k: v for k, v in analysis.items() if k != "pr_records"}
        # Include a compact version of pr_records
        snapshot["pr_records_compact"] = [
            {
                "repo": p["repo"],
                "pr_number": p["pr_number"],
                "risk_score": p["risk_score"],
                "existing_risk_score": p.get("existing_risk_score", 0),
                "new_critical_count": p.get("new_critical_count", 0),
                "existing_critical_count": p.get("existing_critical_count", 0),
                "scan_count": p.get("scan_count", 1),
                "trend": p.get("trend"),
                "was_overridden": p["was_overridden"],
                "was_risk_accepted": p["was_risk_accepted"],
                "was_false_positive": p["was_false_positive"],
                "issues_fixed": p.get("issues_fixed", []),
                "issues_persisted": p.get("issues_persisted", []),
                "tool_findings_summary": p.get("tool_findings_summary", {}),
            }
            for p in analysis["pr_records"]
        ]
        return snapshot

    @classmethod
    def merge_snapshots(cls, snapshots: List[Dict]) -> Dict[str, Any]:
        """
        Merge multiple weekly snapshots into a single monthly analysis.
        Uses the compact pr_records from each snapshot and re-aggregates.
        Deduplicates PRs that appear in multiple weeks (same repo+PR).
        """
        if not snapshots:
            return {"summary": {"total_prs": 0}, "pr_records": [], "repo_risk": {}}

        # Collect all compact PR records, deduplicate by repo::pr
        # Keep the latest entry for each PR
        pr_by_key = {}
        for snap in snapshots:
            for p in snap.get("pr_records_compact", []):
                key = f"{p['repo']}::{p['pr_number']}"
                pr_by_key[key] = p

        # Re-aggregate from deduplicated compact records
        all_pr_records = list(pr_by_key.values())

        # Merge repo_risk from all snapshots (take max/sum as appropriate)
        merged_repo_risk = defaultdict(lambda: {
            "total_prs": 0, "total_scans": 0,
            "total_risk": 0, "max_risk": 0,
            "avg_existing_risk": 0, "max_existing_risk": 0,
            "critical_count": 0, "new_critical_count": 0,
            "existing_critical_count": 0,
            "override_count": 0, "accept_risk_count": 0,
            "false_positive_count": 0,
            "high_risk_prs": [], "top_issues": [],
            "top_new_issues": [], "top_existing_issues": [],
            "tool_findings": {},
            "existing_code_issues": [],
            "fix_count": 0, "persist_count": 0,
            "recommendations": [], "breaking_changes": [],
        })
        for snap in snapshots:
            for repo, data in snap.get("repo_risk", {}).items():
                m = merged_repo_risk[repo]
                m["total_prs"] += data.get("total_prs", 0)
                m["total_scans"] += data.get("total_scans", 0)
                m["total_risk"] += data.get("total_risk", 0)
                m["max_risk"] = max(m["max_risk"], data.get("max_risk", 0))
                m["max_existing_risk"] = max(
                    m["max_existing_risk"], data.get("max_existing_risk", 0)
                )
                m["critical_count"] += data.get("critical_count", 0)
                m["new_critical_count"] += data.get("new_critical_count", 0)
                m["existing_critical_count"] += data.get("existing_critical_count", 0)
                m["override_count"] += data.get("override_count", 0)
                m["fix_count"] += data.get("fix_count", 0)
                m["persist_count"] += data.get("persist_count", 0)

        # Compute averages
        for repo, m in merged_repo_risk.items():
            m["avg_risk"] = round(
                m["total_risk"] / max(m["total_prs"], 1), 1
            )

        # Merge global counters
        merged_issue_types = Counter()
        merged_new_issue_types = Counter()
        merged_existing_issue_types = Counter()
        for snap in snapshots:
            for rule, count in snap.get("global_issue_types", []):
                merged_issue_types[rule] += count
            for rule, count in snap.get("global_new_issue_types", []):
                merged_new_issue_types[rule] += count
            for rule, count in snap.get("global_existing_issue_types", []):
                merged_existing_issue_types[rule] += count

        # Merge summary counters
        total_prs = len(all_pr_records)
        return {
            "summary": {
                "total_prs": total_prs,
                "total_scans": sum(s.get("summary", {}).get("total_scans", 0) for s in snapshots),
                "prs_blocked": sum(1 for p in all_pr_records if p["risk_score"] >= 7),
                "prs_overridden": sum(1 for p in all_pr_records if p["was_overridden"]),
                "risks_accepted": sum(1 for p in all_pr_records if p["was_risk_accepted"]),
                "false_positives": sum(1 for p in all_pr_records if p["was_false_positive"]),
                "high_risk_overridden": sum(
                    1 for p in all_pr_records
                    if p["risk_score"] >= 7 and p["was_overridden"]
                ),
                "empty_reasoning_overrides": 0,  # not tracked in compact
                "avg_risk_score": round(
                    sum(p["risk_score"] for p in all_pr_records) / max(total_prs, 1), 1
                ),
                "avg_existing_risk_score": round(
                    sum(p.get("existing_risk_score", 0) for p in all_pr_records) / max(total_prs, 1), 1
                ),
                "multi_scan_prs": sum(1 for p in all_pr_records if p.get("scan_count", 1) > 1),
                "improving_prs": sum(1 for p in all_pr_records if p.get("trend") == "improving"),
                "worsening_prs": sum(1 for p in all_pr_records if p.get("trend") == "worsening"),
                "total_issues_fixed": sum(len(p.get("issues_fixed", [])) for p in all_pr_records),
                "total_issues_persisted": sum(len(p.get("issues_persisted", [])) for p in all_pr_records),
                "aggregated_from_snapshots": len(snapshots),
            },
            "repo_risk": dict(merged_repo_risk),
            "global_issue_types": merged_issue_types.most_common(15),
            "global_new_issue_types": merged_new_issue_types.most_common(15),
            "global_existing_issue_types": merged_existing_issue_types.most_common(15),
            "pr_records": all_pr_records,
            "override_authors": Counter(),  # not fully tracked in compact
            "high_risk_overridden": [
                p for p in all_pr_records
                if p["risk_score"] >= 7 and p["was_overridden"]
            ],
            "empty_reasoning_overrides": [],
            "risk_distribution": {},
            "existing_risk_distribution": {},
            "new_severity_distribution": {},
            "existing_severity_distribution": {},
        }