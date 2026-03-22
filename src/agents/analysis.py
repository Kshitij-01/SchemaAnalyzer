"""
Analysis Agent definition for SchemaAnalyzer.

This module provides the agent definition and task prompt builder for the
Analysis Agent. In direct mode (no Claude Agent SDK), it orchestrates the
three analysis tools: relationship_analyzer, quality_auditor, feedback_engine.

In SDK mode, this defines the agent that the Orchestrator spawns.
"""

from __future__ import annotations

import sys
from pathlib import Path

_PROMPTS_DIR = Path(__file__).resolve().parents[1] / "prompts"
_ANALYSIS_PROMPT_PATH = _PROMPTS_DIR / "analysis_system.md"


def load_analysis_system_prompt() -> str:
    """Load the analysis agent system prompt from disk."""
    if _ANALYSIS_PROMPT_PATH.exists():
        return _ANALYSIS_PROMPT_PATH.read_text(encoding="utf-8")
    return "You are the Analysis Agent for SchemaAnalyzer."


def get_analysis_agent_definition() -> dict:
    """Return the agent definition dict for use with Claude Agent SDK."""
    return {
        "description": (
            "Reads all source summaries and table MDs. Maps relationships, "
            "audits data quality, traces lineage. Triggers re-profiling via "
            "feedback loop if discrepancies found."
        ),
        "prompt": load_analysis_system_prompt(),
        "tools": [
            "Read", "Write", "Edit", "Glob", "Grep", "Agent",
        ],
    }


def run_analysis_direct(output_dir: str | Path = "output") -> dict:
    """Run the full analysis pipeline in direct mode (no LLM).

    Executes all three analysis tools sequentially:
      1. Relationship Analyzer → output/analysis/relationships.md
      2. Quality Auditor → output/analysis/quality_audit.md
      3. Feedback Engine → output/context/feedback/discrepancy_report.md

    Parameters
    ----------
    output_dir:
        Root output directory containing sources/ with table MDs.

    Returns
    -------
    dict
        Summary of analysis results.
    """
    output_path = Path(output_dir)
    analysis_dir = output_path / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)

    results = {
        "relationships": None,
        "quality_audit": None,
        "feedback": None,
    }

    # --- 1. Relationship Analyzer ---
    print("\n[analysis] Phase 1: Analyzing relationships...", file=sys.stderr)
    try:
        from src.utils.relationship_analyzer import analyze_relationships
        rel_result = analyze_relationships(output_dir)
        results["relationships"] = {
            "status": "completed",
            "output": str(analysis_dir / "relationships.md"),
            "length": len(rel_result),
        }
        print(f"[analysis]   ✓ relationships.md ({len(rel_result):,} chars)", file=sys.stderr)
    except Exception as exc:
        print(f"[analysis]   ✗ Relationship analysis failed: {exc}", file=sys.stderr)
        results["relationships"] = {"status": "failed", "error": str(exc)}

    # --- 2. Quality Auditor ---
    print("\n[analysis] Phase 2: Auditing data quality...", file=sys.stderr)
    try:
        from src.utils.quality_auditor import audit_quality
        qa_result = audit_quality(output_dir)
        results["quality_audit"] = {
            "status": "completed",
            "output": str(analysis_dir / "quality_audit.md"),
            "length": len(qa_result),
        }
        print(f"[analysis]   ✓ quality_audit.md ({len(qa_result):,} chars)", file=sys.stderr)
    except Exception as exc:
        print(f"[analysis]   ✗ Quality audit failed: {exc}", file=sys.stderr)
        results["quality_audit"] = {"status": "failed", "error": str(exc)}

    # --- 3. Feedback Engine ---
    print("\n[analysis] Phase 3: Running feedback checks...", file=sys.stderr)
    try:
        from src.utils.feedback_engine import run_feedback_checks
        fb_report = run_feedback_checks(output_dir)
        results["feedback"] = {
            "status": "completed",
            "discrepancies": len(fb_report.discrepancies),
            "reprofile_requests": len(fb_report.reprofile_requests),
            "flagged_for_review": len(fb_report.flagged_for_review),
            "summary": fb_report.summary,
        }
        print(f"[analysis]   ✓ {len(fb_report.discrepancies)} discrepancies found", file=sys.stderr)
        print(f"[analysis]   ✓ {len(fb_report.reprofile_requests)} re-profile requests", file=sys.stderr)
        print(f"[analysis]   ✓ {len(fb_report.flagged_for_review)} flagged for human review", file=sys.stderr)
    except Exception as exc:
        print(f"[analysis]   ✗ Feedback engine failed: {exc}", file=sys.stderr)
        results["feedback"] = {"status": "failed", "error": str(exc)}

    # --- Summary ---
    completed = sum(1 for v in results.values() if v and v.get("status") == "completed")
    print(f"\n[analysis] Analysis complete: {completed}/3 phases succeeded.", file=sys.stderr)

    return results


if __name__ == "__main__":
    import json
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "output"
    results = run_analysis_direct(output_dir)
    print(json.dumps(results, indent=2))
