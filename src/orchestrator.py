"""
Orchestrator Agent definition for SchemaAnalyzer.

The Orchestrator is the top-level Claude Agent SDK agent that:
  1. Parses user input (natural text with credentials)
  2. Writes a plan to output/context/plan.md
  3. Spawns Discovery Agents (one per data source)
  4. Reads all _summary.md files and writes master_schema.md
  5. (Future) Spawns Analysis and Report agents

This module provides the system prompt, agent definitions, and the
task prompt builder.
"""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
_ORCHESTRATOR_PROMPT_PATH = _PROMPTS_DIR / "orchestrator_system.md"


def load_orchestrator_system_prompt() -> str:
    """Load the orchestrator system prompt from disk."""
    if _ORCHESTRATOR_PROMPT_PATH.exists():
        return _ORCHESTRATOR_PROMPT_PATH.read_text(encoding="utf-8")
    return "You are the SchemaAnalyzer Orchestrator. Parse user input, plan discovery, spawn agents, assemble results."


def get_agent_definitions() -> dict[str, dict]:
    """Return all sub-agent definitions for the orchestrator.

    These are passed to ClaudeAgentOptions.agents so the orchestrator
    can spawn them via the Agent tool.

    Returns
    -------
    dict
        Keyed by agent name, each value is a dict with description, prompt, tools.
    """
    from src.agents.discovery import load_discovery_system_prompt

    # Load analysis and report prompts if available
    analysis_prompt_path = _PROMPTS_DIR / "analysis_system.md"
    report_prompt_path = _PROMPTS_DIR / "report_system.md"

    analysis_prompt = ""
    if analysis_prompt_path.exists():
        analysis_prompt = analysis_prompt_path.read_text(encoding="utf-8")

    report_prompt = ""
    if report_prompt_path.exists():
        report_prompt = report_prompt_path.read_text(encoding="utf-8")

    return {
        "discovery": {
            "description": (
                "Connects to a single data source, discovers schemas and tables, "
                "profiles them via deep agent profilers, validates quality, and "
                "writes a source _summary.md. Spawn one per data source."
            ),
            "prompt": load_discovery_system_prompt(),
            "tools": ["Read", "Write", "Edit", "Bash", "Glob", "Grep", "Agent"],
        },
        "analysis": {
            "description": (
                "Reads all source summaries and table MDs. Maps relationships, "
                "audits data quality, traces lineage. Triggers re-profiling via "
                "feedback loop if discrepancies found."
            ),
            "prompt": analysis_prompt or "You are the Analysis Agent for SchemaAnalyzer.",
            "tools": ["Read", "Write", "Edit", "Glob", "Grep", "Agent"],
        },
        "report": {
            "description": (
                "Reads all analysis and schema files. Writes final reports: "
                "executive summary, data dictionary, recommendations."
            ),
            "prompt": report_prompt or "You are the Report Agent for SchemaAnalyzer.",
            "tools": ["Read", "Write", "Edit", "Glob", "Grep"],
        },
    }


def build_orchestrator_task(user_input: str) -> str:
    """Build the full task prompt for the orchestrator.

    This wraps the user's raw input with instructions for the orchestrator
    to parse credentials, plan, and execute.

    Parameters
    ----------
    user_input:
        The user's natural language input describing their data sources
        and what they want analyzed.

    Returns
    -------
    str
        The task prompt to send to the orchestrator via query().
    """
    project_root = Path(__file__).resolve().parent.parent

    return f"""## SchemaAnalyzer Task

The user has provided the following request:

---
{user_input}
---

### Your Instructions

1. **Parse the input** to identify all data sources and their credentials.
   For each source, extract: source_type, host, port, user, password, database, schema (if specified).

2. **Create the output directory structure**:
   ```bash
   mkdir -p output/sources output/context/discovery output/context/agent_comms output/context/feedback output/analysis output/reports
   ```

3. **Write your plan** to `output/context/plan.md`:
   - List all sources found with masked credentials
   - Number of discovery agents to spawn
   - Execution order

4. **For each data source**, use the discovery agent to discover and profile all tables.

   The discovery agent needs these arguments in its task prompt:
   - Source connection details
   - Output directory: `output/sources/<source_name>/`
   - Tables directory: `output/sources/<source_name>/tables/`

   The profiler script is at: `{project_root}/src/deep_agents/table_profiler.py`
   The connector scripts are at: `{project_root}/src/deep_agents/connector_scripts/`
   The MD template is at: `{project_root}/src/prompts/templates/table_md_template.md`
   The summary template is at: `{project_root}/src/prompts/templates/summary_template.md`

5. **After all discovery agents complete**, read all `_summary.md` files from `output/sources/*/`
   and write `output/master_schema.md` — the unified view of all sources.

6. **Report back** to the user with:
   - What sources were analyzed
   - How many tables profiled
   - Location of output files
   - Any issues encountered
"""
