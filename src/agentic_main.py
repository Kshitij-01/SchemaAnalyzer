#!/usr/bin/env python3
"""SchemaAnalyzer -- Fully Agentic Entry Point.

Uses Claude Agent SDK with real Claude agents that think, decide, and adapt.
Zero deterministic code -- agents make all decisions.

Usage::

    python src/agentic_main.py "Here are my postgres creds: host=..., user=..., \\
        password=..., database=jhonson pharma. Analyze everything and give me a \\
        comprehensive report."

    python src/agentic_main.py "Connect to postgres at host=sqltosnowflake..." \\
        --run-name pharma_deep

The orchestrator (Opus 4.5) plans and coordinates.  Discovery and report agents
run on Sonnet.  The analysis agent also runs on Opus for complex reasoning.
Database tools are available as native MCP calls -- no subprocess overhead.
"""

from __future__ import annotations

import asyncio
import sys
import traceback
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_PROJECT_ROOT))

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    AgentDefinition,
    ResultMessage,
    TextBlock,
    ToolUseBlock,
    ToolResultBlock,
    ClaudeSDKError,
    ProcessError,
)

from src.tools.db_tools import db_server
from src.utils.run_manager import create_run, complete_run
from src.utils.run_logger import RunLogger


# ---------------------------------------------------------------------------
# Prompt loading
# ---------------------------------------------------------------------------

def _load_prompt(name: str) -> str:
    """Load a system prompt from ``src/prompts/``.

    Falls back to a sensible default if the file does not exist so that
    the system can still run while prompts are being iterated on.
    """
    prompts_dir = _PROJECT_ROOT / "src" / "prompts"
    path = prompts_dir / name
    if path.exists():
        return path.read_text(encoding="utf-8")
    # Fallback -- the agent will still work but with less guidance.
    fallback = (
        f"You are the {name.replace('.md', '').replace('agentic_', '')} agent "
        f"for SchemaAnalyzer. Follow the orchestrator's instructions and "
        f"write all output to the designated run directory."
    )
    print(f"[SchemaAnalyzer] WARNING: Prompt file not found: {path}")
    print(f"[SchemaAnalyzer]          Using fallback prompt.")
    return fallback


# ---------------------------------------------------------------------------
# Agent definitions
# ---------------------------------------------------------------------------

def _build_agents() -> dict[str, AgentDefinition]:
    """Construct the agent definitions for the agentic pipeline."""

    discovery_prompt = _load_prompt("agentic_discovery.md")
    analysis_prompt = _load_prompt("agentic_analysis.md")
    report_prompt = _load_prompt("agentic_report.md")

    # Every agent gets the FULL toolset -- no restrictions.
    # Any agent can spawn sub-agents, execute code, query DBs, read/write files.
    # The only difference between agents is their system prompt and model.
    _full_tools = [
        "Read", "Write", "Edit", "Bash", "Glob", "Grep", "Agent",
        "mcp__database__query_postgres",
        "mcp__database__list_postgres_schemas",
        "mcp__database__list_postgres_tables",
        "mcp__database__profile_postgres_table",
    ]

    return {
        "discovery": AgentDefinition(
            description=(
                "Connects to data sources, discovers schemas and tables, and "
                "profiles them.  Can handle any source -- Postgres, Snowflake, "
                "Delta Lake, Parquet, CSV, MongoDB.  Writes connector code on "
                "the fly if needed.  Can spawn sub-agents for parallel profiling "
                "or verification."
            ),
            prompt=discovery_prompt,
            tools=_full_tools,
            model="sonnet",
        ),
        "analysis": AgentDefinition(
            description=(
                "Deep analysis agent.  Reads all profiles and writes "
                "intelligent analysis -- relationships, quality issues, "
                "business insights, schema patterns.  Uses Opus for complex "
                "reasoning.  Can spawn verification sub-agents to re-query "
                "databases when something looks off."
            ),
            prompt=analysis_prompt,
            tools=_full_tools,
            model="opus",
        ),
        "report": AgentDefinition(
            description=(
                "Generates comprehensive HTML reports with Mermaid diagrams, "
                "Chart.js visualizations, and narrative explanations.  Writes "
                "reports as a data analyst would -- with context and "
                "recommendations.  Can spawn sub-agents to verify data or "
                "generate specific report sections."
            ),
            prompt=report_prompt,
            tools=_full_tools,
            model="sonnet",
        ),
        "verification": AgentDefinition(
            description=(
                "Verification agent for resolving doubts about profiled data. "
                "Has full DB access and code execution.  Spawned by ANY agent "
                "when they need to re-query a database to clarify a discrepancy. "
                "Can itself spawn further sub-agents if the investigation requires "
                "deeper digging."
            ),
            prompt=(
                "You are a verification agent for SchemaAnalyzer.  Another agent "
                "has a question about data in a profiled database.\n\n"
                "## Your Capabilities\n"
                "You have FULL access: database queries (MCP tools), code execution "
                "(Bash), file operations (Read/Write/Edit), and you can spawn your "
                "own sub-agents (Agent tool) if you need to parallelize or delegate "
                "part of your investigation.\n\n"
                "## Your Job\n"
                "1. Read the question and context provided\n"
                "2. Run the SQL queries needed to answer it\n"
                "3. If the investigation reveals MORE questions, spawn sub-agents "
                "or run additional queries -- go as deep as needed\n"
                "4. Write your findings to the specified output file\n"
                "5. Include raw query results, your interpretation, and a "
                "definitive answer\n\n"
                "## Be Critical\n"
                "Do not accept surface-level answers.  If a query result seems "
                "wrong, run a different query to cross-check.  If column types "
                "don't match, check pg_catalog directly.  If row counts seem off, "
                "run COUNT(*) yourself.  Your answer must be DEFINITIVE -- the "
                "asking agent is blocked until you respond.\n\n"
                "## Output Format\n"
                "Write markdown with sections: Question, Investigation "
                "(queries + results), Answer, Impact, Confidence (high/medium/low "
                "with reasoning).\n\n"
                "You are read-only.  Never modify source data.  "
                "Mask passwords in output with ***."
            ),
            tools=_full_tools,
            model="sonnet",
        ),
    }


# ---------------------------------------------------------------------------
# Task prompt builder
# ---------------------------------------------------------------------------

def _build_task_prompt(ctx, user_input: str) -> str:  # noqa: ANN001
    """Build the task prompt that the orchestrator receives."""
    return f"""\
## SchemaAnalyzer Task

**Run ID**: {ctx.run_id}
**Run Directory**: {ctx.run_dir}
**Sources Directory**: {ctx.sources_dir}
**Analysis Directory**: {ctx.analysis_dir}
**Context Directory**: {ctx.context_dir}
**Reports Directory**: {ctx.reports_dir}

The user says:
---
{user_input}
---

You are the orchestrator. Parse the input, plan your approach, and execute.
Write all output to the run directory above.
Use the discovery agent for connecting to sources and profiling.
Use the analysis agent for deep analysis (it runs on Opus for complex reasoning).
Use the report agent for generating the final HTML report.

For table profiling grunt work, you can also use the deep agent profiler:
  python {_PROJECT_ROOT}/src/deep_agents/table_profiler.py --help

The postgres connector is at:
  python {_PROJECT_ROOT}/src/deep_agents/connector_scripts/postgres_connector.py --help

Or use the MCP database tools directly (mcp__database__*).

Write a plan to {ctx.context_dir}/plan.md first, then execute.
Log progress to {ctx.context_dir}/progress.md as you go.
"""


# ---------------------------------------------------------------------------
# Main agentic loop
# ---------------------------------------------------------------------------

async def run_agentic(user_input: str, run_name: str | None = None) -> None:
    """Run the fully agentic pipeline.

    Parameters
    ----------
    user_input:
        The free-form request from the user.
    run_name:
        Optional human-friendly suffix for the run id.
    """
    # --- Create run context -------------------------------------------------
    ctx = create_run(config={"input": user_input}, run_name=run_name)
    logger = RunLogger(ctx.run_dir, ctx.run_id)
    logger.log_phase("Initialization")
    logger.log_agent_action("Run Manager", f"Created run: {ctx.run_id}")

    # --- Build agents and task prompt ---------------------------------------
    orchestrator_prompt = _load_prompt("agentic_orchestrator.md")
    agents = _build_agents()
    task = _build_task_prompt(ctx, user_input)

    # --- SDK options --------------------------------------------------------
    options = ClaudeAgentOptions(
        system_prompt=orchestrator_prompt,
        agents=agents,
        mcp_servers={"database": db_server},
        allowed_tools=[
            "Read", "Write", "Edit", "Bash", "Glob", "Grep", "Agent",
            "mcp__database__query_postgres",
            "mcp__database__list_postgres_schemas",
            "mcp__database__list_postgres_tables",
            "mcp__database__profile_postgres_table",
        ],
        permission_mode="bypassPermissions",
        model="claude-opus-4-5",
        cwd=str(_PROJECT_ROOT),
    )

    # --- Print banner -------------------------------------------------------
    print(f"[SchemaAnalyzer] Starting agentic run: {ctx.run_id}")
    print(f"[SchemaAnalyzer] Run directory: {ctx.run_dir}")
    print(f"[SchemaAnalyzer] Orchestrator: Claude Opus 4.5")
    print(f"[SchemaAnalyzer] Analysis: Claude Opus (for complex reasoning)")
    print(f"[SchemaAnalyzer] Profiling: Kimi K2 via deep agents (for grunt work)")
    print()

    logger.log_phase("Agentic Execution")
    logger.log_agent_action("Orchestrator", "Starting Claude SDK client")

    # --- Run the agent ------------------------------------------------------
    try:
        async with ClaudeSDKClient(options=options) as client:
            await client.query(task)

            async for message in client.receive_response():
                if isinstance(message, AssistantMessage):
                    for block in message.content:
                        if isinstance(block, TextBlock):
                            print(block.text, end="", flush=True)
                        elif isinstance(block, ToolUseBlock):
                            logger.log_agent_action(
                                "Orchestrator",
                                f"Tool call: {block.name}",
                            )
                        elif isinstance(block, ToolResultBlock):
                            pass  # Tool results are internal
                elif isinstance(message, ResultMessage):
                    cost = message.total_cost_usd or 0.0
                    duration_s = message.duration_ms / 1000.0
                    turns = message.num_turns

                    print(f"\n\n{'=' * 60}")
                    print(f"[SchemaAnalyzer] Agentic run complete.")
                    print(f"  Run: {ctx.run_id}")
                    print(f"  Cost: ${cost:.4f}")
                    print(f"  Duration: {duration_s:.1f}s")
                    print(f"  Turns: {turns}")
                    print(f"  Output: {ctx.run_dir}")
                    print(f"{'=' * 60}")

                    logger.log_phase("Completion")
                    logger.log_metric("cost_usd", cost)
                    logger.log_metric("duration_seconds", round(duration_s, 2))
                    logger.log_metric("turns", turns)
                    logger.finalize({
                        "cost_usd": cost,
                        "duration_seconds": round(duration_s, 2),
                        "turns": turns,
                        "status": "completed",
                    })

                    complete_run(ctx, {
                        "cost_usd": cost,
                        "duration_seconds": round(duration_s, 2),
                        "turns": turns,
                        "status": "completed",
                    })

    except ProcessError as exc:
        _handle_error(ctx, logger, "ProcessError", exc)
        raise
    except ClaudeSDKError as exc:
        _handle_error(ctx, logger, "ClaudeSDKError", exc)
        raise
    except KeyboardInterrupt:
        print("\n[SchemaAnalyzer] Interrupted by user.")
        logger.log_error("Orchestrator", "Run interrupted by user (KeyboardInterrupt)")
        logger.finalize({"status": "interrupted"})
        complete_run(ctx, {"status": "interrupted"})
    except Exception as exc:
        _handle_error(ctx, logger, "UnexpectedError", exc)
        raise


def _handle_error(ctx, logger, label: str, exc: Exception) -> None:  # noqa: ANN001
    """Log and finalise a failed run."""
    tb = traceback.format_exc()
    print(f"\n[SchemaAnalyzer] {label}: {exc}", file=sys.stderr)
    logger.log_error("Orchestrator", f"{label}: {exc}")
    logger.finalize({"status": "failed", "error": str(exc)})
    complete_run(ctx, {"status": "failed", "error": str(exc)})

    # Persist the traceback for debugging
    error_path = ctx.run_dir / "error_traceback.txt"
    error_path.write_text(tb, encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> tuple[str, str | None]:
    """Parse CLI arguments and return (user_input, run_name)."""
    if len(sys.argv) < 2:
        print(
            "Usage: python src/agentic_main.py \"<your request>\" "
            "[--run-name NAME]"
        )
        print()
        print("Examples:")
        print(
            '  python src/agentic_main.py "Connect to postgres at '
            'host=sqltosnowflake..., analyze jhonson pharma DB"'
        )
        print()
        print(
            '  python src/agentic_main.py "Analyze my DB" '
            "--run-name pharma_deep"
        )
        sys.exit(1)

    user_input = sys.argv[1]
    run_name: str | None = None

    if "--run-name" in sys.argv:
        idx = sys.argv.index("--run-name")
        if idx + 1 < len(sys.argv):
            run_name = sys.argv[idx + 1]
        else:
            print("Error: --run-name requires a value.", file=sys.stderr)
            sys.exit(1)

    return user_input, run_name


def main() -> None:
    """CLI entry point."""
    user_input, run_name = _parse_args()
    asyncio.run(run_agentic(user_input, run_name))


if __name__ == "__main__":
    main()
