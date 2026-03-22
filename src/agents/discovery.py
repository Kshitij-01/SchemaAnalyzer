"""
Discovery Agent definition for SchemaAnalyzer.

This module provides the agent definition, system prompt, and helper functions
that the Orchestrator uses when spawning a Discovery Agent via Claude Agent SDK.

The Discovery Agent:
  1. Connects to one data source
  2. Lists all schemas and tables
  3. Spawns deep agent profilers (cheap models) for batching
  4. Validates profiled MDs
  5. Re-profiles with Claude if quality is bad
  6. Writes _summary.md for the source
"""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_PROMPTS_DIR = Path(__file__).resolve().parents[1] / "prompts"
_DISCOVERY_PROMPT_PATH = _PROMPTS_DIR / "discovery_system.md"
_TABLE_TEMPLATE_PATH = _PROMPTS_DIR / "templates" / "table_md_template.md"
_SUMMARY_TEMPLATE_PATH = _PROMPTS_DIR / "templates" / "summary_template.md"


def load_discovery_system_prompt() -> str:
    """Load the discovery agent system prompt from disk."""
    if _DISCOVERY_PROMPT_PATH.exists():
        return _DISCOVERY_PROMPT_PATH.read_text(encoding="utf-8")
    return "You are a Discovery Agent for SchemaAnalyzer. Discover and profile all tables in the assigned data source."


def get_discovery_agent_definition() -> dict:
    """Return the agent definition dict for use with Claude Agent SDK.

    This is passed to ClaudeAgentOptions.agents or used in the orchestrator
    to define what the discovery agent can do.

    Returns
    -------
    dict
        Keys: description, prompt, tools (all built-in Claude Code tools + Agent).
    """
    return {
        "description": (
            "Connects to a single data source, discovers all schemas and tables, "
            "spawns deep agent profilers for batched table profiling, validates "
            "output quality, re-profiles with Claude if needed, and writes a "
            "source _summary.md."
        ),
        "prompt": load_discovery_system_prompt(),
        "tools": [
            "Read",
            "Write",
            "Edit",
            "Bash",
            "Glob",
            "Grep",
            "Agent",
        ],
    }


def build_discovery_task_prompt(
    source_name: str,
    source_type: str,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    schema_filter: str | None = None,
    output_base: str = "output/sources",
) -> str:
    """Build the task prompt that the orchestrator sends to the discovery agent.

    Parameters
    ----------
    source_name:
        Human-readable name for this source (e.g., "postgres_main").
    source_type:
        One of: postgres, snowflake, mysql, etc.
    host, port, user, password, database:
        Connection credentials.
    schema_filter:
        Optional schema name to limit discovery to.
    output_base:
        Base directory for output. Tables go to {output_base}/{source_name}/tables/.

    Returns
    -------
    str
        The full task prompt for the discovery agent.
    """
    project_root = Path(__file__).resolve().parents[2]
    connector_script = project_root / "src" / "deep_agents" / "connector_scripts" / f"{source_type}_connector.py"
    profiler_script = project_root / "src" / "deep_agents" / "table_profiler.py"
    template_path = _TABLE_TEMPLATE_PATH
    summary_template_path = _SUMMARY_TEMPLATE_PATH

    output_dir = f"{output_base}/{source_name}"
    tables_dir = f"{output_dir}/tables"
    context_dir = f"output/context/discovery"

    schema_clause = ""
    if schema_filter:
        schema_clause = f"\n**Scope**: Only profile schema `{schema_filter}`."

    prompt = f"""## Discovery Task: {source_name}

**Source Type**: {source_type}
**Host**: {host}
**Port**: {port}
**Database**: {database}
**User**: {user}
**Password**: {password}
{schema_clause}

**Output directory**: {output_dir}/
**Tables directory**: {tables_dir}/
**Context log**: {context_dir}/{source_name}.log

---

### Step 1: Connect and Discover

Test the connection and list all schemas and tables:

```bash
python {connector_script} --host {host} --port {port} --user {user} --password {password} --database {database} list-schemas
```

Then for each schema (or just `{schema_filter or "all schemas"}`):

```bash
python {connector_script} --host {host} --port {port} --user {user} --password {password} --database {database} list-tables --schema <SCHEMA>
```

### Step 2: Profile Tables via Deep Agent

For each batch of up to 25 tables, run the table profiler:

```bash
python {profiler_script} --source-type {source_type} --host {host} --port {port} --db {database} --user {user} --password {password} --schema <SCHEMA> --tables "table1,table2,..." --output-dir {tables_dir} --template {template_path} --source-name {source_name} --no-llm
```

Note: Use `--no-llm` for direct profiling (fast, reliable). The profiler will call the connector and format the output into MD files.

### Step 3: Validate Table Profiles

After all batches complete, read each MD file in `{tables_dir}/` and verify:
- Columns section is non-empty
- Statistics section has row count
- Profiling metadata section exists

If any MD is incomplete, re-profile that table yourself by running the connector directly and writing the MD.

### Step 4: Write Source Summary

Read ALL table MD files from `{tables_dir}/`.
Use the summary template at `{summary_template_path}` as guidance.
Write the source summary to `{output_dir}/_summary.md`.

The summary should include:
- Connection info (password masked as ***)
- Total tables, schemas, estimated total rows
- Table overview (name, row count, column count per table)
- Key relationships (foreign keys found across tables)
- Data quality flags (tables with no PK, high null %, etc.)
- Profiling report (how many tables, any re-profiles)

### Step 5: Log Completion

Write completion status to `{context_dir}/{source_name}.log`.
"""
    return prompt
