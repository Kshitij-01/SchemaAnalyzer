#!/usr/bin/env python3
"""
Deep Agent Table Profiler for SchemaAnalyzer.

This script is invoked by the Discovery Agent (Claude) via Bash. It spins up
a LangGraph deep agent running on a cheap model (DeepSeek, Kimi, etc.) that:

  1. Connects to the database using the postgres_connector.py script.
  2. Profiles a batch of tables (runs profile-batch command).
  3. Writes one Markdown file per table using the template.

Usage:
    python table_profiler.py \
        --source-type postgres \
        --host HOST --port PORT --db DB --user USER --password PASS \
        --schema SCHEMA \
        --tables "table1,table2,table3" \
        --output-dir "output/schemas/postgres/public/" \
        --template "src/prompts/templates/table_md_template.md" \
        [--model deepseek_v3_1] \
        [--env-info path/to/env_info_clean.json]

The script uses the ModelRouter to pick the cheapest working model from
env_info_clean.json, then creates a LangGraph deep agent with that model.

If deepagents is not installed, falls back to direct profiling (no LLM) by
calling the connector script and formatting the output into Markdown directly.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Ensure project root is on sys.path so we can import our modules
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.model_router import ModelRouter, ModelConfig  # noqa: E402


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CONNECTOR_SCRIPTS = {
    "postgres": _PROJECT_ROOT / "src" / "deep_agents" / "connector_scripts" / "postgres_connector.py",
    "snowflake": _PROJECT_ROOT / "src" / "deep_agents" / "connector_scripts" / "snowflake_connector.py",
}

BATCH_SIZE = 25  # tables per deep-agent invocation


# ---------------------------------------------------------------------------
# Direct profiling (fallback — no LLM needed)
# ---------------------------------------------------------------------------


def _run_connector(
    source_type: str,
    host: str,
    port: int,
    db: str,
    user: str,
    password: str,
    command: str,
    extra_args: list[str] | None = None,
) -> dict[str, Any] | list[Any]:
    """Execute the connector script and return parsed JSON output."""
    script = CONNECTOR_SCRIPTS.get(source_type)
    if script is None:
        raise ValueError(f"No connector script for source type: {source_type}")

    cmd = [
        sys.executable, str(script),
        "--host", host,
        "--port", str(port),
        "--user", user,
        "--password", password,
        "--database", db,
        command,
    ]
    if extra_args:
        cmd.extend(extra_args)

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=300,  # 5 min per command
    )

    if result.returncode != 0:
        # Try to parse error JSON from stdout
        try:
            return json.loads(result.stdout)
        except (json.JSONDecodeError, ValueError):
            return {"error": f"Connector exited with code {result.returncode}", "stderr": result.stderr[:500]}

    return json.loads(result.stdout)


def _format_table_md(
    profile: dict[str, Any],
    source_name: str,
    source_type: str,
    database: str,
    model_name: str = "direct-profiler",
) -> str:
    """Format a single table profile dict into a Markdown string."""
    schema = profile.get("schema", "unknown")
    table = profile.get("table", "unknown")
    now = datetime.now(timezone.utc).isoformat()

    lines: list[str] = []
    lines.append(f"# Table Profile: {schema}.{table}")
    lines.append("")
    lines.append("| Property | Value |")
    lines.append("|----------|-------|")
    lines.append(f"| **Source** | {source_name} |")
    lines.append(f"| **Schema** | {schema} |")
    lines.append(f"| **Table** | {table} |")
    lines.append(f"| **Type** | TABLE |")
    lines.append(f"| **Database** | {database} |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ---- Columns ----
    columns = profile.get("columns", [])
    lines.append("## Columns")
    lines.append("")
    lines.append("| # | Column Name | Data Type | Max Length | Precision | Scale | Nullable | Default | Description |")
    lines.append("|---|-------------|-----------|-----------|-----------|-------|----------|---------|-------------|")
    for col in columns:
        lines.append(
            f"| {col.get('ordinal_position', '')} "
            f"| {col.get('column_name', '')} "
            f"| {col.get('data_type', '')} "
            f"| {col.get('character_maximum_length', '--')} "
            f"| {col.get('numeric_precision', '--')} "
            f"| {col.get('numeric_scale', '--')} "
            f"| {col.get('is_nullable', '')} "
            f"| {col.get('column_default', '--')} "
            f"| -- |"
        )
    if not columns:
        lines.append("| -- | No columns found | -- | -- | -- | -- | -- | -- | -- |")
    lines.append("")
    lines.append(f"**Total Columns**: {len(columns)}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ---- Constraints ----
    constraints = profile.get("constraints", [])
    pk_constraints = [c for c in constraints if c.get("constraint_type") == "PRIMARY KEY"]
    unique_constraints = [c for c in constraints if c.get("constraint_type") == "UNIQUE"]
    check_constraints = [c for c in constraints if c.get("constraint_type") == "CHECK"]

    lines.append("## Constraints")
    lines.append("")
    lines.append("### Primary Key")
    lines.append("")
    lines.append("| Constraint Name | Columns |")
    lines.append("|----------------|---------|")
    if pk_constraints:
        # Group by constraint name
        pk_grouped: dict[str, list[str]] = {}
        for c in pk_constraints:
            name = c.get("constraint_name", "")
            pk_grouped.setdefault(name, []).append(c.get("column_name", ""))
        for name, cols in pk_grouped.items():
            lines.append(f"| {name} | {', '.join(cols)} |")
    else:
        lines.append("| None | -- |")
    lines.append("")

    lines.append("### Unique Constraints")
    lines.append("")
    lines.append("| Constraint Name | Columns |")
    lines.append("|----------------|---------|")
    if unique_constraints:
        uq_grouped: dict[str, list[str]] = {}
        for c in unique_constraints:
            name = c.get("constraint_name", "")
            uq_grouped.setdefault(name, []).append(c.get("column_name", ""))
        for name, cols in uq_grouped.items():
            lines.append(f"| {name} | {', '.join(cols)} |")
    else:
        lines.append("| None | -- |")
    lines.append("")

    lines.append("### Check Constraints")
    lines.append("")
    lines.append("| Constraint Name | Check Clause |")
    lines.append("|----------------|--------------|")
    if check_constraints:
        for c in check_constraints:
            lines.append(f"| {c.get('constraint_name', '')} | {c.get('check_clause', '--')} |")
    else:
        lines.append("| None | -- |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ---- Indexes ----
    indexes = profile.get("indexes", [])
    lines.append("## Indexes")
    lines.append("")
    lines.append("| Index Name | Definition | Unique | Primary | Type |")
    lines.append("|-----------|-----------|--------|---------|------|")
    for idx in indexes:
        defn = idx.get("index_definition", "--")
        # Truncate long definitions
        if len(defn) > 80:
            defn = defn[:77] + "..."
        is_unique = "YES" if "UNIQUE" in defn.upper() else "NO"
        is_primary = "YES" if "pkey" in idx.get("index_name", "").lower() else "NO"
        lines.append(f"| {idx.get('index_name', '')} | {defn} | {is_unique} | {is_primary} | btree |")
    if not indexes:
        lines.append("| None | -- | -- | -- | -- |")
    lines.append("")
    lines.append(f"**Total Indexes**: {len(indexes)}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ---- Foreign Keys ----
    fk_out = profile.get("foreign_keys_outgoing", [])
    fk_in = profile.get("foreign_keys_incoming", [])

    lines.append("## Foreign Keys")
    lines.append("")
    lines.append("### Outgoing (This Table References)")
    lines.append("")
    lines.append("| Constraint Name | Column | Referenced Schema | Referenced Table | Referenced Column |")
    lines.append("|----------------|--------|-------------------|-----------------|-------------------|")
    if fk_out:
        for fk in fk_out:
            lines.append(
                f"| {fk.get('constraint_name', '')} "
                f"| {fk.get('column_name', '')} "
                f"| {fk.get('foreign_schema', '')} "
                f"| {fk.get('foreign_table', '')} "
                f"| {fk.get('foreign_column', '')} |"
            )
    else:
        lines.append("| None | -- | -- | -- | -- |")
    lines.append("")

    lines.append("### Incoming (Referenced By)")
    lines.append("")
    lines.append("| Constraint Name | Source Schema | Source Table | Source Column |")
    lines.append("|----------------|--------------|-------------|---------------|")
    if fk_in:
        for fk in fk_in:
            lines.append(
                f"| {fk.get('constraint_name', '')} "
                f"| {fk.get('source_schema', '')} "
                f"| {fk.get('source_table', '')} "
                f"| {fk.get('source_column', '')} |"
            )
    else:
        lines.append("| None | -- | -- | -- |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ---- Statistics ----
    lines.append("## Statistics")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| **Row Count** | {profile.get('row_count', 'N/A')} |")
    lines.append(f"| **Total Size** | {profile.get('table_size', 'N/A')} |")
    lines.append("")

    # Null percentages
    null_pcts = profile.get("null_percentages", {})
    lines.append("### Null Percentages")
    lines.append("")
    lines.append("| Column Name | Null % |")
    lines.append("|------------|--------|")
    for col_name, pct in null_pcts.items():
        if col_name.startswith("_"):
            continue
        pct_str = f"{pct:.2f}%" if pct is not None else "N/A"
        lines.append(f"| {col_name} | {pct_str} |")
    if not null_pcts:
        lines.append("| -- | N/A |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ---- Sample Data ----
    sample = profile.get("sample_data", [])
    lines.append("## Sample Data (5 Rows)")
    lines.append("")
    if sample:
        headers = list(sample[0].keys())
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
        for row in sample[:5]:
            vals = []
            for h in headers:
                v = str(row.get(h, ""))
                if len(v) > 50:
                    v = v[:47] + "..."
                # Escape pipe characters in values
                v = v.replace("|", "\\|")
                vals.append(v)
            lines.append("| " + " | ".join(vals) + " |")
    else:
        lines.append("_No sample data available (table may be empty)._")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ---- Profiling Metadata ----
    lines.append("## Profiling Metadata")
    lines.append("")
    lines.append("| Property | Value |")
    lines.append("|----------|-------|")
    lines.append(f"| **Profiled By** | {model_name} |")
    lines.append(f"| **Model** | {model_name} |")
    lines.append(f"| **Timestamp** | {now} |")
    lines.append(f"| **Re-Profiled** | false |")
    lines.append(f"| **Re-Profile Reason** | -- |")
    lines.append(f"| **Source Connector** | {source_type}_connector |")
    lines.append(f"| **Profiling Duration** | -- |")

    return "\n".join(lines)


def profile_tables_direct(
    source_type: str,
    host: str,
    port: int,
    db: str,
    user: str,
    password: str,
    schema: str,
    tables: list[str],
    output_dir: str,
    source_name: str | None = None,
    model_name: str = "direct-profiler",
) -> dict[str, Any]:
    """Profile tables by calling the connector directly (no LLM).

    This is the primary path: call the connector's profile-batch command,
    then format each profile into a Markdown file.

    Returns a summary dict with counts of successes and failures.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    src_name = source_name or f"{source_type}_{host}"

    # Run profile-batch via the connector
    print(f"[table_profiler] Profiling {len(tables)} tables in {schema} via {source_type}_connector...", file=sys.stderr)

    profiles = _run_connector(
        source_type=source_type,
        host=host,
        port=port,
        db=db,
        user=user,
        password=password,
        command="profile-batch",
        extra_args=["--schema", schema, "--tables", ",".join(tables)],
    )

    if isinstance(profiles, dict) and "error" in profiles:
        print(f"[table_profiler] ERROR from connector: {profiles}", file=sys.stderr)
        return {"status": "error", "error": profiles, "tables_profiled": 0, "tables_failed": len(tables)}

    if not isinstance(profiles, list):
        profiles = [profiles]

    successes = 0
    failures = 0

    for profile in profiles:
        table_name = profile.get("table", "unknown")
        try:
            md_content = _format_table_md(
                profile=profile,
                source_name=src_name,
                source_type=source_type,
                database=db,
                model_name=model_name,
            )
            file_path = output_path / f"{schema}.{table_name}.md"
            file_path.write_text(md_content, encoding="utf-8")
            print(f"[table_profiler] Wrote {file_path.name}", file=sys.stderr)
            successes += 1
        except Exception as exc:
            print(f"[table_profiler] FAILED to write {table_name}: {exc}", file=sys.stderr)
            failures += 1

    result = {
        "status": "completed",
        "tables_profiled": successes,
        "tables_failed": failures,
        "output_dir": str(output_path),
        "model": model_name,
    }
    return result


# ---------------------------------------------------------------------------
# Deep Agent profiling (LangGraph + cheap model)
# ---------------------------------------------------------------------------


def _try_deep_agent_profiling(
    model_config: ModelConfig,
    source_type: str,
    host: str,
    port: int,
    db: str,
    user: str,
    password: str,
    schema: str,
    tables: list[str],
    output_dir: str,
    template_path: str,
    source_name: str | None = None,
) -> dict[str, Any] | None:
    """Attempt to profile tables using a LangGraph deep agent.

    Returns a result dict on success, or None if deepagents is unavailable.
    """
    try:
        from deepagents import create_deep_agent
        from deepagents.backends import FilesystemBackend
        from langchain.chat_models import init_chat_model
    except ImportError:
        print("[table_profiler] deepagents not installed; falling back to direct profiling.", file=sys.stderr)
        return None

    # Build the LangChain model from our ModelConfig
    model_kwargs: dict[str, Any] = {
        "api_key": model_config.api_key,
    }
    if model_config.provider == "azure_ai_foundry":
        model_kwargs["base_url"] = model_config.endpoint.replace(
            "/models/chat/completions?api-version=2024-05-01-preview", "/openai/v1/"
        )

    model_string = ModelRouter.get_langchain_model_string(model_config)

    try:
        model = init_chat_model(model_string, **model_kwargs)
    except Exception as exc:
        print(f"[table_profiler] Failed to init model {model_string}: {exc}", file=sys.stderr)
        return None

    # Read the system prompt
    profiler_prompt_path = _PROJECT_ROOT / "src" / "prompts" / "table_profiler_system.md"
    system_prompt = profiler_prompt_path.read_text(encoding="utf-8") if profiler_prompt_path.exists() else ""

    # Read the template
    template_content = ""
    if template_path and Path(template_path).exists():
        template_content = Path(template_path).read_text(encoding="utf-8")

    # Build the connector command string for the agent
    connector_cmd = (
        f"python {CONNECTOR_SCRIPTS[source_type]} "
        f"--host {host} --port {port} --user {user} --password {password} --database {db}"
    )

    table_list_str = ",".join(f"{schema}.{t}" for t in tables)

    task_prompt = f"""Profile the following tables and write one MD file per table to {output_dir}:

Tables: {table_list_str}

Database connection command prefix:
{connector_cmd}

To profile all tables at once, run:
{connector_cmd} profile-batch --schema {schema} --tables {','.join(tables)}

Template for the MD files:
{template_content}

For each table in the JSON output, create a well-formatted MD file named <schema>.<table>.md in {output_dir}.
Follow the template exactly.
"""

    try:
        backend = FilesystemBackend(root_dir=str(_PROJECT_ROOT))
        agent = create_deep_agent(
            model=model,
            system_prompt=system_prompt,
            backend=backend,
        )
        result = agent.invoke({"messages": [{"role": "user", "content": task_prompt}]})
        return {
            "status": "completed",
            "model": model_config.model_name,
            "method": "deep_agent",
            "tables": tables,
        }
    except Exception as exc:
        print(f"[table_profiler] Deep agent failed: {exc}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="table_profiler",
        description="Profile database tables and write MD files. Uses deep agents when available.",
    )
    parser.add_argument("--source-type", required=True, choices=list(CONNECTOR_SCRIPTS.keys()))
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--tables", required=True, help="Comma-separated table names")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--template", default="")
    parser.add_argument("--source-name", default=None)
    parser.add_argument("--model", default=None, help="Model key from env_info (e.g., deepseek_v3_1)")
    parser.add_argument("--env-info", default=str(_PROJECT_ROOT / "env_info_clean.json"))
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM, use direct connector profiling only")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    if not tables:
        print(json.dumps({"error": "No tables provided"}))
        sys.exit(1)

    # Resolve model
    model_name = "direct-profiler"
    model_config = None

    if not args.no_llm:
        try:
            router = ModelRouter(args.env_info)
            if args.model:
                model_config = router.get_model_by_key(args.model)
            else:
                model_config = router.get_cheapest_chat_model()
            model_name = model_config.model_name
            print(f"[table_profiler] Selected model: {model_name}", file=sys.stderr)
        except Exception as exc:
            print(f"[table_profiler] ModelRouter failed: {exc}; using direct profiling.", file=sys.stderr)

    # Try deep agent first (if model available and deepagents installed)
    result = None
    if model_config and not args.no_llm:
        result = _try_deep_agent_profiling(
            model_config=model_config,
            source_type=args.source_type,
            host=args.host,
            port=args.port,
            db=args.db,
            user=args.user,
            password=args.password,
            schema=args.schema,
            tables=tables,
            output_dir=args.output_dir,
            template_path=args.template,
            source_name=args.source_name,
        )

    # Fallback to direct profiling (no LLM — just connector + formatter)
    if result is None:
        print("[table_profiler] Using direct profiling (no LLM).", file=sys.stderr)
        result = profile_tables_direct(
            source_type=args.source_type,
            host=args.host,
            port=args.port,
            db=args.db,
            user=args.user,
            password=args.password,
            schema=args.schema,
            tables=tables,
            output_dir=args.output_dir,
            source_name=args.source_name,
            model_name=model_name,
        )

    print(json.dumps(result, indent=2))
    sys.exit(0 if result.get("status") == "completed" else 1)


if __name__ == "__main__":
    main()
