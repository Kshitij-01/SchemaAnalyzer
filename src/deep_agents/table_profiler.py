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


def _normalize_stats_dict(stats: dict | list | None, key_name: str = "column") -> list[dict]:
    """Convert a {col_name: {stats}} dict to [{column: col_name, ...stats}] list.

    The connector returns stats as dicts keyed by column name, but the
    MD formatter expects a list of dicts with the column name inside.
    """
    if stats is None:
        return []
    if isinstance(stats, list):
        return stats  # already a list
    if isinstance(stats, dict):
        result = []
        for col_name, col_stats in stats.items():
            if isinstance(col_stats, dict):
                entry = {key_name: col_name}
                entry.update(col_stats)
                result.append(entry)
        return result
    return []


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

    # ---- Column Statistics (deep profiling) ----
    # Normalize dict→list format from connector
    numeric_stats = _normalize_stats_dict(profile.get("numeric_stats"))
    text_stats_list = _normalize_stats_dict(profile.get("text_stats"))
    date_stats_list = _normalize_stats_dict(profile.get("date_stats"))
    boolean_stats_list = _normalize_stats_dict(profile.get("boolean_stats"))

    # Numeric columns
    if numeric_stats:
        lines.append("## Column Statistics")
        lines.append("")
        lines.append("### Numeric Columns")
        lines.append("")
        lines.append("| Column | Min | Max | Mean | Median | StdDev | P25 | P75 | Zeros | Zero% | Distinct | Cardinality |")
        lines.append("|--------|-----|-----|------|--------|--------|-----|-----|-------|-------|----------|-------------|")
        for ns in numeric_stats:
            col_name = ns.get("column", "")
            _fmt = lambda v: f"{v:.2f}" if isinstance(v, float) else str(v) if v is not None else "--"
            _pct = lambda v: f"{v:.2f}%" if isinstance(v, (int, float)) and v is not None else "--"
            lines.append(
                f"| {col_name} "
                f"| {_fmt(ns.get('min'))} "
                f"| {_fmt(ns.get('max'))} "
                f"| {_fmt(ns.get('mean'))} "
                f"| {_fmt(ns.get('median'))} "
                f"| {_fmt(ns.get('stddev'))} "
                f"| {_fmt(ns.get('p25'))} "
                f"| {_fmt(ns.get('p75'))} "
                f"| {ns.get('zeros', '--')} "
                f"| {_pct(ns.get('zero_pct'))} "
                f"| {ns.get('distinct', '--')} "
                f"| {_fmt(ns.get('cardinality'))} |"
            )
        lines.append("")

    # Text columns
    text_stats = text_stats_list
    if text_stats:
        # If numeric_stats didn't already open the section header, open it now
        if not numeric_stats:
            lines.append("## Column Statistics")
            lines.append("")
        lines.append("### Text Columns")
        lines.append("")
        lines.append("| Column | Min Len | Max Len | Avg Len | Distinct | Cardinality | Empty | Empty% |")
        lines.append("|--------|---------|---------|---------|----------|-------------|-------|--------|")
        for ts in text_stats:
            col_name = ts.get("column", "")
            _fmt = lambda v: f"{v:.2f}" if isinstance(v, float) else str(v) if v is not None else "--"
            _pct = lambda v: f"{v:.2f}%" if isinstance(v, (int, float)) and v is not None else "--"
            lines.append(
                f"| {col_name} "
                f"| {ts.get('min_len', '--')} "
                f"| {ts.get('max_len', '--')} "
                f"| {_fmt(ts.get('avg_len'))} "
                f"| {ts.get('distinct', '--')} "
                f"| {_fmt(ts.get('cardinality'))} "
                f"| {ts.get('empty', '--')} "
                f"| {_pct(ts.get('empty_pct'))} |"
            )
        lines.append("")

    # Date/Timestamp columns
    date_stats = date_stats_list
    if date_stats:
        if not numeric_stats and not text_stats:
            lines.append("## Column Statistics")
            lines.append("")
        lines.append("### Date/Timestamp Columns")
        lines.append("")
        lines.append("| Column | Earliest | Latest | Range (days) |")
        lines.append("|--------|----------|--------|-------------|")
        for ds in date_stats:
            col_name = ds.get("column", "")
            lines.append(
                f"| {col_name} "
                f"| {ds.get('earliest', '--')} "
                f"| {ds.get('latest', '--')} "
                f"| {ds.get('range_days', '--')} |"
            )
        lines.append("")

    # Boolean columns
    boolean_stats = boolean_stats_list
    if boolean_stats:
        if not numeric_stats and not text_stats and not date_stats:
            lines.append("## Column Statistics")
            lines.append("")
        lines.append("### Boolean Columns")
        lines.append("")
        lines.append("| Column | True | True% | False | False% | Null | Null% |")
        lines.append("|--------|------|-------|-------|--------|------|-------|")
        for bs in boolean_stats:
            col_name = bs.get("column", "")
            _pct = lambda v: f"{v:.2f}%" if isinstance(v, (int, float)) and v is not None else "--"
            lines.append(
                f"| {col_name} "
                f"| {bs.get('true_count', '--')} "
                f"| {_pct(bs.get('true_pct'))} "
                f"| {bs.get('false_count', '--')} "
                f"| {_pct(bs.get('false_pct'))} "
                f"| {bs.get('null_count', '--')} "
                f"| {_pct(bs.get('null_pct'))} |"
            )
        lines.append("")

    # Add separator if any column stats section was rendered
    if numeric_stats or text_stats or date_stats or boolean_stats:
        lines.append("---")
        lines.append("")

    # ---- Top Values (per text column) ----
    text_top_values = profile.get("text_top_values")
    if text_top_values:
        lines.append("## Top Values")
        lines.append("")
        for col_name, values in text_top_values.items():
            lines.append(f"### {col_name}")
            lines.append("")
            lines.append("| # | Value | Count | % |")
            lines.append("|---|-------|-------|---|")
            for i, entry in enumerate(values[:10], start=1):
                val = str(entry.get("value", ""))
                if len(val) > 50:
                    val = val[:47] + "..."
                val = val.replace("|", "\\|")
                count = entry.get("count", "--")
                pct = entry.get("pct")
                pct_str = f"{pct:.2f}%" if isinstance(pct, (int, float)) and pct is not None else "--"
                lines.append(f"| {i} | {val} | {count} | {pct_str} |")
            lines.append("")
        lines.append("---")
        lines.append("")

    # ---- Pattern Detection ----
    raw_patterns = profile.get("text_patterns")
    text_patterns = _normalize_stats_dict(raw_patterns) if raw_patterns else []
    if text_patterns:
        # Filter to columns with at least one pattern > 0%
        pattern_keys = ["email", "url", "phone", "uuid", "ipv4"]
        # Also check _pct suffixed keys from connector
        pattern_keys_pct = [f"{k}_pct" for k in pattern_keys]
        filtered = []
        for tp in text_patterns:
            all_keys = pattern_keys + pattern_keys_pct
            has_nonzero = any(
                isinstance(tp.get(pk), (int, float)) and tp.get(pk, 0) > 0
                for pk in all_keys
            )
            if has_nonzero:
                filtered.append(tp)
        if filtered:
            lines.append("## Pattern Detection")
            lines.append("")
            lines.append("| Column | Email | URL | Phone | UUID | IPv4 |")
            lines.append("|--------|-------|-----|-------|------|------|")
            for tp in filtered:
                col_name = tp.get("column", "")
                def _get_pct(d, key):
                    """Get pattern percentage, checking both 'email' and 'email_pct' keys."""
                    v = d.get(key) or d.get(f"{key}_pct")
                    return f"{v:.2f}%" if isinstance(v, (int, float)) and v is not None else "0.0%"
                lines.append(
                    f"| {col_name} "
                    f"| {_get_pct(tp, 'email')} "
                    f"| {_get_pct(tp, 'url')} "
                    f"| {_get_pct(tp, 'phone')} "
                    f"| {_get_pct(tp, 'uuid')} "
                    f"| {_get_pct(tp, 'ipv4')} |"
                )
            lines.append("")
            lines.append("---")
            lines.append("")

    # ---- Adaptive Insights ----
    adaptive_insights = profile.get("adaptive_insights")
    if adaptive_insights:
        lines.append("## Adaptive Insights")
        lines.append("")
        lines.append("| # | Column | Insight | Details | Severity |")
        lines.append("|---|--------|---------|---------|----------|")
        for i, ai in enumerate(adaptive_insights, start=1):
            col_name = ai.get("column", "--")
            insight = ai.get("insight", "--")
            details = str(ai.get("details", "--"))
            if len(details) > 50:
                details = details[:47] + "..."
            details = details.replace("|", "\\|")
            severity = ai.get("severity", "INFO")
            lines.append(f"| {i} | {col_name} | {insight} | {details} | {severity} |")
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
    run_logger: Any | None = None,
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
            if run_logger:
                run_logger.log_agent_action(
                    "Table Profiler",
                    f"Profiled {table_name}",
                    f"rows={profile.get('row_count', 'N/A')}, columns={len(profile.get('columns', []))}",
                )
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
