"""Generate a source-level summary (_summary.md) from individual table profile MDs.

This is Layer 2 of the output pyramid.  It reads every ``*.md`` file produced
by the table profiler inside ``output/sources/<source>/tables/`` and aggregates
the information into a single source summary written to
``output/sources/<source>/_summary.md``.

No LLM calls are needed -- this is purely mechanical parsing and aggregation.
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_SIZE_UNITS: dict[str, float] = {
    "bytes": 1,
    "kb": 1024,
    "mb": 1024 ** 2,
    "gb": 1024 ** 3,
    "tb": 1024 ** 4,
}


def _parse_size_to_bytes(size_str: str) -> float:
    """Convert a human-readable size string (e.g. '48 kB') to bytes."""
    size_str = size_str.strip()
    match = re.match(r"([\d,.]+)\s*([a-zA-Z]+)", size_str)
    if not match:
        return 0.0
    value = float(match.group(1).replace(",", ""))
    unit = match.group(2).lower().rstrip("b") + "b"  # normalise to e.g. "kb"
    if unit == "b":
        unit = "bytes"
    return value * _SIZE_UNITS.get(unit, 1)


def _bytes_to_human(b: float) -> str:
    """Format a byte count into a human-readable string."""
    if b < 1024:
        return f"{b:.0f} bytes"
    for unit in ("kB", "MB", "GB", "TB"):
        b /= 1024
        if b < 1024:
            return f"{b:,.2f} {unit}"
    return f"{b:,.2f} PB"


def _safe_int(value: str) -> int:
    """Parse an integer from a string, tolerating commas and whitespace."""
    try:
        return int(value.strip().replace(",", ""))
    except (ValueError, TypeError):
        return 0


def _mask_password(password: str | None) -> str:
    """Mask a password for display."""
    if not password:
        return "***"
    return "***"


# ---------------------------------------------------------------------------
# Table MD parser
# ---------------------------------------------------------------------------


def _parse_table_md(file_path: Path) -> dict[str, Any]:
    """Parse a single table profile MD file into a structured dict.

    Parameters
    ----------
    file_path:
        Absolute path to a table profile markdown file.

    Returns
    -------
    dict
        Keys: table_name, schema, source, database, table_type, column_count,
        row_count, total_size, total_size_bytes, pk_columns, fk_outgoing,
        fk_incoming, null_percentages, index_count, has_indexes, re_profiled,
        profiled_by, profiling_timestamp, columns (list of column names).
    """
    text = file_path.read_text(encoding="utf-8")
    result: dict[str, Any] = {
        "file_path": str(file_path),
        "table_name": "",
        "schema": "",
        "source": "",
        "database": "",
        "table_type": "TABLE",
        "column_count": 0,
        "columns": [],
        "row_count": 0,
        "total_size": "0 bytes",
        "total_size_bytes": 0.0,
        "pk_columns": [],
        "fk_outgoing": [],   # list of dicts: {constraint, column, ref_schema, ref_table, ref_column}
        "fk_incoming": [],   # list of dicts: {constraint, src_schema, src_table, src_column}
        "null_percentages": {},  # column_name -> float
        "index_count": 0,
        "has_indexes": False,
        "re_profiled": False,
        "profiled_by": "",
        "profiling_timestamp": "",
    }

    # --- Table name from header ---
    m = re.search(r"^#\s+Table Profile:\s+(.+)$", text, re.MULTILINE)
    if m:
        result["table_name"] = m.group(1).strip()

    # --- Property table at the top ---
    prop_block = re.search(
        r"\|\s*Property\s*\|\s*Value\s*\|.*?\n\|[-| ]+\|\n(.*?)(?:\n---|\n##|\Z)",
        text, re.DOTALL,
    )
    if prop_block:
        for row in prop_block.group(1).strip().splitlines():
            cells = [c.strip().strip("*") for c in row.split("|") if c.strip()]
            if len(cells) >= 2:
                key, val = cells[0].lower(), cells[1]
                if key == "schema":
                    result["schema"] = val
                elif key == "source":
                    result["source"] = val
                elif key == "table":
                    pass  # already captured from header
                elif key == "type":
                    result["table_type"] = val
                elif key == "database":
                    result["database"] = val

    # --- Columns section ---
    col_section = re.search(
        r"## Columns\s*\n(.*?)(?:\n---|\n##|\Z)", text, re.DOTALL,
    )
    if col_section:
        col_text = col_section.group(1)
        # Extract column names from the table rows (skip header + separator)
        col_rows = re.findall(
            r"^\|\s*\d+\s*\|([^|]+)\|", col_text, re.MULTILINE,
        )
        result["columns"] = [c.strip() for c in col_rows]
        # Also try the **Total Columns** line
        total_m = re.search(r"\*\*Total Columns\*\*:\s*(\d+)", col_text)
        if total_m:
            result["column_count"] = int(total_m.group(1))
        else:
            result["column_count"] = len(result["columns"])

    # --- Constraints: Primary Key ---
    pk_section = re.search(
        r"### Primary Key\s*\n(.*?)(?:\n###|\n---|\n##|\Z)", text, re.DOTALL,
    )
    if pk_section:
        pk_text = pk_section.group(1)
        # Parse rows of the PK table
        pk_rows = re.findall(
            r"^\|([^|]+)\|([^|]+)\|",
            pk_text, re.MULTILINE,
        )
        for constraint_name, columns in pk_rows:
            cn = constraint_name.strip().strip("-")
            cols = columns.strip().strip("-")
            if cn and cn.lower() not in ("constraint name", "none", ""):
                result["pk_columns"] = [
                    c.strip() for c in cols.split(",") if c.strip()
                ]

    # --- Foreign Keys: Outgoing ---
    fk_out_section = re.search(
        r"### Outgoing \(This Table References\)\s*\n(.*?)(?:\n###|\n---|\n##|\Z)",
        text, re.DOTALL,
    )
    if fk_out_section:
        fk_text = fk_out_section.group(1)
        fk_rows = re.findall(
            r"^\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|",
            fk_text, re.MULTILINE,
        )
        for row in fk_rows:
            parts = [c.strip().strip("-") for c in row]
            if parts[0].lower() in ("constraint name", "") or parts[0].lower() == "none":
                continue
            result["fk_outgoing"].append({
                "constraint": parts[0],
                "column": parts[1],
                "ref_schema": parts[2],
                "ref_table": parts[3],
                "ref_column": parts[4],
            })

    # --- Foreign Keys: Incoming ---
    fk_in_section = re.search(
        r"### Incoming \(Referenced By\)\s*\n(.*?)(?:\n###|\n---|\n##|\Z)",
        text, re.DOTALL,
    )
    if fk_in_section:
        fk_text = fk_in_section.group(1)
        fk_rows = re.findall(
            r"^\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|",
            fk_text, re.MULTILINE,
        )
        for row in fk_rows:
            parts = [c.strip().strip("-") for c in row]
            if parts[0].lower() in ("constraint name", "") or parts[0].lower() == "none":
                continue
            result["fk_incoming"].append({
                "constraint": parts[0],
                "src_schema": parts[1],
                "src_table": parts[2],
                "src_column": parts[3],
            })

    # --- Statistics ---
    stats_section = re.search(
        r"## Statistics\s*\n(.*?)(?:\n---|\n##|\Z)", text, re.DOTALL,
    )
    if stats_section:
        stats_text = stats_section.group(1)
        row_m = re.search(r"\*\*Row Count\*\*\s*\|\s*([^\n|]+)", stats_text)
        if row_m:
            result["row_count"] = _safe_int(row_m.group(1))
        size_m = re.search(r"\*\*Total Size\*\*\s*\|\s*([^\n|]+)", stats_text)
        if size_m:
            result["total_size"] = size_m.group(1).strip()
            result["total_size_bytes"] = _parse_size_to_bytes(result["total_size"])

    # --- Null Percentages ---
    null_section = re.search(
        r"### Null Percentages\s*\n(.*?)(?:\n---|\n##|\Z)", text, re.DOTALL,
    )
    if null_section:
        null_text = null_section.group(1)
        null_rows = re.findall(
            r"^\|([^|]+)\|([^|]+)\|",
            null_text, re.MULTILINE,
        )
        for col_name, pct_str in null_rows:
            cn = col_name.strip().strip("-")
            ps = pct_str.strip().strip("-").replace("%", "")
            if cn.lower() in ("column name", ""):
                continue
            try:
                result["null_percentages"][cn] = float(ps)
            except ValueError:
                pass

    # --- Indexes ---
    idx_section = re.search(
        r"## Indexes\s*\n(.*?)(?:\n---|\n##|\Z)", text, re.DOTALL,
    )
    if idx_section:
        idx_text = idx_section.group(1)
        total_idx_m = re.search(r"\*\*Total Indexes\*\*:\s*(\d+)", idx_text)
        if total_idx_m:
            result["index_count"] = int(total_idx_m.group(1))
            result["has_indexes"] = result["index_count"] > 0

    # --- Profiling Metadata ---
    prof_section = re.search(
        r"## Profiling Metadata\s*\n(.*?)(?:\n---|\n##|\Z)", text, re.DOTALL,
    )
    if prof_section:
        prof_text = prof_section.group(1)
        profiled_m = re.search(r"\*\*Profiled By\*\*\s*\|\s*([^\n|]+)", prof_text)
        if profiled_m:
            result["profiled_by"] = profiled_m.group(1).strip()
        ts_m = re.search(r"\*\*Timestamp\*\*\s*\|\s*([^\n|]+)", prof_text)
        if ts_m:
            result["profiling_timestamp"] = ts_m.group(1).strip()
        re_m = re.search(r"\*\*Re-Profiled\*\*\s*\|\s*([^\n|]+)", prof_text)
        if re_m:
            result["re_profiled"] = re_m.group(1).strip().lower() == "true"

    return result


# ---------------------------------------------------------------------------
# Summary generator
# ---------------------------------------------------------------------------


def generate_source_summary(
    source_dir: str | Path,
    source_name: str,
    source_type: str,
    connection_info: dict[str, Any] | None = None,
) -> str:
    """Generate a ``_summary.md`` for one data source.

    Reads every ``*.md`` file inside ``{source_dir}/tables/``, parses them
    with :func:`_parse_table_md`, and aggregates the results into a
    Markdown summary that is both returned as a string and written to
    ``{source_dir}/_summary.md``.

    Parameters
    ----------
    source_dir:
        Path to the source directory (e.g. ``output/sources/my_source``).
    source_name:
        Human-readable name for the source.
    source_type:
        Database type (e.g. ``postgres``, ``snowflake``).
    connection_info:
        Optional dict with keys like host, port, database, user, password.
        Password will be masked in the output.

    Returns
    -------
    str
        The full Markdown content of the summary.
    """
    source_dir = Path(source_dir)
    tables_dir = source_dir / "tables"

    if not tables_dir.exists():
        print(f"[summary] WARNING: tables directory not found: {tables_dir}", file=sys.stderr)
        return ""

    md_files = sorted(tables_dir.glob("*.md"))
    if not md_files:
        print(f"[summary] WARNING: no .md files in {tables_dir}", file=sys.stderr)
        return ""

    # -- Parse all table MDs --
    print(f"[summary] Parsing {len(md_files)} table profile(s) for '{source_name}'...", file=sys.stderr)
    tables: list[dict[str, Any]] = []
    for f in md_files:
        print(f"[summary]   parsing {f.name}", file=sys.stderr)
        tables.append(_parse_table_md(f))

    # -- Aggregate stats --
    total_tables = len(tables)
    total_columns = sum(t["column_count"] for t in tables)
    total_rows = sum(t["row_count"] for t in tables)
    total_size_bytes = sum(t["total_size_bytes"] for t in tables)
    total_size_human = _bytes_to_human(total_size_bytes)

    # Schemas
    schemas: dict[str, list[dict]] = defaultdict(list)
    for t in tables:
        schemas[t["schema"] or "default"].append(t)

    # Relationships (all FK outgoing across all tables)
    all_relationships: list[dict[str, str]] = []
    for t in tables:
        for fk in t["fk_outgoing"]:
            all_relationships.append({
                "constraint": fk["constraint"],
                "source_table": t["table_name"],
                "source_column": fk["column"],
                "target_table": f"{fk['ref_schema']}.{fk['ref_table']}",
                "target_column": fk["ref_column"],
            })

    # Data quality flags
    quality_issues: list[dict[str, str]] = []
    no_pk_tables: list[str] = []
    high_null_tables: list[str] = []
    empty_tables: list[str] = []
    no_index_tables: list[str] = []
    wide_tables: list[str] = []

    for t in tables:
        tname = t["table_name"]

        # NO_PRIMARY_KEY
        if not t["pk_columns"]:
            quality_issues.append({
                "table": tname,
                "issue": "NO_PRIMARY_KEY",
                "details": "Table has no primary key defined.",
            })
            no_pk_tables.append(tname)

        # HIGH_NULL_RATE (>20% as requested by user; template uses 90% but user said 20%)
        for col, pct in t["null_percentages"].items():
            if pct > 20.0:
                quality_issues.append({
                    "table": tname,
                    "issue": "HIGH_NULL_RATE",
                    "details": f"Column `{col}` has {pct:.2f}% nulls.",
                })
                if tname not in high_null_tables:
                    high_null_tables.append(tname)

        # EMPTY_TABLE
        if t["row_count"] == 0:
            quality_issues.append({
                "table": tname,
                "issue": "EMPTY_TABLE",
                "details": "Table has 0 rows.",
            })
            empty_tables.append(tname)

        # MISSING_INDEX (no indexes at all beyond the PK)
        if not t["has_indexes"]:
            quality_issues.append({
                "table": tname,
                "issue": "MISSING_INDEX",
                "details": "Table has no indexes.",
            })
            no_index_tables.append(tname)

        # WIDE_TABLE
        if t["column_count"] > 50:
            quality_issues.append({
                "table": tname,
                "issue": "WIDE_TABLE",
                "details": f"Table has {t['column_count']} columns (>50).",
            })
            wide_tables.append(tname)

    # Profiling stats
    total_profiled = total_tables
    re_profiled_tables = [t for t in tables if t["re_profiled"]]
    re_profiled_count = len(re_profiled_tables)

    # Relationship clusters: group tables that share FK links
    adjacency: dict[str, set[str]] = defaultdict(set)
    for rel in all_relationships:
        adjacency[rel["source_table"]].add(rel["target_table"])
        adjacency[rel["target_table"]].add(rel["source_table"])

    # Simple connected-component clustering
    visited: set[str] = set()
    clusters: list[set[str]] = []
    for node in adjacency:
        if node in visited:
            continue
        cluster: set[str] = set()
        stack = [node]
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            cluster.add(current)
            stack.extend(adjacency[current] - visited)
        if len(cluster) > 1:
            clusters.append(cluster)

    # -- Build Markdown --
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn = connection_info or {}
    lines: list[str] = []

    lines.append(f"# Source Summary: {source_name}")
    lines.append("")
    lines.append(f"**Generated**: {timestamp}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Connection Information
    lines.append("## Connection Information")
    lines.append("")
    lines.append("| Property | Value |")
    lines.append("|----------|-------|")
    lines.append(f"| **Source Name** | {source_name} |")
    lines.append(f"| **Source Type** | {source_type} |")
    lines.append(f"| **Host** | {conn.get('host', 'N/A')} |")
    lines.append(f"| **Port** | {conn.get('port', 'N/A')} |")
    lines.append(f"| **Database** | {conn.get('database', 'N/A')} |")
    lines.append(f"| **User** | {conn.get('user', 'N/A')} |")
    lines.append(f"| **Password** | `{_mask_password(conn.get('password'))}` |")
    lines.append(f"| **Connection Status** | {conn.get('status', 'Profiled')} |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # High-Level Statistics
    lines.append("## High-Level Statistics")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| **Total Schemas** | {len(schemas)} |")
    lines.append(f"| **Total Tables** | {total_tables} |")
    lines.append(f"| **Total Columns** | {total_columns} |")
    lines.append(f"| **Estimated Total Rows** | {total_rows:,} |")
    lines.append(f"| **Estimated Total Size** | {total_size_human} |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Schema Overview
    lines.append("## Schema Overview")
    lines.append("")

    # Schemas sub-table
    lines.append("### Schemas")
    lines.append("")
    lines.append("| Schema | Tables | Total Objects |")
    lines.append("|--------|--------|---------------|")
    for schema_name, schema_tables in sorted(schemas.items()):
        lines.append(f"| {schema_name} | {len(schema_tables)} | {len(schema_tables)} |")
    lines.append("")

    # All Tables sub-table
    lines.append("### All Tables")
    lines.append("")
    lines.append("| # | Schema | Table | Type | Columns | Row Count | Size | Has PK | Has FK |")
    lines.append("|---|--------|-------|------|---------|-----------|------|--------|--------|")
    for i, t in enumerate(sorted(tables, key=lambda x: x["table_name"]), 1):
        has_pk = "Yes" if t["pk_columns"] else "No"
        has_fk = "Yes" if t["fk_outgoing"] else "No"
        lines.append(
            f"| {i} | {t['schema']} | {t['table_name'].split('.')[-1]} "
            f"| {t['table_type']} | {t['column_count']} | {t['row_count']:,} "
            f"| {t['total_size']} | {has_pk} | {has_fk} |"
        )
    lines.append("")
    lines.append("---")
    lines.append("")

    # Key Relationships
    lines.append("## Key Relationships")
    lines.append("")
    lines.append("### Foreign Key Summary")
    lines.append("")
    if all_relationships:
        lines.append("| # | Source Table | Source Column | Target Table | Target Column | Constraint |")
        lines.append("|---|-------------|---------------|-------------|---------------|------------|")
        for i, rel in enumerate(all_relationships, 1):
            lines.append(
                f"| {i} | {rel['source_table']} | {rel['source_column']} "
                f"| {rel['target_table']} | {rel['target_column']} "
                f"| {rel['constraint']} |"
            )
    else:
        lines.append("No foreign key relationships found in this source.")
    lines.append("")
    lines.append(f"**Total Foreign Keys**: {len(all_relationships)}")
    lines.append("")

    # Relationship Clusters
    lines.append("### Relationship Clusters")
    lines.append("")
    lines.append("Tables that are heavily interconnected within this source:")
    lines.append("")
    if clusters:
        lines.append("| Cluster | Tables | Relationship Count |")
        lines.append("|---------|--------|--------------------|")
        for i, cluster in enumerate(clusters, 1):
            # Count edges within this cluster
            edge_count = sum(
                1 for rel in all_relationships
                if rel["source_table"] in cluster and rel["target_table"] in cluster
            )
            table_list = ", ".join(sorted(cluster))
            lines.append(f"| Cluster {i} | {table_list} | {edge_count} |")
    else:
        lines.append("No relationship clusters detected.")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Data Quality Flags
    lines.append("## Data Quality Flags")
    lines.append("")
    lines.append("### Critical Issues")
    lines.append("")
    if quality_issues:
        lines.append("| # | Table | Issue | Details |")
        lines.append("|---|-------|-------|---------|")
        for i, issue in enumerate(quality_issues, 1):
            lines.append(
                f"| {i} | {issue['table']} | {issue['issue']} | {issue['details']} |"
            )
    else:
        lines.append("No critical data quality issues detected.")
    lines.append("")

    lines.append("### Quality Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Tables with no PK | {len(no_pk_tables)} |")
    lines.append(f"| Tables with >20% null columns | {len(high_null_tables)} |")
    lines.append(f"| Empty tables | {len(empty_tables)} |")
    lines.append(f"| Tables without indexes | {len(no_index_tables)} |")
    lines.append(f"| Wide tables (>50 columns) | {len(wide_tables)} |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Profiling Report
    lines.append("## Profiling Report")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| **Total Tables Profiled** | {total_profiled} |")
    lines.append(f"| **Re-Profiled Tables** | {re_profiled_count} |")
    lines.append("")

    if re_profiled_tables:
        lines.append("### Re-Profiled Tables")
        lines.append("")
        lines.append("| # | Table | Re-Profiled |")
        lines.append("|---|-------|-------------|")
        for i, t in enumerate(re_profiled_tables, 1):
            lines.append(f"| {i} | {t['table_name']} | Yes |")
        lines.append("")

    lines.append("---")
    lines.append("")

    # Notes
    lines.append("## Notes")
    lines.append("")
    lines.append(f"Summary generated from {total_profiled} table profile(s) in `{tables_dir.as_posix()}`.")
    lines.append("")

    content = "\n".join(lines)

    # Write to disk
    output_path = source_dir / "_summary.md"
    output_path.write_text(content, encoding="utf-8")
    print(f"[summary] Wrote {output_path}", file=sys.stderr)

    return content


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    """Generate summaries for all sources found under output/sources/."""
    base = Path(__file__).resolve().parents[2] / "output" / "sources"
    if not base.exists():
        print(f"[summary] ERROR: output/sources/ not found at {base}", file=sys.stderr)
        sys.exit(1)

    source_dirs = sorted(
        d for d in base.iterdir() if d.is_dir() and (d / "tables").exists()
    )
    if not source_dirs:
        print("[summary] No source directories with tables/ found.", file=sys.stderr)
        sys.exit(1)

    for src_dir in source_dirs:
        src_name = src_dir.name
        print(f"\n[summary] === Processing source: {src_name} ===", file=sys.stderr)
        generate_source_summary(
            source_dir=src_dir,
            source_name=src_name,
            source_type="unknown",
            connection_info={},
        )

    print("\n[summary] Done.", file=sys.stderr)
