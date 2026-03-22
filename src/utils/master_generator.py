"""Generate the master schema overview (master_schema.md) from source summaries.

This is Layer 3 of the output pyramid.  It reads every ``_summary.md`` file
produced by :mod:`summary_generator` inside ``output/sources/*/`` and
aggregates them into a single top-level document at
``output/master_schema.md``.

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
# Size helpers (duplicated for module independence)
# ---------------------------------------------------------------------------

_SIZE_UNITS: dict[str, float] = {
    "bytes": 1,
    "kb": 1024,
    "mb": 1024 ** 2,
    "gb": 1024 ** 3,
    "tb": 1024 ** 4,
}


def _parse_size_to_bytes(size_str: str) -> float:
    """Convert a human-readable size string to bytes."""
    size_str = size_str.strip()
    match = re.match(r"([\d,.]+)\s*([a-zA-Z]+)", size_str)
    if not match:
        return 0.0
    value = float(match.group(1).replace(",", ""))
    unit = match.group(2).lower().rstrip("b") + "b"
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
    """Parse an integer tolerating commas and whitespace."""
    try:
        return int(value.strip().replace(",", ""))
    except (ValueError, TypeError):
        return 0


# ---------------------------------------------------------------------------
# Summary MD parser
# ---------------------------------------------------------------------------


def _parse_summary_md(file_path: Path) -> dict[str, Any]:
    """Parse a ``_summary.md`` file into a structured dict.

    Parameters
    ----------
    file_path:
        Absolute path to a source summary markdown file.

    Returns
    -------
    dict
        Keys: source_name, source_type, host, port, database, total_schemas,
        total_tables, total_columns, total_rows, total_size, total_size_bytes,
        no_pk_count, high_null_count, empty_table_count, no_index_count,
        wide_table_count, total_fk_count, total_profiled, re_profiled_count,
        tables (list of dicts from the All Tables section),
        relationships (list of dicts from the FK Summary section),
        quality_issues (list of dicts from the Critical Issues section).
    """
    text = file_path.read_text(encoding="utf-8")
    result: dict[str, Any] = {
        "file_path": str(file_path),
        "source_name": "",
        "source_type": "",
        "host": "",
        "port": "",
        "database": "",
        "total_schemas": 0,
        "total_tables": 0,
        "total_columns": 0,
        "total_rows": 0,
        "total_size": "0 bytes",
        "total_size_bytes": 0.0,
        "no_pk_count": 0,
        "high_null_count": 0,
        "empty_table_count": 0,
        "no_index_count": 0,
        "wide_table_count": 0,
        "total_fk_count": 0,
        "total_profiled": 0,
        "re_profiled_count": 0,
        "tables": [],
        "relationships": [],
        "quality_issues": [],
    }

    # --- Source name from header ---
    m = re.search(r"^#\s+Source Summary:\s+(.+)$", text, re.MULTILINE)
    if m:
        result["source_name"] = m.group(1).strip()

    # --- Connection Information table ---
    conn_section = re.search(
        r"## Connection Information\s*\n(.*?)(?:\n---|\n##|\Z)", text, re.DOTALL,
    )
    if conn_section:
        conn_text = conn_section.group(1)
        for row in conn_text.strip().splitlines():
            cells = [c.strip().strip("*").strip("`") for c in row.split("|") if c.strip()]
            if len(cells) >= 2:
                key = cells[0].lower()
                val = cells[1]
                if key == "source name":
                    result["source_name"] = result["source_name"] or val
                elif key == "source type":
                    result["source_type"] = val
                elif key == "host":
                    result["host"] = val
                elif key == "port":
                    result["port"] = val
                elif key == "database":
                    result["database"] = val

    # --- High-Level Statistics ---
    stats_section = re.search(
        r"## High-Level Statistics\s*\n(.*?)(?:\n---|\n##|\Z)", text, re.DOTALL,
    )
    if stats_section:
        stats_text = stats_section.group(1)
        _extract_stat = lambda label: re.search(
            rf"\*\*{re.escape(label)}\*\*\s*\|\s*([^\n|]+)", stats_text
        )
        m = _extract_stat("Total Schemas")
        if m:
            result["total_schemas"] = _safe_int(m.group(1))
        m = _extract_stat("Total Tables")
        if m:
            result["total_tables"] = _safe_int(m.group(1))
        m = _extract_stat("Total Columns")
        if m:
            result["total_columns"] = _safe_int(m.group(1))
        m = _extract_stat("Estimated Total Rows")
        if m:
            result["total_rows"] = _safe_int(m.group(1))
        m = _extract_stat("Estimated Total Size")
        if m:
            result["total_size"] = m.group(1).strip()
            result["total_size_bytes"] = _parse_size_to_bytes(result["total_size"])

    # --- All Tables ---
    tables_section = re.search(
        r"### All Tables\s*\n(.*?)(?:\n---|\n##|\n###|\Z)", text, re.DOTALL,
    )
    if tables_section:
        table_rows = re.findall(
            r"^\|\s*\d+\s*\|(.+)\|$",
            tables_section.group(1), re.MULTILINE,
        )
        for row in table_rows:
            cells = [c.strip() for c in row.split("|")]
            if len(cells) >= 7:
                result["tables"].append({
                    "schema": cells[0],
                    "table": cells[1],
                    "type": cells[2],
                    "columns": _safe_int(cells[3]),
                    "rows": _safe_int(cells[4]),
                    "size": cells[5],
                    "has_pk": cells[6].lower() in ("yes", "true"),
                    "has_fk": cells[7].lower() in ("yes", "true") if len(cells) > 7 else False,
                })

    # --- Foreign Key Summary ---
    fk_section = re.search(
        r"### Foreign Key Summary\s*\n(.*?)(?:\n\*\*Total|\n---|\n##|\n###|\Z)",
        text, re.DOTALL,
    )
    if fk_section:
        fk_rows = re.findall(
            r"^\|\s*\d+\s*\|(.+)\|$",
            fk_section.group(1), re.MULTILINE,
        )
        for row in fk_rows:
            cells = [c.strip() for c in row.split("|")]
            if len(cells) >= 5:
                result["relationships"].append({
                    "source_table": cells[0],
                    "source_column": cells[1],
                    "target_table": cells[2],
                    "target_column": cells[3],
                    "constraint": cells[4],
                })

    total_fk_m = re.search(r"\*\*Total Foreign Keys\*\*:\s*(\d+)", text)
    if total_fk_m:
        result["total_fk_count"] = int(total_fk_m.group(1))

    # --- Quality Summary ---
    quality_section = re.search(
        r"### Quality Summary\s*\n(.*?)(?:\n---|\n##|\n###|\Z)", text, re.DOTALL,
    )
    if quality_section:
        qt = quality_section.group(1)
        for label, key in [
            ("Tables with no PK", "no_pk_count"),
            ("Tables with >20% null columns", "high_null_count"),
            ("Tables with >90% null columns", "high_null_count"),
            ("Empty tables", "empty_table_count"),
            ("Tables without indexes", "no_index_count"),
            ("Wide tables", "wide_table_count"),
        ]:
            m = re.search(rf"{re.escape(label)}\s*\|\s*(\d+)", qt)
            if m:
                result[key] = int(m.group(1))

    # --- Critical Issues ---
    issues_section = re.search(
        r"### Critical Issues\s*\n(.*?)(?:\n###|\n---|\n##|\Z)", text, re.DOTALL,
    )
    if issues_section:
        issue_rows = re.findall(
            r"^\|\s*\d+\s*\|(.+)\|$",
            issues_section.group(1), re.MULTILINE,
        )
        for row in issue_rows:
            cells = [c.strip() for c in row.split("|")]
            if len(cells) >= 3:
                result["quality_issues"].append({
                    "table": cells[0],
                    "issue": cells[1],
                    "details": cells[2],
                })

    # --- Profiling Report ---
    prof_section = re.search(
        r"## Profiling Report\s*\n(.*?)(?:\n---|\n##|\Z)", text, re.DOTALL,
    )
    if prof_section:
        pt = prof_section.group(1)
        m = re.search(r"\*\*Total Tables Profiled\*\*\s*\|\s*(\d+)", pt)
        if m:
            result["total_profiled"] = int(m.group(1))
        m = re.search(r"\*\*Re-Profiled Tables\*\*\s*\|\s*(\d+)", pt)
        if m:
            result["re_profiled_count"] = int(m.group(1))

    return result


# ---------------------------------------------------------------------------
# Cross-source relationship detection
# ---------------------------------------------------------------------------


def _detect_cross_source_links(
    summaries: list[dict[str, Any]],
) -> list[dict[str, str]]:
    """Detect potential cross-source relationships based on shared column names.

    For each pair of sources, if two tables share a column name that looks like
    a key column (ends with ``_id``, or is named ``id``), it is flagged as a
    potential cross-source link.

    Parameters
    ----------
    summaries:
        List of parsed summary dicts (one per source).

    Returns
    -------
    list[dict]
        Each dict has: source_a, table_a, column_a, source_b, table_b,
        column_b, confidence, basis.
    """
    if len(summaries) < 2:
        return []

    # Build an index: column_name -> [(source, schema.table)]
    col_index: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for s in summaries:
        for t in s.get("tables", []):
            # We only have table-level info from summaries; the column names
            # themselves aren't stored in the summary.  Fall back to looking
            # at FK columns as those are the most meaningful link candidates.
            pass

    # Strategy: match tables by name across sources.  If two sources both have
    # a table named "customers" or "products", that is a potential shared entity.
    table_index: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for s in summaries:
        for t in s.get("tables", []):
            table_name = t.get("table", "").lower()
            table_index[table_name].append((
                s["source_name"],
                f"{t.get('schema', '')}.{t.get('table', '')}",
                t.get("table", ""),
            ))

    links: list[dict[str, str]] = []
    for table_name, occurrences in table_index.items():
        if len(occurrences) < 2:
            continue
        # Pair every combination
        for i in range(len(occurrences)):
            for j in range(i + 1, len(occurrences)):
                src_a, fqn_a, tbl_a = occurrences[i]
                src_b, fqn_b, tbl_b = occurrences[j]
                if src_a == src_b:
                    continue
                links.append({
                    "source_a": src_a,
                    "table_a": fqn_a,
                    "column_a": "(shared table name)",
                    "source_b": src_b,
                    "table_b": fqn_b,
                    "column_b": "(shared table name)",
                    "confidence": "MEDIUM",
                    "basis": "Same table name across sources",
                })

    return links


# ---------------------------------------------------------------------------
# Quality scoring
# ---------------------------------------------------------------------------


def _compute_quality_score(summary: dict[str, Any]) -> int:
    """Compute a simple quality score (0-100) for a source.

    Deductions:
    - 10 points per table with no PK
    - 5 points per table with high null columns
    - 3 points per empty table
    - 2 points per table without indexes
    - 1 point per wide table

    The score is clamped to [0, 100].
    """
    score = 100
    score -= summary.get("no_pk_count", 0) * 10
    score -= summary.get("high_null_count", 0) * 5
    score -= summary.get("empty_table_count", 0) * 3
    score -= summary.get("no_index_count", 0) * 2
    score -= summary.get("wide_table_count", 0) * 1
    return max(0, min(100, score))


def _score_to_grade(score: int) -> str:
    """Map a numeric score to a letter grade."""
    if score >= 90:
        return "A"
    if score >= 80:
        return "B"
    if score >= 70:
        return "C"
    if score >= 60:
        return "D"
    return "F"


# ---------------------------------------------------------------------------
# Master schema generator
# ---------------------------------------------------------------------------


def generate_master_schema(output_dir: str | Path = "output") -> str:
    """Generate ``master_schema.md`` from all source summaries.

    Reads every ``_summary.md`` found in ``{output_dir}/sources/*/``,
    aggregates cross-source statistics, detects potential cross-source
    relationships, and writes the master overview to
    ``{output_dir}/master_schema.md``.

    Parameters
    ----------
    output_dir:
        Path to the top-level output directory (default ``"output"``).

    Returns
    -------
    str
        The full Markdown content of the master schema.
    """
    output_dir = Path(output_dir)
    sources_dir = output_dir / "sources"

    if not sources_dir.exists():
        print(f"[master] WARNING: sources directory not found: {sources_dir}", file=sys.stderr)
        return ""

    summary_files = sorted(sources_dir.glob("*/_summary.md"))
    if not summary_files:
        print(f"[master] WARNING: no _summary.md files found in {sources_dir}", file=sys.stderr)
        return ""

    # -- Parse all summaries --
    print(f"[master] Parsing {len(summary_files)} source summary(ies)...", file=sys.stderr)
    summaries: list[dict[str, Any]] = []
    for f in summary_files:
        print(f"[master]   parsing {f.parent.name}/_summary.md", file=sys.stderr)
        summaries.append(_parse_summary_md(f))

    # -- Aggregates --
    total_sources = len(summaries)
    total_tables = sum(s["total_tables"] for s in summaries)
    total_columns = sum(s["total_columns"] for s in summaries)
    total_rows = sum(s["total_rows"] for s in summaries)
    total_size_bytes = sum(s["total_size_bytes"] for s in summaries)
    total_size_human = _bytes_to_human(total_size_bytes)

    # Quality scores
    for s in summaries:
        s["quality_score"] = _compute_quality_score(s)
        s["quality_grade"] = _score_to_grade(s["quality_score"])

    overall_score = (
        round(sum(s["quality_score"] for s in summaries) / total_sources)
        if total_sources > 0
        else 0
    )
    overall_grade = _score_to_grade(overall_score)

    # Cross-source links
    cross_links = _detect_cross_source_links(summaries)

    # Aggregate quality
    agg_no_pk = sum(s["no_pk_count"] for s in summaries)
    agg_high_null = sum(s["high_null_count"] for s in summaries)
    agg_empty = sum(s["empty_table_count"] for s in summaries)
    agg_no_index = sum(s["no_index_count"] for s in summaries)
    agg_wide = sum(s["wide_table_count"] for s in summaries)
    agg_quality_issues = []
    for s in summaries:
        for issue in s["quality_issues"]:
            agg_quality_issues.append({
                "source": s["source_name"],
                **issue,
            })

    # Profiling totals
    total_profiled = sum(s["total_profiled"] for s in summaries)
    total_re_profiled = sum(s["re_profiled_count"] for s in summaries)

    # File index
    file_tree_lines: list[str] = []
    for s in summaries:
        src_path = Path(s["file_path"]).parent
        file_tree_lines.append(f"    {src_path.name}/")
        file_tree_lines.append(f"      _summary.md")
        file_tree_lines.append(f"      tables/")
        tables_dir = src_path / "tables"
        if tables_dir.exists():
            for tf in sorted(tables_dir.glob("*.md")):
                file_tree_lines.append(f"        {tf.name}")

    # -- Build Markdown --
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines: list[str] = []

    lines.append("# Master Schema Overview")
    lines.append("")
    lines.append(f"**Generated**: {timestamp}")
    lines.append(f"**Sources Analyzed**: {total_sources}")
    lines.append(f"**Total Tables**: {total_tables}")
    lines.append(f"**Total Columns**: {total_columns}")
    lines.append(f"**Estimated Total Rows**: {total_rows:,}")
    lines.append(f"**Estimated Total Size**: {total_size_human}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Sources Analyzed
    lines.append("## Sources Analyzed")
    lines.append("")
    lines.append("| # | Source Name | Type | Database | Schemas | Tables | Rows | Size | Quality Score |")
    lines.append("|---|-----------|------|----------|---------|--------|------|------|---------------|")
    for i, s in enumerate(summaries, 1):
        lines.append(
            f"| {i} | {s['source_name']} | {s['source_type']} "
            f"| {s['database']} | {s['total_schemas']} | {s['total_tables']} "
            f"| {s['total_rows']:,} | {s['total_size']} "
            f"| {s['quality_score']}/100 |"
        )
    lines.append("")
    lines.append(f"**Total Sources**: {total_sources}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Cross-Source Relationships
    lines.append("## Cross-Source Relationships")
    lines.append("")
    lines.append("### Identified Cross-Source Links")
    lines.append("")
    if cross_links:
        lines.append("| # | Source A | Table A | Column A | Source B | Table B | Column B | Confidence | Basis |")
        lines.append("|---|---------|---------|----------|---------|---------|----------|------------|-------|")
        for i, link in enumerate(cross_links, 1):
            lines.append(
                f"| {i} | {link['source_a']} | {link['table_a']} | {link['column_a']} "
                f"| {link['source_b']} | {link['table_b']} | {link['column_b']} "
                f"| {link['confidence']} | {link['basis']} |"
            )
    else:
        lines.append("No cross-source relationships detected.")
        if total_sources < 2:
            lines.append("")
            lines.append("_Only one source analyzed. Cross-source detection requires multiple sources._")
    lines.append("")
    lines.append("**Confidence Levels**:")
    lines.append("- **HIGH**: Exact column name and type match with at least one side having a primary key.")
    lines.append("- **MEDIUM**: Column name match with compatible (but not identical) types.")
    lines.append("- **LOW**: Pattern-based inference (e.g., naming convention match only).")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Overall Data Quality
    lines.append("## Overall Data Quality")
    lines.append("")
    lines.append("### Quality Scores")
    lines.append("")
    lines.append("| Source | Score | Grade | Top Issue |")
    lines.append("|--------|-------|-------|-----------|")
    for s in summaries:
        # Determine top issue
        top_issue = "--"
        if s["no_pk_count"] > 0:
            top_issue = f"{s['no_pk_count']} tables missing PK"
        elif s["high_null_count"] > 0:
            top_issue = f"{s['high_null_count']} tables with high nulls"
        elif s["empty_table_count"] > 0:
            top_issue = f"{s['empty_table_count']} empty tables"
        elif s["no_index_count"] > 0:
            top_issue = f"{s['no_index_count']} tables without indexes"
        lines.append(
            f"| {s['source_name']} | {s['quality_score']}/100 "
            f"| {s['quality_grade']} | {top_issue} |"
        )
    lines.append(f"| **Overall** | **{overall_score}/100** | **{overall_grade}** | -- |")
    lines.append("")
    lines.append("**Grading Scale**: A (90-100), B (80-89), C (70-79), D (60-69), F (0-59)")
    lines.append("")

    # Quality Issue Distribution
    lines.append("### Quality Issue Distribution")
    lines.append("")
    lines.append("| Issue Category | Count | Severity |")
    lines.append("|---------------|-------|----------|")
    lines.append(f"| Missing Primary Keys | {agg_no_pk} | HIGH |")
    lines.append(f"| High Null Rate Columns | {agg_high_null} | MEDIUM |")
    lines.append(f"| Empty Tables | {agg_empty} | LOW |")
    lines.append(f"| Missing Indexes | {agg_no_index} | MEDIUM |")
    lines.append(f"| Wide Tables (>50 columns) | {agg_wide} | LOW |")
    lines.append("")

    # Tables Flagged for Review
    if agg_quality_issues:
        lines.append("### Tables Flagged for Review")
        lines.append("")
        lines.append("| # | Source | Table | Issue | Details |")
        lines.append("|---|--------|-------|-------|---------|")
        for i, issue in enumerate(agg_quality_issues, 1):
            lines.append(
                f"| {i} | {issue['source']} | {issue['table']} "
                f"| {issue['issue']} | {issue['details']} |"
            )
        lines.append("")

    lines.append("---")
    lines.append("")

    # Profiling Metadata
    lines.append("## Profiling Metadata")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| **Total Sources** | {total_sources} |")
    lines.append(f"| **Total Tables Profiled** | {total_profiled} |")
    lines.append(f"| **Tables Re-Profiled** | {total_re_profiled} |")
    lines.append(f"| **Master Generated At** | {timestamp} |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # File Index
    lines.append("## File Index")
    lines.append("")
    lines.append("All generated files for this analysis run:")
    lines.append("")
    lines.append("```")
    lines.append("output/")
    lines.append("  master_schema.md                          <- This file")
    lines.append("  sources/")
    for fl in file_tree_lines:
        lines.append(fl)
    lines.append("```")
    lines.append("")

    content = "\n".join(lines)

    # Write to disk
    output_path = output_dir / "master_schema.md"
    output_path.write_text(content, encoding="utf-8")
    print(f"[master] Wrote {output_path}", file=sys.stderr)

    return content


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    """Generate the master schema from all source summaries in output/."""
    base = Path(__file__).resolve().parents[2] / "output"
    if not base.exists():
        print(f"[master] ERROR: output/ not found at {base}", file=sys.stderr)
        sys.exit(1)

    print("[master] Generating master schema...", file=sys.stderr)
    generate_master_schema(output_dir=base)
    print("[master] Done.", file=sys.stderr)
