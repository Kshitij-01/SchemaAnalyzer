"""Comprehensive data quality audit across all profiled sources.

Reads every table profile MD under ``output/sources/*/tables/*.md``,
evaluates each table against multiple quality dimensions (Completeness,
Integrity, Structure, Consistency), scores them, and writes two reports:

- ``output/analysis/quality_audit.md``        -- full audit report
- ``output/context/feedback/quality_scores.md`` -- simple score table

Pure Python, no LLM calls required.
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Severity levels
# ---------------------------------------------------------------------------

SEVERITY_CRITICAL = "CRITICAL"
SEVERITY_HIGH = "HIGH"
SEVERITY_MEDIUM = "MEDIUM"
SEVERITY_LOW = "LOW"

SEVERITY_PENALTY: dict[str, int] = {
    SEVERITY_CRITICAL: 25,
    SEVERITY_HIGH: 15,
    SEVERITY_MEDIUM: 8,
    SEVERITY_LOW: 3,
}

SEVERITY_ORDER: dict[str, int] = {
    SEVERITY_CRITICAL: 0,
    SEVERITY_HIGH: 1,
    SEVERITY_MEDIUM: 2,
    SEVERITY_LOW: 3,
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class Issue:
    """A single quality issue found on a table."""

    __slots__ = ("dimension", "severity", "table_name", "source", "description")

    def __init__(
        self,
        dimension: str,
        severity: str,
        table_name: str,
        source: str,
        description: str,
    ) -> None:
        self.dimension = dimension
        self.severity = severity
        self.table_name = table_name
        self.source = source
        self.description = description

    def sort_key(self) -> tuple[int, str, str]:
        return (SEVERITY_ORDER.get(self.severity, 9), self.source, self.table_name)


# ---------------------------------------------------------------------------
# Helpers (reused / adapted from summary_generator)
# ---------------------------------------------------------------------------

_SIZE_UNITS: dict[str, float] = {
    "bytes": 1,
    "kb": 1024,
    "mb": 1024 ** 2,
    "gb": 1024 ** 3,
    "tb": 1024 ** 4,
}


def _parse_size_to_bytes(size_str: str) -> float:
    size_str = size_str.strip()
    match = re.match(r"([\d,.]+)\s*([a-zA-Z]+)", size_str)
    if not match:
        return 0.0
    value = float(match.group(1).replace(",", ""))
    unit = match.group(2).lower().rstrip("b") + "b"
    if unit == "b":
        unit = "bytes"
    return value * _SIZE_UNITS.get(unit, 1)


def _safe_int(value: str) -> int:
    try:
        return int(value.strip().replace(",", ""))
    except (ValueError, TypeError):
        return 0


# ---------------------------------------------------------------------------
# Table MD parser (extended from summary_generator._parse_table_md)
# ---------------------------------------------------------------------------

def _parse_table_md(file_path: Path) -> dict[str, Any]:
    """Parse a single table profile MD into a structured dict.

    Extends the summary_generator parser to also capture column data types,
    unique constraints, check constraints, and per-index primary flag for
    quality auditing purposes.
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
        "columns": [],            # list of column name strings
        "column_types": {},       # column_name -> data_type
        "row_count": 0,
        "total_size": "0 bytes",
        "total_size_bytes": 0.0,
        "pk_columns": [],
        "has_unique_constraints": False,
        "has_check_constraints": False,
        "fk_outgoing": [],
        "fk_incoming": [],
        "null_percentages": {},   # column_name -> float
        "index_count": 0,
        "non_pk_index_count": 0,
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
                elif key == "type":
                    result["table_type"] = val
                elif key == "database":
                    result["database"] = val

    # --- Columns section (with data types) ---
    col_section = re.search(
        r"## Columns\s*\n(.*?)(?:\n---|\n##|\Z)", text, re.DOTALL,
    )
    if col_section:
        col_text = col_section.group(1)
        # Each column row: | # | Column Name | Data Type | ...
        col_rows = re.findall(
            r"^\|\s*\d+\s*\|([^|]+)\|([^|]+)\|",
            col_text, re.MULTILINE,
        )
        for col_name_raw, dtype_raw in col_rows:
            col_name = col_name_raw.strip()
            dtype = dtype_raw.strip()
            result["columns"].append(col_name)
            result["column_types"][col_name] = dtype
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
        pk_rows = re.findall(r"^\|([^|]+)\|([^|]+)\|", pk_text, re.MULTILINE)
        for constraint_name, columns in pk_rows:
            cn = constraint_name.strip().strip("-")
            cols = columns.strip().strip("-")
            if cn and cn.lower() not in ("constraint name", "none", ""):
                result["pk_columns"] = [
                    c.strip() for c in cols.split(",") if c.strip()
                ]

    # --- Constraints: Unique ---
    uq_section = re.search(
        r"### Unique Constraints\s*\n(.*?)(?:\n###|\n---|\n##|\Z)", text, re.DOTALL,
    )
    if uq_section:
        uq_text = uq_section.group(1)
        uq_rows = re.findall(r"^\|([^|]+)\|([^|]+)\|", uq_text, re.MULTILINE)
        for constraint_name, _ in uq_rows:
            cn = constraint_name.strip().strip("-")
            if cn and cn.lower() not in ("constraint name", "none", ""):
                result["has_unique_constraints"] = True
                break

    # --- Constraints: Check ---
    ck_section = re.search(
        r"### Check Constraints\s*\n(.*?)(?:\n###|\n---|\n##|\Z)", text, re.DOTALL,
    )
    if ck_section:
        ck_text = ck_section.group(1)
        ck_rows = re.findall(r"^\|([^|]+)\|([^|]+)\|", ck_text, re.MULTILINE)
        for constraint_name, _ in ck_rows:
            cn = constraint_name.strip().strip("-")
            if cn and cn.lower() not in ("constraint name", "none", ""):
                result["has_check_constraints"] = True
                break

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
            r"^\|([^|]+)\|([^|]+)\|", null_text, re.MULTILINE,
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

        # Count non-PK indexes by looking at the Primary column
        idx_rows = re.findall(
            r"^\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|",
            idx_text, re.MULTILINE,
        )
        non_pk_count = 0
        for row in idx_rows:
            parts = [c.strip() for c in row]
            # parts: [name, definition, unique, primary, type]
            if parts[0].lower() in ("index name", "") or parts[0].strip("-") == "":
                continue
            if parts[3].strip().upper() == "NO":
                non_pk_count += 1
        result["non_pk_index_count"] = non_pk_count

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
# Quality evaluation
# ---------------------------------------------------------------------------

def _evaluate_completeness(table: dict[str, Any], source: str) -> list[Issue]:
    """Evaluate completeness dimension for a single table."""
    issues: list[Issue] = []
    tname = table["table_name"]

    # Null percentage thresholds
    for col, pct in table["null_percentages"].items():
        if pct > 90.0:
            issues.append(Issue(
                "Completeness", SEVERITY_CRITICAL, tname, source,
                f"Column `{col}` has {pct:.1f}% null values (>90%)",
            ))
        elif pct > 50.0:
            issues.append(Issue(
                "Completeness", SEVERITY_HIGH, tname, source,
                f"Column `{col}` has {pct:.1f}% null values (>50%)",
            ))
        elif pct > 20.0:
            issues.append(Issue(
                "Completeness", SEVERITY_MEDIUM, tname, source,
                f"Column `{col}` has {pct:.1f}% null values (>20%)",
            ))

    # Empty / stale tables
    if table["row_count"] == 0:
        issues.append(Issue(
            "Completeness", SEVERITY_LOW, tname, source,
            "Table has 0 rows (empty or stale)",
        ))

    return issues


def _evaluate_integrity(
    table: dict[str, Any],
    source: str,
    all_table_names_in_source: set[str],
) -> list[Issue]:
    """Evaluate integrity dimension for a single table."""
    issues: list[Issue] = []
    tname = table["table_name"]

    # No primary key
    if not table["pk_columns"]:
        issues.append(Issue(
            "Integrity", SEVERITY_HIGH, tname, source,
            "Table has no primary key defined",
        ))

    # FK referencing non-existent tables within the same source
    for fk in table["fk_outgoing"]:
        ref_full = f"{fk['ref_schema']}.{fk['ref_table']}"
        if ref_full not in all_table_names_in_source:
            issues.append(Issue(
                "Integrity", SEVERITY_CRITICAL, tname, source,
                f"Foreign key `{fk['constraint']}` references `{ref_full}` "
                f"which was not found among profiled tables",
            ))

    # No constraints at all
    has_any_constraint = bool(
        table["pk_columns"]
        or table["has_unique_constraints"]
        or table["has_check_constraints"]
        or table["fk_outgoing"]
    )
    if not has_any_constraint:
        issues.append(Issue(
            "Integrity", SEVERITY_MEDIUM, tname, source,
            "Table has no constraints defined (no PK, unique, check, or FK)",
        ))

    return issues


def _evaluate_structure(table: dict[str, Any], source: str) -> list[Issue]:
    """Evaluate structure dimension for a single table."""
    issues: list[Issue] = []
    tname = table["table_name"]

    # No indexes besides PK
    if table["non_pk_index_count"] == 0:
        issues.append(Issue(
            "Structure", SEVERITY_MEDIUM, tname, source,
            "Table has no indexes besides the primary key index",
        ))

    # Wide table
    if table["column_count"] > 30:
        issues.append(Issue(
            "Structure", SEVERITY_LOW, tname, source,
            f"Table has {table['column_count']} columns (>30) -- may need normalization",
        ))

    return issues


def _evaluate_consistency(
    tables_by_source: dict[str, list[dict[str, Any]]],
) -> list[Issue]:
    """Evaluate cross-table consistency dimensions."""
    issues: list[Issue] = []

    # --- Within each source: same column name, different types ---
    for source, tables in tables_by_source.items():
        # column_name -> {data_type -> [table_names]}
        col_type_map: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
        for t in tables:
            for col, dtype in t["column_types"].items():
                col_type_map[col][dtype].append(t["table_name"])

        for col, types_dict in col_type_map.items():
            if len(types_dict) > 1:
                parts: list[str] = []
                for dtype, tbl_list in sorted(types_dict.items()):
                    parts.append(f"`{dtype}` in {', '.join(tbl_list)}")
                issues.append(Issue(
                    "Consistency", SEVERITY_HIGH, "(cross-table)", source,
                    f"Column `{col}` has different types across tables: "
                    + "; ".join(parts),
                ))

    # --- Across sources: same table name, different column counts ---
    # Collect base table name (schema.table) -> {source: column_count}
    table_col_counts: dict[str, dict[str, int]] = defaultdict(dict)
    for source, tables in tables_by_source.items():
        for t in tables:
            base_name = t["table_name"].split(".")[-1] if "." in t["table_name"] else t["table_name"]
            table_col_counts[base_name][source] = t["column_count"]

    for base_name, source_counts in table_col_counts.items():
        if len(source_counts) < 2:
            continue
        counts = list(source_counts.values())
        if len(set(counts)) > 1:
            detail_parts = [f"{src}: {cnt} cols" for src, cnt in sorted(source_counts.items())]
            issues.append(Issue(
                "Consistency", SEVERITY_MEDIUM, base_name, "(cross-source)",
                f"Table `{base_name}` appears in multiple sources with "
                f"different column counts: {', '.join(detail_parts)}",
            ))

    return issues


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _score_grade(score: float) -> str:
    """Map a numeric score (0-100) to a letter grade."""
    if score >= 90:
        return "A"
    if score >= 80:
        return "B"
    if score >= 70:
        return "C"
    if score >= 60:
        return "D"
    return "F"


def _compute_table_score(issues: list[Issue]) -> int:
    """Compute quality score for a single table (0-100)."""
    score = 100
    for issue in issues:
        score -= SEVERITY_PENALTY.get(issue.severity, 0)
    return max(score, 0)


def _weighted_average(scores_weights: list[tuple[float, float]]) -> float:
    """Weighted average; falls back to simple average if total weight is 0."""
    total_weight = sum(w for _, w in scores_weights)
    if total_weight == 0:
        if not scores_weights:
            return 0.0
        return sum(s for s, _ in scores_weights) / len(scores_weights)
    return sum(s * w for s, w in scores_weights) / total_weight


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def _build_full_report(
    tables: list[dict[str, Any]],
    issues_by_table: dict[str, list[Issue]],
    table_scores: dict[str, int],
    source_scores: dict[str, float],
    overall_score: float,
    all_issues: list[Issue],
    tables_by_source: dict[str, list[dict[str, Any]]],
) -> str:
    """Build the full quality_audit.md content."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    overall_grade = _score_grade(overall_score)
    total_tables = len(tables)

    # Count issues by severity
    sev_counts: dict[str, int] = {
        SEVERITY_CRITICAL: 0,
        SEVERITY_HIGH: 0,
        SEVERITY_MEDIUM: 0,
        SEVERITY_LOW: 0,
    }
    for issue in all_issues:
        sev_counts[issue.severity] = sev_counts.get(issue.severity, 0) + 1
    total_issues = sum(sev_counts.values())

    lines: list[str] = []

    # ---- Executive Summary ----
    lines.append("# Data Quality Audit Report")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| **Overall Score** | {overall_score:.1f} / 100 |")
    lines.append(f"| **Grade** | {overall_grade} |")
    lines.append(f"| **Sources Analyzed** | {len(tables_by_source)} |")
    lines.append(f"| **Tables Analyzed** | {total_tables} |")
    lines.append(f"| **Total Issues** | {total_issues} |")
    lines.append(f"| **Critical** | {sev_counts[SEVERITY_CRITICAL]} |")
    lines.append(f"| **High** | {sev_counts[SEVERITY_HIGH]} |")
    lines.append(f"| **Medium** | {sev_counts[SEVERITY_MEDIUM]} |")
    lines.append(f"| **Low** | {sev_counts[SEVERITY_LOW]} |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ---- Scoring Methodology ----
    lines.append("## Scoring Methodology")
    lines.append("")
    lines.append("Each table starts at **100 points**. Deductions are applied per issue:")
    lines.append("")
    lines.append("| Severity | Deduction |")
    lines.append("|----------|-----------|")
    lines.append("| CRITICAL | -25 |")
    lines.append("| HIGH | -15 |")
    lines.append("| MEDIUM | -8 |")
    lines.append("| LOW | -3 |")
    lines.append("")
    lines.append("Minimum score is **0**. Per-source scores are weighted averages of table "
                 "scores (by row count). The overall score is the weighted average across sources.")
    lines.append("")
    lines.append("**Grading scale:** A: 90+, B: 80-89, C: 70-79, D: 60-69, F: <60")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ---- Per-Source Quality Summary ----
    lines.append("## Per-Source Quality Summary")
    lines.append("")
    lines.append("| Source | Tables | Score | Grade | Top Issues |")
    lines.append("|--------|--------|-------|-------|------------|")

    for source in sorted(tables_by_source.keys()):
        src_tables = tables_by_source[source]
        src_score = source_scores.get(source, 0.0)
        src_grade = _score_grade(src_score)
        # Collect top issues for this source
        src_issues = [i for i in all_issues if i.source == source]
        top = []
        for sev in (SEVERITY_CRITICAL, SEVERITY_HIGH, SEVERITY_MEDIUM, SEVERITY_LOW):
            cnt = sum(1 for i in src_issues if i.severity == sev)
            if cnt > 0:
                top.append(f"{cnt} {sev}")
        top_str = ", ".join(top) if top else "None"
        lines.append(f"| {source} | {len(src_tables)} | {src_score:.1f} | {src_grade} | {top_str} |")

    lines.append("")
    lines.append("---")
    lines.append("")

    # ---- Top 20 Worst Tables ----
    lines.append("## Top 20 Worst Tables")
    lines.append("")
    # Sort tables by score ascending (worst first), then by name
    scored_list: list[tuple[str, str, int, int]] = []
    for t in tables:
        key = f"{t['source']}::{t['table_name']}"
        score = table_scores.get(key, 100)
        issue_count = len(issues_by_table.get(key, []))
        scored_list.append((t["source"], t["table_name"], score, issue_count))
    scored_list.sort(key=lambda x: (x[2], x[0], x[1]))

    lines.append("| Rank | Source | Table | Score | Grade | Issues |")
    lines.append("|------|--------|-------|-------|-------|--------|")
    for rank, (src, tname, score, issue_count) in enumerate(scored_list[:20], 1):
        grade = _score_grade(score)
        lines.append(f"| {rank} | {src} | {tname} | {score} | {grade} | {issue_count} |")

    lines.append("")
    lines.append("---")
    lines.append("")

    # ---- Issue Breakdown by Dimension ----
    lines.append("## Issue Breakdown by Dimension")
    lines.append("")

    for dimension in ("Completeness", "Integrity", "Structure", "Consistency"):
        dim_issues = sorted(
            [i for i in all_issues if i.dimension == dimension],
            key=lambda i: i.sort_key(),
        )
        lines.append(f"### {dimension}")
        lines.append("")
        if dim_issues:
            lines.append("| Severity | Source | Table | Description |")
            lines.append("|----------|--------|-------|-------------|")
            for issue in dim_issues:
                lines.append(
                    f"| {issue.severity} | {issue.source} | {issue.table_name} "
                    f"| {issue.description} |"
                )
        else:
            lines.append("No issues found in this dimension.")
        lines.append("")

    lines.append("---")
    lines.append("")

    # ---- Detailed Findings per Source ----
    lines.append("## Detailed Findings per Source")
    lines.append("")

    for source in sorted(tables_by_source.keys()):
        src_tables = tables_by_source[source]
        src_score = source_scores.get(source, 0.0)
        src_grade = _score_grade(src_score)

        lines.append(f"### {source}")
        lines.append("")
        lines.append(f"**Source Score:** {src_score:.1f} / 100 ({src_grade})")
        lines.append("")
        lines.append("| Table | Score | Grade | Issues |")
        lines.append("|-------|-------|-------|--------|")

        for t in sorted(src_tables, key=lambda x: x["table_name"]):
            key = f"{source}::{t['table_name']}"
            score = table_scores.get(key, 100)
            grade = _score_grade(score)
            tbl_issues = issues_by_table.get(key, [])
            if tbl_issues:
                summaries = []
                for sev in (SEVERITY_CRITICAL, SEVERITY_HIGH, SEVERITY_MEDIUM, SEVERITY_LOW):
                    cnt = sum(1 for i in tbl_issues if i.severity == sev)
                    if cnt > 0:
                        summaries.append(f"{cnt} {sev}")
                issue_str = ", ".join(summaries)
            else:
                issue_str = "None"
            lines.append(f"| {t['table_name']} | {score} | {grade} | {issue_str} |")

        # List individual issues for this source
        src_issues = sorted(
            [i for i in all_issues if i.source == source],
            key=lambda i: i.sort_key(),
        )
        if src_issues:
            lines.append("")
            lines.append("**Issues:**")
            lines.append("")
            for issue in src_issues:
                lines.append(
                    f"- **[{issue.severity}]** `{issue.table_name}` "
                    f"({issue.dimension}): {issue.description}"
                )

        lines.append("")
        lines.append("---")
        lines.append("")

    # ---- Analysis Metadata ----
    lines.append("## Analysis Metadata")
    lines.append("")
    lines.append("| Property | Value |")
    lines.append("|----------|-------|")
    lines.append(f"| **Timestamp** | {timestamp} |")
    lines.append(f"| **Tables Analyzed** | {total_tables} |")
    lines.append(f"| **Sources Analyzed** | {len(tables_by_source)} |")
    lines.append(f"| **Total Issues Found** | {total_issues} |")
    lines.append(f"| **Overall Score** | {overall_score:.1f} |")
    lines.append(f"| **Overall Grade** | {overall_grade} |")
    lines.append("")

    return "\n".join(lines)


def _build_scores_report(
    tables: list[dict[str, Any]],
    table_scores: dict[str, int],
    issues_by_table: dict[str, list[Issue]],
) -> str:
    """Build the simple quality_scores.md content (sorted worst to best)."""
    rows: list[tuple[str, str, int, str, int]] = []
    for t in tables:
        key = f"{t['source']}::{t['table_name']}"
        score = table_scores.get(key, 100)
        grade = _score_grade(score)
        issue_count = len(issues_by_table.get(key, []))
        rows.append((t["source"], t["table_name"], score, grade, issue_count))

    # Sort worst (lowest score) to best (highest score)
    rows.sort(key=lambda r: (r[2], r[0], r[1]))

    lines: list[str] = []
    lines.append("# Quality Scores")
    lines.append("")
    lines.append(f"*Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}*")
    lines.append("")
    lines.append("| Source | Table | Score | Grade | Issues |")
    lines.append("|--------|-------|-------|-------|--------|")
    for src, tname, score, grade, issue_count in rows:
        lines.append(f"| {src} | {tname} | {score} | {grade} | {issue_count} |")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def audit_quality(output_dir: str | Path = "output") -> str:
    """Run a comprehensive data quality audit across all profiled sources.

    Parameters
    ----------
    output_dir:
        Root output directory containing ``sources/*/tables/*.md``.

    Returns
    -------
    str
        The full audit report Markdown content.
    """
    output_dir = Path(output_dir)
    sources_dir = output_dir / "sources"

    if not sources_dir.exists():
        print(f"[quality] ERROR: sources directory not found: {sources_dir}", file=sys.stderr)
        return ""

    # Discover source directories
    source_dirs = sorted(
        d for d in sources_dir.iterdir()
        if d.is_dir() and (d / "tables").exists()
    )
    if not source_dirs:
        print("[quality] ERROR: no source directories with tables/ found.", file=sys.stderr)
        return ""

    print(f"[quality] Found {len(source_dirs)} source(s) to audit.", file=sys.stderr)

    # ------------------------------------------------------------------
    # Phase 1: Parse all table MDs
    # ------------------------------------------------------------------
    all_tables: list[dict[str, Any]] = []
    tables_by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for src_dir in source_dirs:
        source_name = src_dir.name
        md_files = sorted((src_dir / "tables").glob("*.md"))
        print(
            f"[quality] Parsing {len(md_files)} table(s) for source '{source_name}'...",
            file=sys.stderr,
        )
        for md_file in md_files:
            print(f"[quality]   {md_file.name}", file=sys.stderr)
            t = _parse_table_md(md_file)
            # Ensure source is populated (fallback to directory name)
            if not t["source"]:
                t["source"] = source_name
            all_tables.append(t)
            tables_by_source[t["source"]].append(t)

    total_tables = len(all_tables)
    print(f"[quality] Parsed {total_tables} table(s) across {len(tables_by_source)} source(s).",
          file=sys.stderr)

    # ------------------------------------------------------------------
    # Phase 2: Evaluate quality dimensions
    # ------------------------------------------------------------------
    print("[quality] Evaluating quality dimensions...", file=sys.stderr)

    all_issues: list[Issue] = []
    issues_by_table: dict[str, list[Issue]] = defaultdict(list)

    for source, src_tables in tables_by_source.items():
        # Build set of known table names in this source for FK validation
        known_tables = {t["table_name"] for t in src_tables}

        for t in src_tables:
            key = f"{source}::{t['table_name']}"

            # Completeness
            for issue in _evaluate_completeness(t, source):
                all_issues.append(issue)
                issues_by_table[key].append(issue)

            # Integrity
            for issue in _evaluate_integrity(t, source, known_tables):
                all_issues.append(issue)
                issues_by_table[key].append(issue)

            # Structure
            for issue in _evaluate_structure(t, source):
                all_issues.append(issue)
                issues_by_table[key].append(issue)

    # Consistency (cross-table analysis)
    consistency_issues = _evaluate_consistency(tables_by_source)
    all_issues.extend(consistency_issues)
    # Consistency issues are cross-table; associate with a synthetic key
    for issue in consistency_issues:
        key = f"{issue.source}::{issue.table_name}"
        issues_by_table[key].append(issue)

    print(
        f"[quality] Found {len(all_issues)} issue(s) total.",
        file=sys.stderr,
    )

    # ------------------------------------------------------------------
    # Phase 3: Scoring
    # ------------------------------------------------------------------
    print("[quality] Computing scores...", file=sys.stderr)

    # Per-table scores
    table_scores: dict[str, int] = {}
    for t in all_tables:
        key = f"{t['source']}::{t['table_name']}"
        table_scores[key] = _compute_table_score(issues_by_table.get(key, []))

    # Per-source scores (weighted by row count)
    source_scores: dict[str, float] = {}
    source_weights: list[tuple[float, float]] = []

    for source, src_tables in tables_by_source.items():
        scores_weights: list[tuple[float, float]] = []
        for t in src_tables:
            key = f"{source}::{t['table_name']}"
            score = table_scores.get(key, 100)
            weight = max(t["row_count"], 1)  # at least 1 so empty tables still count
            scores_weights.append((float(score), float(weight)))
        src_score = _weighted_average(scores_weights)
        source_scores[source] = src_score
        total_source_rows = sum(max(t["row_count"], 1) for t in src_tables)
        source_weights.append((src_score, float(total_source_rows)))

    # Overall score (weighted average across sources)
    overall_score = _weighted_average(source_weights)

    print(
        f"[quality] Overall score: {overall_score:.1f} ({_score_grade(overall_score)})",
        file=sys.stderr,
    )

    # ------------------------------------------------------------------
    # Phase 4: Generate reports
    # ------------------------------------------------------------------
    print("[quality] Generating reports...", file=sys.stderr)

    # Full audit report
    full_report = _build_full_report(
        tables=all_tables,
        issues_by_table=issues_by_table,
        table_scores=table_scores,
        source_scores=source_scores,
        overall_score=overall_score,
        all_issues=all_issues,
        tables_by_source=tables_by_source,
    )

    audit_path = output_dir / "analysis" / "quality_audit.md"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(full_report, encoding="utf-8")
    print(f"[quality] Wrote {audit_path}", file=sys.stderr)

    # Simple scores report
    scores_report = _build_scores_report(
        tables=all_tables,
        table_scores=table_scores,
        issues_by_table=issues_by_table,
    )

    scores_path = output_dir / "context" / "feedback" / "quality_scores.md"
    scores_path.parent.mkdir(parents=True, exist_ok=True)
    scores_path.write_text(scores_report, encoding="utf-8")
    print(f"[quality] Wrote {scores_path}", file=sys.stderr)

    print("[quality] Done.", file=sys.stderr)
    return full_report


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    base = Path(__file__).resolve().parents[2] / "output"
    result = audit_quality(base)
    if not result:
        sys.exit(1)
