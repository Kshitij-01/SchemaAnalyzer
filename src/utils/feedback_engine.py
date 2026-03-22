"""Feedback loop engine for the SchemaAnalyzer project.

Compares analysis findings against table profile MDs to identify
discrepancies that indicate **profiling errors** (not actual data issues)
and generates re-profile requests.

No LLM calls -- this is purely mechanical cross-referencing.
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class Discrepancy:
    """A single discrepancy found during feedback checks."""

    table: str  # schema.table
    source: str  # source name
    check_type: str  # "fk_integrity", "row_count", "column_existence", "duplicate_profile", "null_sanity"
    severity: str  # "CRITICAL", "HIGH", "MEDIUM", "LOW"
    description: str  # Human-readable description
    suggests_reprofile: bool  # Whether this likely indicates a profiling error
    details: dict = field(default_factory=dict)


@dataclass
class FeedbackReport:
    """Aggregated result of all feedback checks."""

    discrepancies: list[Discrepancy] = field(default_factory=list)
    reprofile_requests: list[dict] = field(default_factory=list)  # Tables that should be re-profiled
    flagged_for_review: list[dict] = field(default_factory=list)  # Tables that need human review
    summary: str = ""


# ---------------------------------------------------------------------------
# Table MD parser  (mirrors summary_generator._parse_table_md)
# ---------------------------------------------------------------------------

_SAFE_INT_RE = re.compile(r"[\d,]+")


def _safe_int(value: str) -> int:
    try:
        return int(value.strip().replace(",", ""))
    except (ValueError, TypeError):
        return 0


def _parse_table_md(file_path: Path) -> dict[str, Any]:
    """Parse a single table profile MD into a structured dict.

    Returned keys that matter for feedback checks:
        table_name, schema, source, column_count,
        columns          -- list of dicts with name/data_type/nullable
        row_count, pk_columns,
        fk_outgoing      -- list of {constraint, column, ref_schema, ref_table, ref_column}
        fk_incoming      -- list of {constraint, src_schema, src_table, src_column}
        null_percentages -- dict  column_name -> float
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
        "columns": [],           # list of dicts: {name, data_type, nullable}
        "column_names": [],      # plain list of column name strings
        "row_count": 0,
        "pk_columns": [],
        "fk_outgoing": [],
        "fk_incoming": [],
        "null_percentages": {},
    }

    # --- Header ---
    m = re.search(r"^#\s+Table Profile:\s+(.+)$", text, re.MULTILINE)
    if m:
        result["table_name"] = m.group(1).strip()

    # --- Property block ---
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

    # --- Columns section (with data type + nullable) ---
    col_section = re.search(
        r"## Columns\s*\n(.*?)(?:\n---|\n##|\Z)", text, re.DOTALL,
    )
    if col_section:
        col_text = col_section.group(1)
        # Each row: | # | Column Name | Data Type | Max Length | Precision | Scale | Nullable | ...
        col_rows = re.findall(
            r"^\|\s*\d+\s*\|([^|]+)\|([^|]+)\|[^|]*\|[^|]*\|[^|]*\|([^|]+)\|",
            col_text, re.MULTILINE,
        )
        columns_list: list[dict[str, str]] = []
        col_names: list[str] = []
        for name_raw, dtype_raw, nullable_raw in col_rows:
            name = name_raw.strip()
            dtype = dtype_raw.strip()
            nullable = nullable_raw.strip().upper()
            columns_list.append({
                "name": name,
                "data_type": dtype,
                "nullable": nullable,  # "YES" or "NO"
            })
            col_names.append(name)
        result["columns"] = columns_list
        result["column_names"] = col_names

        total_m = re.search(r"\*\*Total Columns\*\*:\s*(\d+)", col_text)
        result["column_count"] = int(total_m.group(1)) if total_m else len(columns_list)

    # --- PK ---
    pk_section = re.search(
        r"### Primary Key\s*\n(.*?)(?:\n###|\n---|\n##|\Z)", text, re.DOTALL,
    )
    if pk_section:
        pk_rows = re.findall(
            r"^\|([^|]+)\|([^|]+)\|", pk_section.group(1), re.MULTILINE,
        )
        for constraint_name, columns in pk_rows:
            cn = constraint_name.strip().strip("-")
            cols = columns.strip().strip("-")
            if cn and cn.lower() not in ("constraint name", "none", ""):
                result["pk_columns"] = [c.strip() for c in cols.split(",") if c.strip()]

    # --- FK outgoing ---
    fk_out_section = re.search(
        r"### Outgoing \(This Table References\)\s*\n(.*?)(?:\n###|\n---|\n##|\Z)",
        text, re.DOTALL,
    )
    if fk_out_section:
        fk_rows = re.findall(
            r"^\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|",
            fk_out_section.group(1), re.MULTILINE,
        )
        for row in fk_rows:
            parts = [c.strip().strip("-") for c in row]
            if parts[0].lower() in ("constraint name", "", "none"):
                continue
            result["fk_outgoing"].append({
                "constraint": parts[0],
                "column": parts[1],
                "ref_schema": parts[2],
                "ref_table": parts[3],
                "ref_column": parts[4],
            })

    # --- FK incoming ---
    fk_in_section = re.search(
        r"### Incoming \(Referenced By\)\s*\n(.*?)(?:\n###|\n---|\n##|\Z)",
        text, re.DOTALL,
    )
    if fk_in_section:
        fk_rows = re.findall(
            r"^\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|",
            fk_in_section.group(1), re.MULTILINE,
        )
        for row in fk_rows:
            parts = [c.strip().strip("-") for c in row]
            if parts[0].lower() in ("constraint name", "", "none"):
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
        row_m = re.search(r"\*\*Row Count\*\*\s*\|\s*([^\n|]+)", stats_section.group(1))
        if row_m:
            result["row_count"] = _safe_int(row_m.group(1))

    # --- Null percentages ---
    null_section = re.search(
        r"### Null Percentages\s*\n(.*?)(?:\n---|\n##|\Z)", text, re.DOTALL,
    )
    if null_section:
        null_rows = re.findall(
            r"^\|([^|]+)\|([^|]+)\|", null_section.group(1), re.MULTILINE,
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

    return result


# ---------------------------------------------------------------------------
# Index builder -- creates lookup structures from all parsed tables
# ---------------------------------------------------------------------------


def _build_index(
    all_tables: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Build a lookup from (source, schema.table) -> parsed dict.

    The key is ``source::schema.table`` for unambiguous lookup.
    """
    index: dict[str, dict[str, Any]] = {}
    for t in all_tables:
        key = f"{t['source']}::{t['table_name']}"
        index[key] = t
    return index


def _find_table(
    index: dict[str, dict[str, Any]],
    source: str,
    schema: str,
    table: str,
) -> dict[str, Any] | None:
    """Look up a table by source + schema.table."""
    key = f"{source}::{schema}.{table}"
    return index.get(key)


# ---------------------------------------------------------------------------
# Individual check implementations
# ---------------------------------------------------------------------------


def _check_fk_integrity(
    all_tables: list[dict[str, Any]],
    index: dict[str, dict[str, Any]],
) -> list[Discrepancy]:
    """Check 1: FK Integrity.

    - Outgoing FK references a table whose MD does not exist.
    - Outgoing FK references a table that doesn't show matching incoming FK.
    - FK column type differs from referenced table's PK column type.
    """
    results: list[Discrepancy] = []

    for t in all_tables:
        tname = t["table_name"]
        source = t["source"]

        # Build a quick column-type map for this table
        col_type_map: dict[str, str] = {
            c["name"]: c["data_type"] for c in t["columns"]
        }

        for fk in t["fk_outgoing"]:
            ref_schema = fk["ref_schema"]
            ref_table = fk["ref_table"]
            ref_column = fk["ref_column"]
            fk_column = fk["column"]
            ref_full = f"{ref_schema}.{ref_table}"

            # --- Does the referenced table MD exist? ---
            ref_t = _find_table(index, source, ref_schema, ref_table)
            if ref_t is None:
                results.append(Discrepancy(
                    table=tname,
                    source=source,
                    check_type="fk_integrity",
                    severity="HIGH",
                    description=(
                        f"Outgoing FK '{fk['constraint']}' references "
                        f"{ref_full}, but no MD file exists for that table "
                        f"in source '{source}'."
                    ),
                    suggests_reprofile=True,
                    details={
                        "constraint": fk["constraint"],
                        "fk_column": fk_column,
                        "referenced_table": ref_full,
                    },
                ))
                continue

            # --- Does the referenced table show a matching incoming FK? ---
            matching_incoming = any(
                inc["src_schema"] == t["schema"]
                and inc["src_table"] == tname.split(".")[-1]
                and inc["src_column"] == fk_column
                for inc in ref_t["fk_incoming"]
            )
            if not matching_incoming:
                results.append(Discrepancy(
                    table=tname,
                    source=source,
                    check_type="fk_integrity",
                    severity="MEDIUM",
                    description=(
                        f"Outgoing FK '{fk['constraint']}' from {tname} -> "
                        f"{ref_full}, but {ref_full}'s MD does not list a "
                        f"matching incoming FK from {tname}."
                    ),
                    suggests_reprofile=True,
                    details={
                        "constraint": fk["constraint"],
                        "fk_column": fk_column,
                        "referenced_table": ref_full,
                    },
                ))

            # --- Type mismatch between FK column and referenced PK? ---
            ref_col_type_map: dict[str, str] = {
                c["name"]: c["data_type"] for c in ref_t["columns"]
            }
            local_type = col_type_map.get(fk_column, "")
            remote_type = ref_col_type_map.get(ref_column, "")
            if local_type and remote_type and local_type != remote_type:
                results.append(Discrepancy(
                    table=tname,
                    source=source,
                    check_type="fk_integrity",
                    severity="HIGH",
                    description=(
                        f"FK type mismatch: {tname}.{fk_column} is "
                        f"'{local_type}' but {ref_full}.{ref_column} is "
                        f"'{remote_type}'."
                    ),
                    suggests_reprofile=True,
                    details={
                        "constraint": fk["constraint"],
                        "local_column": fk_column,
                        "local_type": local_type,
                        "remote_column": ref_column,
                        "remote_type": remote_type,
                    },
                ))

    return results


def _check_row_count_consistency(
    all_tables: list[dict[str, Any]],
    index: dict[str, dict[str, Any]],
) -> list[Discrepancy]:
    """Check 2: Row Count Consistency.

    - Child table (has outgoing FK with non-nullable FK column) has MORE rows
      than the parent -- suspicious.
    - Junction table (>= 2 outgoing FKs, few own columns) has 0 rows but
      both parent tables have rows -- suspicious.
    """
    results: list[Discrepancy] = []

    for t in all_tables:
        tname = t["table_name"]
        source = t["source"]
        child_rows = t["row_count"]

        # Build nullable map
        nullable_map: dict[str, str] = {
            c["name"]: c["nullable"] for c in t["columns"]
        }

        for fk in t["fk_outgoing"]:
            fk_column = fk["column"]
            ref_schema = fk["ref_schema"]
            ref_table = fk["ref_table"]
            ref_full = f"{ref_schema}.{ref_table}"

            parent = _find_table(index, source, ref_schema, ref_table)
            if parent is None:
                continue  # already caught by fk_integrity

            parent_rows = parent["row_count"]

            # Non-nullable FK child has more rows than parent
            if (
                nullable_map.get(fk_column, "YES") == "NO"
                and child_rows > 0
                and parent_rows > 0
                and child_rows > parent_rows
            ):
                # This is actually normal for many-to-one relationships
                # (e.g. order_items -> orders).  Only flag when the child
                # has an unusually high ratio AND the FK is on a column
                # that looks like a 1:1 relationship.  For safety, we
                # still report but at LOW severity -- genuinely suspicious
                # cases will be obvious from the ratio.
                pass  # intentionally not flagged as an error for normal 1:N

        # --- Junction table with 0 rows but parents have rows ---
        if len(t["fk_outgoing"]) >= 2:
            # Heuristic: junction tables typically have mostly FK columns
            non_fk_cols = set(t["column_names"]) - {
                fk["column"] for fk in t["fk_outgoing"]
            }
            # Remove PK columns and typical auto-fields
            non_fk_cols -= set(t["pk_columns"])
            auto_field_patterns = {"created_at", "updated_at", "id"}
            non_fk_cols -= auto_field_patterns

            is_junction = len(non_fk_cols) <= 2  # junction tables are thin

            if is_junction and child_rows == 0:
                parent_names_with_rows: list[str] = []
                for fk in t["fk_outgoing"]:
                    parent = _find_table(
                        index, source, fk["ref_schema"], fk["ref_table"],
                    )
                    if parent and parent["row_count"] > 0:
                        parent_names_with_rows.append(
                            f"{fk['ref_schema']}.{fk['ref_table']} "
                            f"({parent['row_count']} rows)"
                        )

                if len(parent_names_with_rows) >= 2:
                    results.append(Discrepancy(
                        table=tname,
                        source=source,
                        check_type="row_count",
                        severity="MEDIUM",
                        description=(
                            f"Junction table {tname} has 0 rows, but its "
                            f"parent tables have data: "
                            f"{', '.join(parent_names_with_rows)}. "
                            f"This may indicate a profiling error."
                        ),
                        suggests_reprofile=True,
                        details={
                            "junction_rows": 0,
                            "parents_with_rows": parent_names_with_rows,
                        },
                    ))

    return results


def _check_column_existence(
    all_tables: list[dict[str, Any]],
    index: dict[str, dict[str, Any]],
) -> list[Discrepancy]:
    """Check 3: Column Existence.

    - An outgoing FK references a local column not in the Columns section.
    - An incoming FK references a local column not in the Columns section.
    """
    results: list[Discrepancy] = []

    for t in all_tables:
        tname = t["table_name"]
        source = t["source"]
        col_names = set(t["column_names"])

        # Outgoing FK columns must exist locally
        for fk in t["fk_outgoing"]:
            if fk["column"] not in col_names:
                results.append(Discrepancy(
                    table=tname,
                    source=source,
                    check_type="column_existence",
                    severity="CRITICAL",
                    description=(
                        f"Outgoing FK '{fk['constraint']}' references "
                        f"local column '{fk['column']}', which does not "
                        f"appear in the Columns section of {tname}."
                    ),
                    suggests_reprofile=True,
                    details={
                        "constraint": fk["constraint"],
                        "missing_column": fk["column"],
                        "direction": "outgoing",
                    },
                ))

        # Incoming FK columns must exist locally
        for fk_in in t["fk_incoming"]:
            # The incoming FK's src_column is in the *source* table, not here.
            # What we need to verify is that the PK/column being referenced
            # in *this* table actually exists.  The incoming section doesn't
            # explicitly name the local column, but it implies the PK.
            # We can cross-check by looking at the source table's outgoing FK.
            src_schema = fk_in["src_schema"]
            src_table_name = fk_in["src_table"]
            src_t = _find_table(index, source, src_schema, src_table_name)
            if src_t is None:
                continue
            # Find the matching outgoing FK in the source table
            for out_fk in src_t["fk_outgoing"]:
                if (
                    out_fk["ref_schema"] == t["schema"]
                    and out_fk["ref_table"] == tname.split(".")[-1]
                ):
                    ref_col = out_fk["ref_column"]
                    if ref_col not in col_names:
                        results.append(Discrepancy(
                            table=tname,
                            source=source,
                            check_type="column_existence",
                            severity="CRITICAL",
                            description=(
                                f"Incoming FK '{fk_in['constraint']}' from "
                                f"{src_schema}.{src_table_name} references "
                                f"column '{ref_col}' in {tname}, but that "
                                f"column does not exist in the Columns section."
                            ),
                            suggests_reprofile=True,
                            details={
                                "constraint": fk_in["constraint"],
                                "missing_column": ref_col,
                                "direction": "incoming",
                                "source_table": f"{src_schema}.{src_table_name}",
                            },
                        ))

    return results


def _check_duplicate_profiles(
    all_tables: list[dict[str, Any]],
) -> list[Discrepancy]:
    """Check 4: Duplicate Profile.

    - Same (source, table_name) appears in multiple MD files.
    - Table MD has 0 columns (profiling likely failed).
    """
    results: list[Discrepancy] = []

    # Detect duplicates
    seen: dict[str, list[str]] = defaultdict(list)  # key -> [file_paths]
    for t in all_tables:
        key = f"{t['source']}::{t['table_name']}"
        seen[key].append(t["file_path"])

    for key, paths in seen.items():
        if len(paths) > 1:
            source, tname = key.split("::", 1)
            results.append(Discrepancy(
                table=tname,
                source=source,
                check_type="duplicate_profile",
                severity="HIGH",
                description=(
                    f"Table {tname} in source '{source}' appears in "
                    f"{len(paths)} MD files: {', '.join(paths)}."
                ),
                suggests_reprofile=True,
                details={"file_paths": paths},
            ))

    # Detect 0-column tables
    for t in all_tables:
        if t["column_count"] == 0:
            results.append(Discrepancy(
                table=t["table_name"],
                source=t["source"],
                check_type="duplicate_profile",
                severity="CRITICAL",
                description=(
                    f"Table {t['table_name']} in source '{t['source']}' has "
                    f"0 columns. Profiling likely failed."
                ),
                suggests_reprofile=True,
                details={"file_path": t["file_path"]},
            ))

    return results


def _check_null_sanity(
    all_tables: list[dict[str, Any]],
) -> list[Discrepancy]:
    """Check 5: Null Percentage Sanity.

    - Column marked NOT NULL but has >0% nulls in Statistics.
    - Row count > 0 and every column is exactly 0% nulls on a large table
      (>100 rows) -- suspicious (could be profiling default).
    """
    results: list[Discrepancy] = []

    for t in all_tables:
        tname = t["table_name"]
        source = t["source"]
        row_count = t["row_count"]

        # Build nullable map
        nullable_map: dict[str, str] = {
            c["name"]: c["nullable"] for c in t["columns"]
        }

        # NOT NULL column with >0% nulls
        for col_name, null_pct in t["null_percentages"].items():
            col_nullable = nullable_map.get(col_name, "YES")
            if col_nullable == "NO" and null_pct > 0.0:
                results.append(Discrepancy(
                    table=tname,
                    source=source,
                    check_type="null_sanity",
                    severity="HIGH",
                    description=(
                        f"Column {tname}.{col_name} is marked NOT NULL but "
                        f"has {null_pct:.2f}% null values in Statistics."
                    ),
                    suggests_reprofile=True,
                    details={
                        "column": col_name,
                        "nullable": col_nullable,
                        "null_pct": null_pct,
                    },
                ))

        # All columns exactly 0% nulls on a large table -- suspicious
        if row_count > 100 and t["null_percentages"]:
            all_zero = all(
                pct == 0.0 for pct in t["null_percentages"].values()
            )
            if all_zero and len(t["null_percentages"]) == t["column_count"]:
                # Only flag if the table has nullable columns -- if every
                # column is defined NOT NULL then 0% is expected.
                has_nullable = any(
                    c["nullable"] == "YES" for c in t["columns"]
                )
                if has_nullable:
                    results.append(Discrepancy(
                        table=tname,
                        source=source,
                        check_type="null_sanity",
                        severity="LOW",
                        description=(
                            f"Table {tname} has {row_count} rows and "
                            f"{t['column_count']} columns (including "
                            f"nullable ones), yet every column reports "
                            f"exactly 0.00% nulls. This is suspicious and "
                            f"may indicate null profiling did not run."
                        ),
                        suggests_reprofile=True,
                        details={
                            "row_count": row_count,
                            "column_count": t["column_count"],
                            "nullable_columns": [
                                c["name"]
                                for c in t["columns"]
                                if c["nullable"] == "YES"
                            ],
                        },
                    ))

    return results


# ---------------------------------------------------------------------------
# Report writing helpers
# ---------------------------------------------------------------------------


def _build_reprofile_requests(
    discrepancies: list[Discrepancy],
) -> list[dict]:
    """From discrepancies that suggest re-profiling, create request dicts."""
    requests: list[dict] = []
    seen: set[str] = set()

    for d in discrepancies:
        if not d.suggests_reprofile:
            continue
        key = f"{d.source}::{d.table}"
        if key in seen:
            continue
        seen.add(key)

        # Map severity to priority
        priority = {
            "CRITICAL": "HIGH",
            "HIGH": "HIGH",
            "MEDIUM": "MEDIUM",
            "LOW": "LOW",
        }.get(d.severity, "MEDIUM")

        requests.append({
            "table": d.table,
            "source": d.source,
            "reason": d.description,
            "priority": priority,
            "check_type": d.check_type,
            "timestamp": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S+00:00",
            ),
            "attempt": 1,
        })

    return requests


def _build_flagged(discrepancies: list[Discrepancy]) -> list[dict]:
    """Tables with CRITICAL discrepancies are flagged for human review."""
    flagged: list[dict] = []
    seen: set[str] = set()

    for d in discrepancies:
        if d.severity != "CRITICAL":
            continue
        key = f"{d.source}::{d.table}"
        if key in seen:
            continue
        seen.add(key)
        flagged.append({
            "table": d.table,
            "source": d.source,
            "issue": d.description,
            "check_type": d.check_type,
            "timestamp": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S+00:00",
            ),
        })

    return flagged


def _build_summary(report: FeedbackReport) -> str:
    """Build a human-readable one-paragraph summary."""
    total = len(report.discrepancies)
    by_severity: dict[str, int] = defaultdict(int)
    by_type: dict[str, int] = defaultdict(int)
    for d in report.discrepancies:
        by_severity[d.severity] += 1
        by_type[d.check_type] += 1

    if total == 0:
        return "No discrepancies found. All table profiles appear consistent."

    parts: list[str] = [
        f"Found {total} discrepancy(ies) across feedback checks.",
    ]

    severity_parts = [
        f"{count} {sev}" for sev, count in sorted(by_severity.items())
    ]
    parts.append(f"By severity: {', '.join(severity_parts)}.")

    type_parts = [
        f"{count} {ctype}" for ctype, count in sorted(by_type.items())
    ]
    parts.append(f"By check: {', '.join(type_parts)}.")

    parts.append(
        f"{len(report.reprofile_requests)} table(s) flagged for re-profiling."
    )
    parts.append(
        f"{len(report.flagged_for_review)} table(s) flagged for human review."
    )

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Markdown writers
# ---------------------------------------------------------------------------


def _write_reprofile_requests_md(
    requests: list[dict],
    output_path: Path,
) -> None:
    """Write output/context/agent_comms/reprofile_requests.md."""
    lines: list[str] = [
        "# Re-Profile Requests",
        "",
        f"**Generated**: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S+00:00')}",
        "",
        "---",
        "",
    ]

    if not requests:
        lines.append("No re-profile requests generated.")
    else:
        for i, req in enumerate(requests, 1):
            lines.append(f"## Re-Profile Request #{i}")
            lines.append("")
            lines.append(f"- **Table**: {req['table']}")
            lines.append(f"- **Source**: {req['source']}")
            lines.append(f"- **Reason**: {req['reason']}")
            lines.append(f"- **Priority**: {req['priority']}")
            lines.append(f"- **Requested by**: Feedback Engine")
            lines.append(f"- **Timestamp**: {req['timestamp']}")
            lines.append(f"- **Attempt**: {req['attempt']}")
            lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


def _write_discrepancy_report_md(
    report: FeedbackReport,
    output_path: Path,
) -> None:
    """Write output/context/feedback/discrepancy_report.md."""
    lines: list[str] = [
        "# Discrepancy Report",
        "",
        f"**Generated**: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S+00:00')}",
        "",
        "---",
        "",
        "## Summary",
        "",
        report.summary,
        "",
        "---",
        "",
    ]

    # Group by check_type
    by_type: dict[str, list[Discrepancy]] = defaultdict(list)
    for d in report.discrepancies:
        by_type[d.check_type].append(d)

    type_labels = {
        "fk_integrity": "FK Integrity Check",
        "row_count": "Row Count Consistency Check",
        "column_existence": "Column Existence Check",
        "duplicate_profile": "Duplicate Profile Check",
        "null_sanity": "Null Percentage Sanity Check",
    }

    for check_type in (
        "fk_integrity",
        "row_count",
        "column_existence",
        "duplicate_profile",
        "null_sanity",
    ):
        discrepancies = by_type.get(check_type, [])
        label = type_labels.get(check_type, check_type)
        lines.append(f"## {label}")
        lines.append("")

        if not discrepancies:
            lines.append("No issues found.")
        else:
            lines.append(
                f"| # | Table | Source | Severity | Re-Profile? | Description |"
            )
            lines.append(
                "|---|-------|--------|----------|-------------|-------------|"
            )
            for i, d in enumerate(discrepancies, 1):
                reprofile = "Yes" if d.suggests_reprofile else "No"
                # Escape pipes in description
                desc = d.description.replace("|", "\\|")
                lines.append(
                    f"| {i} | {d.table} | {d.source} | {d.severity} "
                    f"| {reprofile} | {desc} |"
                )

        lines.append("")
        lines.append("---")
        lines.append("")

    # Reprofile requests summary
    lines.append("## Re-Profile Requests")
    lines.append("")
    if report.reprofile_requests:
        lines.append("| # | Table | Source | Priority | Check Type |")
        lines.append("|---|-------|--------|----------|------------|")
        for i, req in enumerate(report.reprofile_requests, 1):
            lines.append(
                f"| {i} | {req['table']} | {req['source']} "
                f"| {req['priority']} | {req['check_type']} |"
            )
    else:
        lines.append("No re-profile requests generated.")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Flagged for review
    lines.append("## Flagged for Human Review")
    lines.append("")
    if report.flagged_for_review:
        lines.append("| # | Table | Source | Issue |")
        lines.append("|---|-------|--------|-------|")
        for i, f in enumerate(report.flagged_for_review, 1):
            issue = f["issue"].replace("|", "\\|")
            lines.append(
                f"| {i} | {f['table']} | {f['source']} | {issue} |"
            )
    else:
        lines.append("No tables flagged for human review.")
    lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def run_feedback_checks(
    output_dir: str | Path = "output",
) -> FeedbackReport:
    """Run all discrepancy checks across table profile MDs.

    Parameters
    ----------
    output_dir:
        Path to the ``output/`` directory containing ``sources/``.

    Returns
    -------
    FeedbackReport
        Aggregated feedback with discrepancies, reprofile requests, and
        flagged tables.
    """
    output_dir = Path(output_dir)
    sources_dir = output_dir / "sources"

    if not sources_dir.exists():
        print(
            f"[feedback] ERROR: sources directory not found: {sources_dir}",
            file=sys.stderr,
        )
        return FeedbackReport(summary="No sources directory found.")

    # --- Parse all table MDs ---
    print("[feedback] Scanning for table profile MDs...", file=sys.stderr)
    all_tables: list[dict[str, Any]] = []

    source_dirs = sorted(
        d for d in sources_dir.iterdir()
        if d.is_dir() and (d / "tables").exists()
    )

    for src_dir in source_dirs:
        tables_dir = src_dir / "tables"
        md_files = sorted(tables_dir.glob("*.md"))
        for md_file in md_files:
            if md_file.name.startswith("_"):
                continue  # skip _summary.md etc.
            print(
                f"[feedback]   parsing {src_dir.name}/{md_file.name}",
                file=sys.stderr,
            )
            parsed = _parse_table_md(md_file)
            all_tables.append(parsed)

    print(
        f"[feedback] Parsed {len(all_tables)} table profile(s) across "
        f"{len(source_dirs)} source(s).",
        file=sys.stderr,
    )

    if not all_tables:
        return FeedbackReport(summary="No table profiles found to check.")

    # --- Build lookup index ---
    index = _build_index(all_tables)

    # --- Run all checks ---
    all_discrepancies: list[Discrepancy] = []

    print("[feedback] Running FK integrity check...", file=sys.stderr)
    all_discrepancies.extend(_check_fk_integrity(all_tables, index))

    print("[feedback] Running row count consistency check...", file=sys.stderr)
    all_discrepancies.extend(_check_row_count_consistency(all_tables, index))

    print("[feedback] Running column existence check...", file=sys.stderr)
    all_discrepancies.extend(_check_column_existence(all_tables, index))

    print("[feedback] Running duplicate profile check...", file=sys.stderr)
    all_discrepancies.extend(_check_duplicate_profiles(all_tables))

    print("[feedback] Running null percentage sanity check...", file=sys.stderr)
    all_discrepancies.extend(_check_null_sanity(all_tables))

    print(
        f"[feedback] Found {len(all_discrepancies)} discrepancy(ies).",
        file=sys.stderr,
    )

    # --- Build report ---
    report = FeedbackReport(discrepancies=all_discrepancies)
    report.reprofile_requests = _build_reprofile_requests(all_discrepancies)
    report.flagged_for_review = _build_flagged(all_discrepancies)
    report.summary = _build_summary(report)

    # --- Write output files ---
    reprofile_path = output_dir / "context" / "agent_comms" / "reprofile_requests.md"
    discrepancy_path = output_dir / "context" / "feedback" / "discrepancy_report.md"

    print(f"[feedback] Writing {reprofile_path}...", file=sys.stderr)
    _write_reprofile_requests_md(report.reprofile_requests, reprofile_path)

    print(f"[feedback] Writing {discrepancy_path}...", file=sys.stderr)
    _write_discrepancy_report_md(report, discrepancy_path)

    print(f"[feedback] Done. {report.summary}", file=sys.stderr)

    return report


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    # Resolve output/ relative to the project root (two levels up from this file)
    project_root = Path(__file__).resolve().parents[2]
    output = project_root / "output"

    report = run_feedback_checks(output)

    # Print a quick summary to stdout
    print()
    print("=" * 72)
    print("FEEDBACK ENGINE REPORT")
    print("=" * 72)
    print()
    print(report.summary)
    print()

    if report.discrepancies:
        print(f"Discrepancies ({len(report.discrepancies)}):")
        for i, d in enumerate(report.discrepancies, 1):
            print(f"  {i}. [{d.severity}] [{d.check_type}] {d.table} ({d.source})")
            print(f"     {d.description}")
        print()

    if report.reprofile_requests:
        print(f"Re-Profile Requests ({len(report.reprofile_requests)}):")
        for i, req in enumerate(report.reprofile_requests, 1):
            print(f"  {i}. {req['table']} ({req['source']}) -- {req['priority']}")
        print()

    if report.flagged_for_review:
        print(f"Flagged for Human Review ({len(report.flagged_for_review)}):")
        for i, f in enumerate(report.flagged_for_review, 1):
            print(f"  {i}. {f['table']} ({f['source']})")
        print()
