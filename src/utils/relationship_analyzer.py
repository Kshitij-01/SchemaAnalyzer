"""Analyze relationships across all profiled tables and produce a comprehensive report.

Reads every ``*.md`` table profile across all sources under
``output/sources/*/tables/`` and builds an explicit + implicit relationship
graph.  The final report is written to ``output/analysis/relationships.md``.

No LLM calls are needed -- this is purely mechanical parsing and aggregation.
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
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ColumnInfo:
    """Minimal info about a single column."""
    name: str
    data_type: str
    nullable: str


@dataclass
class FKOutgoing:
    """An outgoing foreign key reference from this table."""
    constraint: str
    column: str
    ref_schema: str
    ref_table: str
    ref_column: str


@dataclass
class FKIncoming:
    """An incoming foreign key reference *to* this table."""
    constraint: str
    src_schema: str
    src_table: str
    src_column: str


@dataclass
class TableInfo:
    """Parsed representation of a single table MD."""
    source: str
    schema: str
    table: str
    database: str
    pk_columns: list[str] = field(default_factory=list)
    columns: list[ColumnInfo] = field(default_factory=list)
    fk_outgoing: list[FKOutgoing] = field(default_factory=list)
    fk_incoming: list[FKIncoming] = field(default_factory=list)

    @property
    def fqn(self) -> str:
        """Fully-qualified name: source/schema.table"""
        return f"{self.source}/{self.schema}.{self.table}"

    @property
    def schema_table(self) -> str:
        """schema.table"""
        return f"{self.schema}.{self.table}"

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def non_fk_column_count(self) -> int:
        fk_cols = {fk.column for fk in self.fk_outgoing}
        return sum(1 for c in self.columns if c.name not in fk_cols)


@dataclass
class ExplicitRelationship:
    """A foreign-key-backed relationship."""
    constraint: str
    source_fqn: str          # source/schema.table
    source_column: str
    target_fqn: str          # source/schema.table
    target_column: str
    same_source: bool


@dataclass
class ImplicitRelationship:
    """A heuristically-detected relationship (no explicit FK)."""
    table_a_fqn: str
    column_a: str
    table_b_fqn: str
    column_b: str
    confidence: str           # HIGH / MEDIUM / LOW
    reason: str


# ---------------------------------------------------------------------------
# MD parser (extended from summary_generator's _parse_table_md)
# ---------------------------------------------------------------------------

def _parse_table_md(file_path: Path) -> TableInfo | None:
    """Parse a single table profile MD file into a *TableInfo* object.

    Returns *None* when the file cannot be meaningfully parsed.
    """
    try:
        text = file_path.read_text(encoding="utf-8")
    except Exception:
        return None

    source = schema = table = database = ""

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
                    schema = val
                elif key == "source":
                    source = val
                elif key == "table":
                    table = val
                elif key == "database":
                    database = val

    if not source or not table:
        return None

    info = TableInfo(source=source, schema=schema, table=table, database=database)

    # --- Columns section (with data types) ---
    col_section = re.search(
        r"## Columns\s*\n(.*?)(?:\n---|\n##|\Z)", text, re.DOTALL,
    )
    if col_section:
        col_text = col_section.group(1)
        # Each row: | # | Column Name | Data Type | Max Length | ... | Nullable | ...
        col_rows = re.findall(
            r"^\|\s*\d+\s*\|([^|]+)\|([^|]+)\|[^|]+\|[^|]+\|[^|]+\|([^|]+)\|",
            col_text, re.MULTILINE,
        )
        for name, dtype, nullable in col_rows:
            info.columns.append(ColumnInfo(
                name=name.strip(),
                data_type=dtype.strip(),
                nullable=nullable.strip(),
            ))

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
                info.pk_columns = [c.strip() for c in cols.split(",") if c.strip()]

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
            info.fk_outgoing.append(FKOutgoing(
                constraint=parts[0],
                column=parts[1],
                ref_schema=parts[2],
                ref_table=parts[3],
                ref_column=parts[4],
            ))

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
            info.fk_incoming.append(FKIncoming(
                constraint=parts[0],
                src_schema=parts[1],
                src_table=parts[2],
                src_column=parts[3],
            ))

    return info


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_type(dtype: str) -> str:
    """Map a Postgres type string to a coarse bucket for compatibility checks."""
    d = dtype.lower().strip()
    if d in ("integer", "bigint", "smallint", "serial", "bigserial"):
        return "integer"
    if d.startswith("character") or d.startswith("varchar") or d == "text":
        return "string"
    if d.startswith("numeric") or d.startswith("decimal") or d in ("real", "double precision"):
        return "numeric"
    if d in ("boolean",):
        return "boolean"
    if "timestamp" in d:
        return "timestamp"
    if d == "date":
        return "date"
    if d == "uuid":
        return "uuid"
    return d


def _types_compatible(t1: str, t2: str) -> bool:
    """Check whether two normalised types are compatible for implicit FK matching."""
    n1, n2 = _normalize_type(t1), _normalize_type(t2)
    if n1 == n2:
        return True
    # integer and numeric are broadly compatible for FK-like patterns
    if {n1, n2} <= {"integer", "numeric"}:
        return True
    return False


def _build_lookup(tables: list[TableInfo]) -> dict[str, TableInfo]:
    """Build a dict keyed by source/schema.table -> TableInfo."""
    return {t.fqn: t for t in tables}


def _find_table_by_schema_table(
    tables_by_fqn: dict[str, TableInfo],
    source: str,
    schema: str,
    table_name: str,
) -> TableInfo | None:
    """Find a table within the same source by schema.table."""
    key = f"{source}/{schema}.{table_name}"
    return tables_by_fqn.get(key)


# ---------------------------------------------------------------------------
# Relationship discovery
# ---------------------------------------------------------------------------

def _discover_explicit(
    tables: list[TableInfo],
    tables_by_fqn: dict[str, TableInfo],
) -> tuple[list[ExplicitRelationship], list[dict[str, str]]]:
    """Walk outgoing FKs of every table and build ExplicitRelationship list.

    Also returns a list of orphaned FKs (target table not found in any MD).
    """
    relationships: list[ExplicitRelationship] = []
    orphaned: list[dict[str, str]] = []
    seen: set[str] = set()  # deduplicate by constraint name + source fqn

    for tbl in tables:
        for fk in tbl.fk_outgoing:
            dedup_key = f"{tbl.fqn}|{fk.constraint}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            # Try to resolve the target table -- same source first
            target = _find_table_by_schema_table(
                tables_by_fqn, tbl.source, fk.ref_schema, fk.ref_table,
            )
            same_source = True
            if target is None:
                # Try all sources
                same_source = False
                for fqn, candidate in tables_by_fqn.items():
                    if (
                        candidate.schema == fk.ref_schema
                        and candidate.table == fk.ref_table
                        and candidate.source != tbl.source
                    ):
                        target = candidate
                        break

            if target is None:
                orphaned.append({
                    "source_fqn": tbl.fqn,
                    "source_column": fk.column,
                    "constraint": fk.constraint,
                    "expected_target": f"{fk.ref_schema}.{fk.ref_table}.{fk.ref_column}",
                })
                continue

            relationships.append(ExplicitRelationship(
                constraint=fk.constraint,
                source_fqn=tbl.fqn,
                source_column=fk.column,
                target_fqn=target.fqn,
                target_column=fk.ref_column,
                same_source=same_source if target.source == tbl.source else False,
            ))
            # Correct same_source if we matched within same source
            if target.source == tbl.source:
                relationships[-1].same_source = True

    return relationships, orphaned


def _discover_implicit(
    tables: list[TableInfo],
    tables_by_fqn: dict[str, TableInfo],
    explicit_pairs: set[tuple[str, str, str, str]],
) -> list[ImplicitRelationship]:
    """Heuristically discover implicit relationships.

    Rules
    -----
    1. Column named ``<name>_id`` matching a table named ``<name>`` or ``<name>s``
       that has a PK containing that column name (or ``id``).
    2. Identical column names with compatible types across different tables/schemas.
    """
    implicit: list[ImplicitRelationship] = []
    seen: set[tuple[str, str, str, str]] = set()

    # Build a reverse lookup: table name (lowercase) -> list of TableInfo
    name_lookup: dict[str, list[TableInfo]] = defaultdict(list)
    for t in tables:
        name_lookup[t.table.lower()].append(t)

    # Build PK lookup: fqn -> set of pk columns
    pk_lookup: dict[str, set[str]] = {
        t.fqn: set(t.pk_columns) for t in tables
    }

    # Build column type lookup: fqn -> {col_name: data_type}
    col_type_lookup: dict[str, dict[str, str]] = {}
    for t in tables:
        col_type_lookup[t.fqn] = {c.name: c.data_type for c in t.columns}

    # --- Rule 1: *_id columns matching table names ---
    for tbl in tables:
        for col in tbl.columns:
            if not col.name.endswith("_id"):
                continue
            base = col.name[:-3]  # strip _id
            # Look for tables named <base> or <base>s
            candidates: list[TableInfo] = []
            for variant in (base, base + "s", base + "es"):
                candidates.extend(name_lookup.get(variant, []))

            for target in candidates:
                if target.fqn == tbl.fqn:
                    continue
                # Skip if already captured as explicit
                pair_key = (tbl.fqn, col.name, target.fqn, col.name)
                reverse_key = (target.fqn, col.name, tbl.fqn, col.name)
                if pair_key in explicit_pairs or reverse_key in explicit_pairs:
                    continue
                if pair_key in seen or reverse_key in seen:
                    continue

                # Determine confidence
                target_pk = pk_lookup.get(target.fqn, set())
                target_types = col_type_lookup.get(target.fqn, {})

                # Check if the target table has a matching PK column
                matching_pk_col = None
                if col.name in target_pk:
                    matching_pk_col = col.name
                elif "id" in target_pk:
                    matching_pk_col = "id"
                else:
                    # Check for <table>_id in PK
                    for pk_col in target_pk:
                        if pk_col == col.name:
                            matching_pk_col = pk_col
                            break

                if matching_pk_col:
                    pk_type = target_types.get(matching_pk_col, "")
                    if pk_type and _types_compatible(col.data_type, pk_type):
                        confidence = "HIGH"
                        reason = (
                            f"Column '{col.name}' matches table '{target.table}' "
                            f"with PK '{matching_pk_col}' (compatible types: "
                            f"{col.data_type} ~ {pk_type})"
                        )
                    else:
                        confidence = "MEDIUM"
                        reason = (
                            f"Column '{col.name}' matches table '{target.table}' "
                            f"with PK '{matching_pk_col}' (type match uncertain)"
                        )
                else:
                    confidence = "LOW"
                    reason = (
                        f"Column '{col.name}' name-matches table '{target.table}' "
                        f"but no obvious PK alignment"
                    )

                seen.add(pair_key)
                implicit.append(ImplicitRelationship(
                    table_a_fqn=tbl.fqn,
                    column_a=col.name,
                    table_b_fqn=target.fqn,
                    column_b=matching_pk_col or col.name,
                    confidence=confidence,
                    reason=reason,
                ))

    # --- Rule 2: Identical column name + compatible type across different schemas/sources ---
    # Build index: col_name -> list of (fqn, data_type)
    col_index: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for t in tables:
        for c in t.columns:
            col_index[c.name].append((t.fqn, c.data_type))

    for col_name, occurrences in col_index.items():
        # Only consider _id columns or common join patterns
        if not col_name.endswith("_id") and col_name != "id":
            continue
        if len(occurrences) < 2:
            continue
        # Cross-pair within different schemas or sources
        for i in range(len(occurrences)):
            fqn_a, type_a = occurrences[i]
            tbl_a = tables_by_fqn[fqn_a]
            for j in range(i + 1, len(occurrences)):
                fqn_b, type_b = occurrences[j]
                tbl_b = tables_by_fqn[fqn_b]
                # Only cross-schema or cross-source
                if tbl_a.source == tbl_b.source and tbl_a.schema == tbl_b.schema:
                    continue
                pair_key = (fqn_a, col_name, fqn_b, col_name)
                reverse_key = (fqn_b, col_name, fqn_a, col_name)
                if pair_key in explicit_pairs or reverse_key in explicit_pairs:
                    continue
                if pair_key in seen or reverse_key in seen:
                    continue

                if _types_compatible(type_a, type_b):
                    pk_a = pk_lookup.get(fqn_a, set())
                    pk_b = pk_lookup.get(fqn_b, set())
                    if col_name in pk_a or col_name in pk_b:
                        confidence = "HIGH"
                        reason = (
                            f"Shared column '{col_name}' (compatible types: "
                            f"{type_a} ~ {type_b}) with PK on one side"
                        )
                    else:
                        confidence = "MEDIUM"
                        reason = (
                            f"Shared column '{col_name}' (compatible types: "
                            f"{type_a} ~ {type_b}) across schemas/sources"
                        )
                else:
                    confidence = "LOW"
                    reason = (
                        f"Shared column '{col_name}' but types differ "
                        f"({type_a} vs {type_b})"
                    )

                seen.add(pair_key)
                implicit.append(ImplicitRelationship(
                    table_a_fqn=fqn_a,
                    column_a=col_name,
                    table_b_fqn=fqn_b,
                    column_b=col_name,
                    confidence=confidence,
                    reason=reason,
                ))

    return implicit


# ---------------------------------------------------------------------------
# Graph analysis
# ---------------------------------------------------------------------------

def _build_adjacency(
    explicit: list[ExplicitRelationship],
) -> dict[str, set[str]]:
    """Build an undirected adjacency set from explicit relationships."""
    adj: dict[str, set[str]] = defaultdict(set)
    for rel in explicit:
        adj[rel.source_fqn].add(rel.target_fqn)
        adj[rel.target_fqn].add(rel.source_fqn)
    return adj


def _find_clusters(
    all_fqns: set[str],
    adj: dict[str, set[str]],
) -> list[set[str]]:
    """Find connected components (clusters) via BFS."""
    visited: set[str] = set()
    clusters: list[set[str]] = []
    for node in sorted(all_fqns):
        if node in visited:
            continue
        cluster: set[str] = set()
        queue = [node]
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            cluster.add(current)
            for neighbour in adj.get(current, set()):
                if neighbour not in visited:
                    queue.append(neighbour)
        clusters.append(cluster)
    return clusters


def _classify_tables(
    tables: list[TableInfo],
    explicit: list[ExplicitRelationship],
) -> tuple[list[TableInfo], list[TableInfo], list[TableInfo], list[TableInfo]]:
    """Classify tables into junction, root, leaf, and orphan categories.

    Returns (junction_tables, root_tables, leaf_tables, orphan_tables).
    """
    # Build directed edge sets
    has_outgoing: set[str] = set()
    has_incoming: set[str] = set()
    for rel in explicit:
        has_outgoing.add(rel.source_fqn)
        has_incoming.add(rel.target_fqn)

    junction: list[TableInfo] = []
    root: list[TableInfo] = []
    leaf: list[TableInfo] = []
    orphan: list[TableInfo] = []

    for tbl in tables:
        fqn = tbl.fqn
        out_count = len(tbl.fk_outgoing)
        in_present = fqn in has_incoming
        out_present = fqn in has_outgoing

        # Junction: >=2 outgoing FKs and <=3 non-FK columns
        if out_count >= 2 and tbl.non_fk_column_count <= 3:
            junction.append(tbl)
            continue

        if in_present and not out_present:
            root.append(tbl)
        elif out_present and not in_present:
            leaf.append(tbl)
        elif not in_present and not out_present:
            orphan.append(tbl)
        # else: interior table (both in and out) -- not classified as root/leaf

    return junction, root, leaf, orphan


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def _fmt_table_row(*cells: str) -> str:
    return "| " + " | ".join(cells) + " |"


def _render_report(
    tables: list[TableInfo],
    explicit: list[ExplicitRelationship],
    implicit: list[ImplicitRelationship],
    orphaned_fks: list[dict[str, str]],
    junction: list[TableInfo],
    root_tables: list[TableInfo],
    leaf_tables: list[TableInfo],
    orphan_tables: list[TableInfo],
    adj: dict[str, set[str]],
    clusters: list[set[str]],
) -> str:
    """Render the full Markdown report."""
    lines: list[str] = []
    w = lines.append

    # --- header ---
    w("# Relationship Analysis Report")
    w("")

    # --- Summary Statistics ---
    intra = [r for r in explicit if r.same_source]
    cross = [r for r in explicit if not r.same_source]
    w("## Summary Statistics")
    w("")
    w(_fmt_table_row("Metric", "Count"))
    w(_fmt_table_row("------", "-----"))
    w(_fmt_table_row("Total Tables Analyzed", str(len(tables))))
    w(_fmt_table_row("Explicit Relationships (FK-based)", str(len(explicit))))
    w(_fmt_table_row("  Intra-Source", str(len(intra))))
    w(_fmt_table_row("  Cross-Source", str(len(cross))))
    w(_fmt_table_row("Implicit Relationships (heuristic)", str(len(implicit))))
    high = sum(1 for r in implicit if r.confidence == "HIGH")
    med = sum(1 for r in implicit if r.confidence == "MEDIUM")
    low = sum(1 for r in implicit if r.confidence == "LOW")
    w(_fmt_table_row("  HIGH confidence", str(high)))
    w(_fmt_table_row("  MEDIUM confidence", str(med)))
    w(_fmt_table_row("  LOW confidence", str(low)))
    w(_fmt_table_row("Orphaned FK References", str(len(orphaned_fks))))
    w(_fmt_table_row("Connected Clusters", str(len([c for c in clusters if len(c) > 1]))))
    w(_fmt_table_row("Isolated Tables (orphans)", str(len(orphan_tables))))
    w(_fmt_table_row("Junction Tables", str(len(junction))))
    w(_fmt_table_row("Root Tables", str(len(root_tables))))
    w(_fmt_table_row("Leaf Tables", str(len(leaf_tables))))
    w("")

    # --- Intra-Source Relationships ---
    w("---")
    w("")
    w("## Intra-Source Relationships")
    w("")
    if intra:
        by_source: dict[str, list[ExplicitRelationship]] = defaultdict(list)
        for r in intra:
            src = r.source_fqn.split("/")[0]
            by_source[src].append(r)

        for src_name in sorted(by_source):
            rels = by_source[src_name]
            w(f"### Source: `{src_name}`")
            w("")
            w(_fmt_table_row("Constraint", "From (schema.table.column)", "To (schema.table.column)"))
            w(_fmt_table_row("----------", "-------------------------", "-----------------------"))
            for r in sorted(rels, key=lambda x: x.constraint):
                from_part = r.source_fqn.split("/", 1)[1] + "." + r.source_column
                to_part = r.target_fqn.split("/", 1)[1] + "." + r.target_column
                w(_fmt_table_row(f"`{r.constraint}`", f"`{from_part}`", f"`{to_part}`"))
            w("")
    else:
        w("_No intra-source foreign key relationships found._")
        w("")

    # --- Cross-Source Relationships ---
    w("---")
    w("")
    w("## Cross-Source Relationships")
    w("")
    if cross:
        w(_fmt_table_row("Constraint", "From (source/schema.table.column)", "To (source/schema.table.column)"))
        w(_fmt_table_row("----------", "--------------------------------", "------------------------------"))
        for r in sorted(cross, key=lambda x: x.constraint):
            from_part = f"{r.source_fqn}.{r.source_column}"
            to_part = f"{r.target_fqn}.{r.target_column}"
            w(_fmt_table_row(f"`{r.constraint}`", f"`{from_part}`", f"`{to_part}`"))
        w("")
    else:
        w("_No cross-source foreign key relationships detected._")
        w("")

    # --- Implicit Relationships ---
    w("---")
    w("")
    w("## Implicit Relationships")
    w("")
    if implicit:
        # Group by confidence
        for level in ("HIGH", "MEDIUM", "LOW"):
            subset = [r for r in implicit if r.confidence == level]
            if not subset:
                continue
            w(f"### {level} Confidence ({len(subset)})")
            w("")
            w(_fmt_table_row("Table A", "Column", "Table B", "Column", "Reason"))
            w(_fmt_table_row("-------", "------", "-------", "------", "------"))
            for r in sorted(subset, key=lambda x: (x.table_a_fqn, x.column_a)):
                w(_fmt_table_row(
                    f"`{r.table_a_fqn}`", f"`{r.column_a}`",
                    f"`{r.table_b_fqn}`", f"`{r.column_b}`",
                    r.reason,
                ))
            w("")
    else:
        w("_No implicit relationships detected._")
        w("")

    # --- Relationship Graph ---
    w("---")
    w("")
    w("## Relationship Graph")
    w("")
    w("Text-based adjacency list showing FK connections per source.")
    w("")
    # Group tables by source for the graph view
    source_tables: dict[str, list[TableInfo]] = defaultdict(list)
    for t in tables:
        source_tables[t.source].append(t)

    for src_name in sorted(source_tables):
        w(f"### Source: `{src_name}`")
        w("")
        w("```")
        src_fqns = {t.fqn for t in source_tables[src_name]}
        has_any_edge = False
        for t in sorted(source_tables[src_name], key=lambda x: x.schema_table):
            neighbours = adj.get(t.fqn, set())
            if not neighbours:
                continue
            has_any_edge = True
            neighbour_labels = sorted(neighbours)
            w(f"  {t.schema_table}")
            for n in neighbour_labels:
                # Mark cross-source edges
                marker = "" if n in src_fqns else " [cross-source]"
                n_label = n.split("/", 1)[1] if "/" in n else n
                w(f"    -> {n_label}{marker}")
        if not has_any_edge:
            w("  (no FK connections)")
        w("```")
        w("")

    # --- Clusters ---
    w("### Connected Clusters")
    w("")
    multi_clusters = [c for c in clusters if len(c) > 1]
    if multi_clusters:
        for idx, cluster in enumerate(sorted(multi_clusters, key=lambda c: -len(c)), 1):
            w(f"**Cluster {idx}** ({len(cluster)} tables):")
            for fqn in sorted(cluster):
                w(f"  - `{fqn}`")
            w("")
    else:
        w("_No multi-table clusters found._")
        w("")

    # --- Junction Tables ---
    w("---")
    w("")
    w("## Junction Tables")
    w("")
    w("Tables with >= 2 outgoing FKs and <= 3 non-FK columns (likely join/bridge tables).")
    w("")
    if junction:
        w(_fmt_table_row("Table", "Outgoing FKs", "Non-FK Columns", "FK Targets"))
        w(_fmt_table_row("-----", "------------", "--------------", "----------"))
        for t in sorted(junction, key=lambda x: x.fqn):
            targets = ", ".join(
                f"{fk.ref_schema}.{fk.ref_table}" for fk in t.fk_outgoing
            )
            w(_fmt_table_row(
                f"`{t.fqn}`",
                str(len(t.fk_outgoing)),
                str(t.non_fk_column_count),
                targets,
            ))
        w("")
    else:
        w("_No junction tables detected._")
        w("")

    # --- Root Tables ---
    w("---")
    w("")
    w("## Root Tables")
    w("")
    w("Tables with incoming FKs but no outgoing FKs (source-of-truth entities).")
    w("")
    if root_tables:
        w(_fmt_table_row("Table", "Incoming FK Count"))
        w(_fmt_table_row("-----", "-----------------"))
        for t in sorted(root_tables, key=lambda x: x.fqn):
            w(_fmt_table_row(f"`{t.fqn}`", str(len(t.fk_incoming))))
        w("")
    else:
        w("_No root tables detected._")
        w("")

    # --- Leaf Tables ---
    w("---")
    w("")
    w("## Leaf Tables")
    w("")
    w("Tables with outgoing FKs but no incoming FKs.")
    w("")
    if leaf_tables:
        w(_fmt_table_row("Table", "Outgoing FK Count"))
        w(_fmt_table_row("-----", "-----------------"))
        for t in sorted(leaf_tables, key=lambda x: x.fqn):
            w(_fmt_table_row(f"`{t.fqn}`", str(len(t.fk_outgoing))))
        w("")
    else:
        w("_No leaf tables detected._")
        w("")

    # --- Orphan Tables ---
    w("---")
    w("")
    w("## Orphan Tables")
    w("")
    w("Tables with no explicit FK relationships (neither incoming nor outgoing).")
    w("")
    if orphan_tables:
        w(_fmt_table_row("Table", "Source", "Columns"))
        w(_fmt_table_row("-----", "------", "-------"))
        for t in sorted(orphan_tables, key=lambda x: x.fqn):
            w(_fmt_table_row(f"`{t.schema_table}`", f"`{t.source}`", str(len(t.columns))))
        w("")
    else:
        w("_No orphan tables detected._")
        w("")

    # --- Orphaned FKs ---
    w("---")
    w("")
    w("## Orphaned Foreign Keys")
    w("")
    w("FK constraints that reference tables not found in any profiled MD.")
    w("")
    if orphaned_fks:
        w(_fmt_table_row("Source Table", "Column", "Constraint", "Expected Target"))
        w(_fmt_table_row("-----------", "------", "----------", "---------------"))
        for o in sorted(orphaned_fks, key=lambda x: x["source_fqn"]):
            w(_fmt_table_row(
                f"`{o['source_fqn']}`",
                f"`{o['source_column']}`",
                f"`{o['constraint']}`",
                f"`{o['expected_target']}`",
            ))
        w("")
    else:
        w("_No orphaned foreign keys detected._")
        w("")

    # --- Analysis Metadata ---
    w("---")
    w("")
    w("## Analysis Metadata")
    w("")
    now = datetime.now(timezone.utc).isoformat()
    sources_seen = sorted({t.source for t in tables})
    schemas_seen = sorted({f"{t.source}/{t.schema}" for t in tables})
    w(_fmt_table_row("Property", "Value"))
    w(_fmt_table_row("--------", "-----"))
    w(_fmt_table_row("Timestamp", now))
    w(_fmt_table_row("Tables Analyzed", str(len(tables))))
    w(_fmt_table_row("Sources", ", ".join(f"`{s}`" for s in sources_seen)))
    w(_fmt_table_row("Schemas", ", ".join(f"`{s}`" for s in schemas_seen)))
    w(_fmt_table_row("Explicit Relationships", str(len(explicit))))
    w(_fmt_table_row("Implicit Relationships", str(len(implicit))))
    w(_fmt_table_row("Orphaned FK References", str(len(orphaned_fks))))
    w("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze_relationships(output_dir: str | Path = "output") -> str:
    """Analyse relationships across all profiled table MDs.

    Parameters
    ----------
    output_dir:
        Root output directory (contains ``sources/`` and will receive
        ``analysis/relationships.md``).

    Returns
    -------
    str
        The absolute path of the written report file.
    """
    output_path = Path(output_dir).resolve()
    tables_glob = output_path / "sources" / "*" / "tables" / "*.md"

    # 1. Discover and parse all table MDs
    md_files = sorted(output_path.glob("sources/*/tables/*.md"))
    _log(f"Found {len(md_files)} table MD files to analyse.")

    tables: list[TableInfo] = []
    for md_file in md_files:
        _log(f"  Parsing {md_file.name} ...")
        info = _parse_table_md(md_file)
        if info is not None:
            tables.append(info)

    _log(f"Successfully parsed {len(tables)} tables across "
         f"{len({t.source for t in tables})} sources.")

    if not tables:
        _log("No tables found -- nothing to analyse.")
        return ""

    # 2. Build lookup
    tables_by_fqn = _build_lookup(tables)

    # 3. Explicit relationships
    _log("Discovering explicit (FK-based) relationships ...")
    explicit, orphaned_fks = _discover_explicit(tables, tables_by_fqn)
    _log(f"  {len(explicit)} explicit relationships, "
         f"{len(orphaned_fks)} orphaned FK references.")

    # 4. Implicit relationships
    _log("Discovering implicit (heuristic) relationships ...")
    explicit_pairs: set[tuple[str, str, str, str]] = set()
    for r in explicit:
        explicit_pairs.add((r.source_fqn, r.source_column, r.target_fqn, r.target_column))
        explicit_pairs.add((r.target_fqn, r.target_column, r.source_fqn, r.source_column))
    implicit = _discover_implicit(tables, tables_by_fqn, explicit_pairs)
    _log(f"  {len(implicit)} implicit relationships detected.")

    # 5. Graph analysis
    _log("Building relationship graph ...")
    adj = _build_adjacency(explicit)
    all_fqns = {t.fqn for t in tables}
    clusters = _find_clusters(all_fqns, adj)

    # 6. Classification
    _log("Classifying tables (junction / root / leaf / orphan) ...")
    junction, root_tables, leaf_tables, orphan_tables = _classify_tables(tables, explicit)

    # 7. Render report
    _log("Rendering report ...")
    report = _render_report(
        tables, explicit, implicit, orphaned_fks,
        junction, root_tables, leaf_tables, orphan_tables,
        adj, clusters,
    )

    # 8. Write output
    analysis_dir = output_path / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)
    out_file = analysis_dir / "relationships.md"
    out_file.write_text(report, encoding="utf-8")
    _log(f"Report written to {out_file}")

    return str(out_file)


def _log(msg: str) -> None:
    """Print progress messages to stderr."""
    print(msg, file=sys.stderr)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    result = analyze_relationships("output")
    if result:
        print(f"Relationship analysis complete: {result}")
    else:
        print("No output produced.", file=sys.stderr)
        sys.exit(1)
