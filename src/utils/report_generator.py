"""Generate a self-contained HTML report from SchemaAnalyzer table profiles.

Produces a single HTML file with embedded CSS, Mermaid ER diagrams,
Chart.js visualisations, interactive tables, and quality dashboards.
CDNs: Mermaid 11, Chart.js 4.  No other external dependencies.
"""

from __future__ import annotations

import html
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Re-use the table MD parser from summary_generator
# ---------------------------------------------------------------------------

# Inline the parser so report_generator is self-contained and can be run
# independently.  We keep perfect parity with summary_generator._parse_table_md.

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


def _bytes_to_human(b: float) -> str:
    if b < 1024:
        return f"{b:.0f} bytes"
    for unit in ("kB", "MB", "GB", "TB"):
        b /= 1024
        if b < 1024:
            return f"{b:,.2f} {unit}"
    return f"{b:,.2f} PB"


def _safe_int(value: str) -> int:
    try:
        return int(value.strip().replace(",", ""))
    except (ValueError, TypeError):
        return 0


def _safe_float(value: str) -> float:
    try:
        return float(value.strip().replace(",", "").replace("%", ""))
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Extended table MD parser  (superset of summary_generator._parse_table_md)
# ---------------------------------------------------------------------------


def _parse_table_md(file_path: Path) -> dict[str, Any]:
    """Parse a single table profile MD into a rich dict."""
    text = file_path.read_text(encoding="utf-8")
    result: dict[str, Any] = {
        "file_path": str(file_path),
        "table_name": "",
        "short_name": "",
        "schema": "",
        "source": "",
        "database": "",
        "table_type": "TABLE",
        "column_count": 0,
        "columns": [],          # list[str]
        "column_details": [],   # list[dict] with name, data_type, nullable, default
        "row_count": 0,
        "total_size": "0 bytes",
        "total_size_bytes": 0.0,
        "pk_columns": [],
        "unique_constraints": [],
        "check_constraints": [],
        "fk_outgoing": [],
        "fk_incoming": [],
        "null_percentages": {},
        "index_count": 0,
        "indexes": [],
        "has_indexes": False,
        "sample_data_header": [],
        "sample_data_rows": [],
        "re_profiled": False,
        "profiled_by": "",
        "profiling_timestamp": "",
    }

    # --- Table name from header ---
    m = re.search(r"^#\s+Table Profile:\s+(.+)$", text, re.MULTILINE)
    if m:
        result["table_name"] = m.group(1).strip()
        result["short_name"] = result["table_name"].split(".")[-1]

    # --- Property table ---
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

    # --- Columns section ---
    col_section = re.search(
        r"## Columns\s*\n(.*?)(?:\n---|\n##|\Z)", text, re.DOTALL,
    )
    if col_section:
        col_text = col_section.group(1)
        # Full column rows: | # | name | type | maxlen | prec | scale | nullable | default | desc |
        col_rows = re.findall(
            r"^\|\s*\d+\s*\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|",
            col_text, re.MULTILINE,
        )
        for row in col_rows:
            name = row[0].strip()
            result["columns"].append(name)
            result["column_details"].append({
                "name": name,
                "data_type": row[1].strip(),
                "max_length": row[2].strip(),
                "precision": row[3].strip(),
                "scale": row[4].strip(),
                "nullable": row[5].strip(),
                "default": row[6].strip(),
                "description": row[7].strip(),
            })
        total_m = re.search(r"\*\*Total Columns\*\*:\s*(\d+)", col_text)
        if total_m:
            result["column_count"] = int(total_m.group(1))
        else:
            result["column_count"] = len(result["columns"])

    # --- Primary Key ---
    pk_section = re.search(
        r"### Primary Key\s*\n(.*?)(?:\n###|\n---|\n##|\Z)", text, re.DOTALL,
    )
    if pk_section:
        pk_rows = re.findall(r"^\|([^|]+)\|([^|]+)\|", pk_section.group(1), re.MULTILINE)
        for cname, cols in pk_rows:
            cn = cname.strip().strip("-")
            if cn and cn.lower() not in ("constraint name", "none", ""):
                result["pk_columns"] = [c.strip() for c in cols.split(",") if c.strip()]

    # --- Unique Constraints ---
    uq_section = re.search(
        r"### Unique Constraints\s*\n(.*?)(?:\n###|\n---|\n##|\Z)", text, re.DOTALL,
    )
    if uq_section:
        uq_rows = re.findall(r"^\|([^|]+)\|([^|]+)\|", uq_section.group(1), re.MULTILINE)
        for cname, cols in uq_rows:
            cn = cname.strip().strip("-")
            if cn and cn.lower() not in ("constraint name", "none", ""):
                result["unique_constraints"].append({
                    "name": cn,
                    "columns": cols.strip(),
                })

    # --- Check Constraints ---
    ck_section = re.search(
        r"### Check Constraints\s*\n(.*?)(?:\n###|\n---|\n##|\Z)", text, re.DOTALL,
    )
    if ck_section:
        ck_rows = re.findall(r"^\|([^|]+)\|([^|]+)\|", ck_section.group(1), re.MULTILINE)
        for cname, clause in ck_rows:
            cn = cname.strip().strip("-")
            if cn and cn.lower() not in ("constraint name", "none", ""):
                result["check_constraints"].append({
                    "name": cn,
                    "clause": clause.strip(),
                })

    # --- FK Outgoing ---
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
            if parts[0].lower() in ("constraint name", "") or parts[0].lower() == "none":
                continue
            result["fk_outgoing"].append({
                "constraint": parts[0],
                "column": parts[1],
                "ref_schema": parts[2],
                "ref_table": parts[3],
                "ref_column": parts[4],
            })

    # --- FK Incoming ---
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
        null_rows = re.findall(r"^\|([^|]+)\|([^|]+)\|", null_section.group(1), re.MULTILINE)
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
        # Parse index rows
        idx_rows = re.findall(
            r"^\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|",
            idx_text, re.MULTILINE,
        )
        for row in idx_rows:
            parts = [c.strip().strip("-") for c in row]
            if parts[0].lower() in ("index name", ""):
                continue
            result["indexes"].append({
                "name": parts[0],
                "definition": parts[1],
                "unique": parts[2],
                "primary": parts[3],
                "type": parts[4],
            })

    # --- Sample Data ---
    sample_section = re.search(
        r"## Sample Data.*?\n(.*?)(?:\n---|\n##|\Z)", text, re.DOTALL,
    )
    if sample_section:
        sample_text = sample_section.group(1).strip()
        sample_lines = [
            l for l in sample_text.splitlines()
            if l.strip().startswith("|") and "---" not in l
        ]
        if sample_lines:
            result["sample_data_header"] = [
                c.strip() for c in sample_lines[0].split("|") if c.strip()
            ]
            for row_line in sample_lines[1:]:
                row_cells = [c.strip() for c in row_line.split("|") if c.strip()]
                if row_cells:
                    result["sample_data_rows"].append(row_cells)

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
# Summary MD parser  (lightweight, pulls high-level stats)
# ---------------------------------------------------------------------------


def _parse_summary_md(file_path: Path) -> dict[str, Any]:
    """Parse a _summary.md for connection info and high-level stats."""
    text = file_path.read_text(encoding="utf-8")
    summary: dict[str, Any] = {
        "source_name": "",
        "source_type": "",
        "host": "",
        "port": "",
        "database": "",
        "total_schemas": 0,
        "total_tables": 0,
        "total_columns": 0,
        "total_rows": 0,
        "total_size": "",
        "quality_issues": [],
    }

    # Connection info
    for key, field in [
        ("Source Name", "source_name"),
        ("Source Type", "source_type"),
        ("Host", "host"),
        ("Port", "port"),
        ("Database", "database"),
    ]:
        m = re.search(rf"\*\*{key}\*\*\s*\|\s*([^\n|]+)", text)
        if m:
            summary[field] = m.group(1).strip()

    # High-level stats
    for key, field in [
        ("Total Schemas", "total_schemas"),
        ("Total Tables", "total_tables"),
        ("Total Columns", "total_columns"),
        ("Estimated Total Rows", "total_rows"),
    ]:
        m = re.search(rf"\*\*{key}\*\*\s*\|\s*([^\n|]+)", text)
        if m:
            summary[field] = _safe_int(m.group(1))

    m = re.search(r"\*\*Estimated Total Size\*\*\s*\|\s*([^\n|]+)", text)
    if m:
        summary["total_size"] = m.group(1).strip()

    # Quality issues from Critical Issues table
    issues_section = re.search(
        r"### Critical Issues\s*\n(.*?)(?:\n###|\n---|\n##|\Z)", text, re.DOTALL,
    )
    if issues_section:
        issue_rows = re.findall(
            r"^\|\s*\d+\s*\|([^|]+)\|([^|]+)\|([^|]+)\|",
            issues_section.group(1), re.MULTILINE,
        )
        for table, issue, details in issue_rows:
            summary["quality_issues"].append({
                "table": table.strip(),
                "issue": issue.strip(),
                "details": details.strip(),
            })

    return summary


# ---------------------------------------------------------------------------
# Helper: escape for HTML and JS contexts
# ---------------------------------------------------------------------------

def _h(s: str) -> str:
    """HTML-escape a string."""
    return html.escape(str(s))


def _j(s: str) -> str:
    """Escape a string for embedding in a JS string literal (single-quoted)."""
    return str(s).replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")


# ---------------------------------------------------------------------------
# Quality score computation
# ---------------------------------------------------------------------------

def _compute_quality_score(table: dict[str, Any]) -> int:
    """Compute a 0-100 quality score for a single table."""
    score = 100

    # No PK: -20
    if not table["pk_columns"]:
        score -= 20

    # No indexes: -10
    if not table["has_indexes"]:
        score -= 10

    # High null columns
    for col, pct in table["null_percentages"].items():
        if pct > 50:
            score -= 8
        elif pct > 20:
            score -= 4
        elif pct > 10:
            score -= 2

    # Empty table: -15
    if table["row_count"] == 0:
        score -= 15

    # Wide table (>50 cols): -5
    if table["column_count"] > 50:
        score -= 5

    return max(0, min(100, score))


# ---------------------------------------------------------------------------
# Classify tables for dependency graph
# ---------------------------------------------------------------------------

def _classify_tables(tables: list[dict[str, Any]]) -> dict[str, str]:
    """Classify tables as root, leaf, junction, or regular."""
    classifications: dict[str, str] = {}
    for t in tables:
        name = t["table_name"]
        has_outgoing = len(t["fk_outgoing"]) > 0
        has_incoming = len(t["fk_incoming"]) > 0

        if has_outgoing and not has_incoming:
            # Only references others, nobody references it -> leaf
            classifications[name] = "leaf"
        elif has_incoming and not has_outgoing:
            # Referenced by others, references nobody -> root
            classifications[name] = "root"
        elif has_outgoing and has_incoming:
            # If it has 3+ FK outgoing, likely a junction table
            if len(t["fk_outgoing"]) >= 3:
                classifications[name] = "junction"
            else:
                classifications[name] = "regular"
        else:
            # No FK at all -> root (standalone)
            classifications[name] = "root"

    return classifications


# ---------------------------------------------------------------------------
# Build all relationships as a flat list
# ---------------------------------------------------------------------------

def _collect_relationships(tables: list[dict[str, Any]]) -> list[dict[str, str]]:
    rels = []
    for t in tables:
        for fk in t["fk_outgoing"]:
            ref_table = f"{fk['ref_schema']}.{fk['ref_table']}"
            rels.append({
                "constraint": fk["constraint"],
                "source_table": t["table_name"],
                "source_column": fk["column"],
                "target_table": ref_table,
                "target_column": fk["ref_column"],
            })
    return rels


# ---------------------------------------------------------------------------
# HTML report generation
# ---------------------------------------------------------------------------

_TOOL_VERSION = "1.0.0"


def generate_html_report(
    source_dir: str | Path,
    source_name: str,
    output_path: str | Path | None = None,
) -> str:
    """Generate a self-contained HTML report from table profiles.

    Parameters
    ----------
    source_dir:
        Path to the source directory (e.g. ``output/sources/jhonson_pharma``).
    source_name:
        Human-readable name for the data source.
    output_path:
        Where to write the HTML file.  Defaults to ``{source_dir}/report.html``.

    Returns
    -------
    str
        The full HTML string.
    """
    source_dir = Path(source_dir)
    tables_dir = source_dir / "tables"

    if not tables_dir.exists():
        print(f"[report] ERROR: tables directory not found: {tables_dir}", file=sys.stderr)
        return ""

    md_files = sorted(tables_dir.glob("*.md"))
    if not md_files:
        print(f"[report] ERROR: no .md files in {tables_dir}", file=sys.stderr)
        return ""

    # Parse tables
    print(f"[report] Parsing {len(md_files)} table profile(s)...", file=sys.stderr)
    tables: list[dict[str, Any]] = []
    for f in md_files:
        tables.append(_parse_table_md(f))

    # Parse summary if available
    summary_path = source_dir / "_summary.md"
    summary: dict[str, Any] = {}
    if summary_path.exists():
        summary = _parse_summary_md(summary_path)

    # Aggregate
    total_tables = len(tables)
    total_columns = sum(t["column_count"] for t in tables)
    total_rows = sum(t["row_count"] for t in tables)
    total_size_bytes = sum(t["total_size_bytes"] for t in tables)
    total_size = _bytes_to_human(total_size_bytes)

    relationships = _collect_relationships(tables)
    classifications = _classify_tables(tables)
    quality_scores: dict[str, int] = {}
    for t in tables:
        quality_scores[t["table_name"]] = _compute_quality_score(t)
    avg_quality = round(sum(quality_scores.values()) / max(len(quality_scores), 1))

    # Quality issue severity
    severity_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    recommendations: list[str] = []
    for t in tables:
        tname = t["table_name"]
        if not t["pk_columns"]:
            severity_counts["CRITICAL"] += 1
            recommendations.append(
                f"Add a primary key to <strong>{_h(tname)}</strong> to ensure row uniqueness and enable efficient lookups."
            )
        for col, pct in t["null_percentages"].items():
            if pct > 50:
                severity_counts["HIGH"] += 1
                recommendations.append(
                    f"Investigate high null rate ({pct:.1f}%) in <strong>{_h(tname)}.{_h(col)}</strong>. "
                    f"Consider adding a NOT NULL constraint or a default value if the column is required."
                )
            elif pct > 20:
                severity_counts["MEDIUM"] += 1
                recommendations.append(
                    f"Column <strong>{_h(tname)}.{_h(col)}</strong> has {pct:.1f}% null values. "
                    f"Verify whether this is expected or indicates missing data."
                )
        if not t["has_indexes"]:
            severity_counts["MEDIUM"] += 1
            recommendations.append(
                f"Table <strong>{_h(tname)}</strong> has no indexes. "
                f"Consider adding indexes on frequently queried columns."
            )
        if t["row_count"] == 0:
            severity_counts["LOW"] += 1
            recommendations.append(
                f"Table <strong>{_h(tname)}</strong> is empty (0 rows). "
                f"Verify if this is expected or if data loading failed."
            )
        # Check FK columns without indexes
        for fk in t["fk_outgoing"]:
            fk_col = fk["column"]
            idx_names = [idx["name"] for idx in t["indexes"]]
            # Simple heuristic: check if any index name contains the FK column name
            has_idx = any(fk_col in idx_name for idx_name in idx_names)
            if not has_idx and t["index_count"] > 0:
                severity_counts["LOW"] += 1
                recommendations.append(
                    f"Consider adding an index on <strong>{_h(tname)}.{_h(fk_col)}</strong> "
                    f"(foreign key to {_h(fk['ref_table'])}) to improve join performance."
                )

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # -----------------------------------------------------------------------
    # Build HTML
    # -----------------------------------------------------------------------
    parts: list[str] = []

    # ---- Head ----
    parts.append(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Schema Report: {_h(source_name)}</title>
<script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
/* ===== Reset & Base ===== */
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
html {{ scroll-behavior: smooth; }}
body {{
    font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, 'Helvetica Neue', Arial, sans-serif;
    background: #0f0f23;
    color: #e0e0e0;
    line-height: 1.6;
    min-height: 100vh;
}}
a {{ color: #64b5f6; text-decoration: none; }}
a:hover {{ text-decoration: underline; }}

/* ===== Layout ===== */
.container {{ max-width: 1400px; margin: 0 auto; padding: 0 24px 60px; }}

/* ===== Hero ===== */
.hero {{
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 40%, #0f3460 100%);
    padding: 48px 0 40px;
    border-bottom: 3px solid #e94560;
    margin-bottom: 40px;
}}
.hero .container {{ display: flex; flex-direction: column; gap: 24px; }}
.hero h1 {{
    font-size: 2.4rem;
    font-weight: 700;
    color: #fff;
    letter-spacing: -0.5px;
}}
.hero h1 span {{ color: #e94560; }}
.hero .subtitle {{
    font-size: 1rem;
    color: #8892b0;
    margin-top: -12px;
}}
.stats-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 16px;
    margin-top: 8px;
}}
.stat-card {{
    background: rgba(255,255,255,0.06);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    padding: 20px;
    text-align: center;
    backdrop-filter: blur(8px);
    transition: transform 0.2s, border-color 0.2s;
}}
.stat-card:hover {{
    transform: translateY(-2px);
    border-color: #e94560;
}}
.stat-card .value {{
    font-size: 2rem;
    font-weight: 700;
    color: #fff;
    display: block;
}}
.stat-card .label {{
    font-size: 0.8rem;
    text-transform: uppercase;
    letter-spacing: 1.2px;
    color: #8892b0;
    margin-top: 4px;
}}
.stat-card.quality .value {{
    color: {('#4caf50' if avg_quality >= 80 else '#ff9800' if avg_quality >= 60 else '#e94560')};
}}

/* ===== Navigation ===== */
.nav-bar {{
    background: #16213e;
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 12px;
    padding: 12px 20px;
    margin-bottom: 36px;
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    position: sticky;
    top: 12px;
    z-index: 100;
    backdrop-filter: blur(12px);
}}
.nav-bar a {{
    color: #8892b0;
    font-size: 0.82rem;
    font-weight: 500;
    padding: 6px 14px;
    border-radius: 6px;
    transition: all 0.2s;
    white-space: nowrap;
}}
.nav-bar a:hover {{
    background: rgba(233, 69, 96, 0.15);
    color: #e94560;
    text-decoration: none;
}}

/* ===== Sections ===== */
.section {{
    margin-bottom: 48px;
}}
.section-title {{
    font-size: 1.5rem;
    font-weight: 700;
    color: #fff;
    margin-bottom: 20px;
    padding-bottom: 12px;
    border-bottom: 2px solid #1a1a2e;
    display: flex;
    align-items: center;
    gap: 10px;
}}
.section-title .icon {{
    width: 28px;
    height: 28px;
    border-radius: 6px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 0.9rem;
    flex-shrink: 0;
}}
.section-title .icon.er {{ background: #e94560; }}
.section-title .icon.dep {{ background: #0f3460; }}
.section-title .icon.quality {{ background: #ff9800; }}
.section-title .icon.table {{ background: #4caf50; }}
.section-title .icon.rel {{ background: #9c27b0; }}
.section-title .icon.vol {{ background: #00bcd4; }}
.section-title .icon.flow {{ background: #3f51b5; }}
.section-title .icon.rec {{ background: #ff5722; }}

/* ===== Cards ===== */
.card {{
    background: #16213e;
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 12px;
    padding: 28px;
    margin-bottom: 20px;
}}
.card.mermaid-card {{
    overflow-x: auto;
    padding: 20px;
}}

/* ===== Mermaid ===== */
.mermaid {{
    display: flex;
    justify-content: center;
    min-height: 200px;
}}
.mermaid svg {{
    max-width: 100%;
    height: auto;
}}

/* ===== Charts ===== */
.chart-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
    gap: 20px;
}}
.chart-card {{
    background: #16213e;
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 12px;
    padding: 24px;
}}
.chart-card h3 {{
    font-size: 1rem;
    color: #8892b0;
    margin-bottom: 16px;
    font-weight: 500;
}}
.chart-card canvas {{
    width: 100% !important;
    max-height: 360px;
}}

/* ===== Tables ===== */
.data-table {{
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    font-size: 0.85rem;
}}
.data-table th {{
    background: #0f3460;
    color: #fff;
    font-weight: 600;
    text-transform: uppercase;
    font-size: 0.72rem;
    letter-spacing: 0.8px;
    padding: 12px 14px;
    text-align: left;
    position: sticky;
    top: 0;
    z-index: 2;
    white-space: nowrap;
}}
.data-table th:first-child {{ border-radius: 8px 0 0 0; }}
.data-table th:last-child {{ border-radius: 0 8px 0 0; }}
.data-table td {{
    padding: 10px 14px;
    border-bottom: 1px solid rgba(255,255,255,0.04);
    color: #c0c0c0;
}}
.data-table tr:nth-child(even) td {{ background: rgba(255,255,255,0.02); }}
.data-table tr:hover td {{ background: rgba(233, 69, 96, 0.08); }}
.table-wrapper {{
    overflow-x: auto;
    border-radius: 12px;
    border: 1px solid rgba(255,255,255,0.06);
}}

/* ===== Null Heatmap ===== */
.null-cell {{
    text-align: center;
    font-weight: 600;
    font-size: 0.78rem;
    padding: 6px 10px;
    border-radius: 4px;
    min-width: 55px;
    display: inline-block;
}}
.null-green  {{ background: rgba(76,175,80,0.25); color: #81c784; }}
.null-yellow {{ background: rgba(255,235,59,0.2); color: #fff176; }}
.null-orange {{ background: rgba(255,152,0,0.25); color: #ffb74d; }}
.null-red    {{ background: rgba(233,69,96,0.3); color: #ef9a9a; }}

/* ===== Collapsible Details ===== */
details {{
    background: #16213e;
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 12px;
    margin-bottom: 12px;
    transition: border-color 0.2s;
}}
details[open] {{ border-color: rgba(233,69,96,0.3); }}
details summary {{
    padding: 16px 24px;
    cursor: pointer;
    font-weight: 600;
    font-size: 1rem;
    color: #fff;
    list-style: none;
    display: flex;
    align-items: center;
    justify-content: space-between;
    user-select: none;
}}
details summary::-webkit-details-marker {{ display: none; }}
details summary::after {{
    content: '+';
    font-size: 1.3rem;
    color: #e94560;
    font-weight: 300;
    transition: transform 0.2s;
}}
details[open] summary::after {{
    content: '\\2212';
}}
details .detail-body {{
    padding: 0 24px 24px;
}}
details .detail-body h4 {{
    color: #8892b0;
    font-size: 0.85rem;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    margin: 20px 0 10px;
    font-weight: 600;
}}
details .detail-body h4:first-child {{ margin-top: 0; }}
.table-meta {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 10px;
    margin-bottom: 16px;
}}
.table-meta .meta-item {{
    background: rgba(255,255,255,0.03);
    border-radius: 8px;
    padding: 12px;
    text-align: center;
}}
.table-meta .meta-item .meta-val {{
    font-size: 1.2rem;
    font-weight: 700;
    color: #fff;
    display: block;
}}
.table-meta .meta-item .meta-lbl {{
    font-size: 0.7rem;
    text-transform: uppercase;
    color: #8892b0;
    letter-spacing: 0.5px;
}}

/* ===== Recommendations ===== */
.rec-item {{
    background: rgba(255,255,255,0.03);
    border-left: 3px solid #e94560;
    border-radius: 0 8px 8px 0;
    padding: 14px 18px;
    margin-bottom: 10px;
    font-size: 0.88rem;
    line-height: 1.55;
}}
.rec-item strong {{ color: #64b5f6; }}

/* ===== Search ===== */
.search-box {{
    background: rgba(255,255,255,0.06);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 8px;
    padding: 10px 16px;
    color: #e0e0e0;
    font-size: 0.88rem;
    width: 100%;
    max-width: 400px;
    margin-bottom: 16px;
    outline: none;
    transition: border-color 0.2s;
}}
.search-box:focus {{ border-color: #e94560; }}
.search-box::placeholder {{ color: #555; }}

/* ===== Footer ===== */
.footer {{
    text-align: center;
    padding: 40px 0 24px;
    border-top: 1px solid rgba(255,255,255,0.06);
    color: #555;
    font-size: 0.78rem;
}}
.footer strong {{ color: #8892b0; }}

/* ===== Responsive ===== */
@media (max-width: 768px) {{
    .hero h1 {{ font-size: 1.6rem; }}
    .stats-grid {{ grid-template-columns: repeat(2, 1fr); }}
    .chart-grid {{ grid-template-columns: 1fr; }}
    .nav-bar {{ position: static; }}
}}
</style>
</head>
<body>
""")

    # ---- Hero Section ----
    parts.append(f"""
<div class="hero">
  <div class="container">
    <h1>Schema<span>Analyzer</span> Report</h1>
    <div class="subtitle">{_h(source_name)} &mdash; Generated {_h(timestamp)}</div>
    <div class="stats-grid">
      <div class="stat-card"><span class="value">{total_tables}</span><span class="label">Tables</span></div>
      <div class="stat-card"><span class="value">{total_columns}</span><span class="label">Columns</span></div>
      <div class="stat-card"><span class="value">{total_rows:,}</span><span class="label">Total Rows</span></div>
      <div class="stat-card"><span class="value">{_h(total_size)}</span><span class="label">Total Size</span></div>
      <div class="stat-card quality"><span class="value">{avg_quality}/100</span><span class="label">Avg Quality Score</span></div>
    </div>
  </div>
</div>
""")

    # ---- Navigation ----
    parts.append("""
<div class="container">
<nav class="nav-bar">
  <a href="#er-diagram">ER Diagram</a>
  <a href="#dependency-graph">Dependencies</a>
  <a href="#data-quality">Data Quality</a>
  <a href="#table-profiles">Table Profiles</a>
  <a href="#relationships">Relationships</a>
  <a href="#data-volume">Data Volume</a>
  <a href="#schema-lineage">Schema Lineage</a>
  <a href="#recommendations">Recommendations</a>
</nav>
""")

    # ===================================================================
    # SECTION 1: ER Diagram (Mermaid erDiagram)
    # ===================================================================
    parts.append("""
<div class="section" id="er-diagram">
  <div class="section-title"><span class="icon er">&#9707;</span> Entity-Relationship Diagram</div>
  <div class="card mermaid-card">
    <pre class="mermaid">
erDiagram
""")

    # Build ER entities showing PK and FK columns only
    for t in tables:
        short = t["short_name"]
        # Collect PK and FK column names
        pk_set = set(t["pk_columns"])
        fk_cols = {fk["column"] for fk in t["fk_outgoing"]}
        shown_cols = pk_set | fk_cols
        parts.append(f"    {short} {{\n")
        for cd in t["column_details"]:
            cname = cd["name"]
            if cname in shown_cols:
                dtype = cd["data_type"].replace(" ", "_")
                marker = "PK" if cname in pk_set else "FK"
                parts.append(f"        {dtype} {cname} {marker}\n")
        parts.append(f"    }}\n")

    # Relationships
    for rel in relationships:
        src_short = rel["source_table"].split(".")[-1]
        tgt_short = rel["target_table"].split(".")[-1]
        # Use }o--|| cardinality (many-to-one from source to target)
        parts.append(f'    {tgt_short} ||--o{{ {src_short} : "{rel["source_column"]}"\n')

    parts.append("""    </pre>
  </div>
</div>
""")

    # ===================================================================
    # SECTION 2: Table Dependency Graph
    # ===================================================================
    parts.append("""
<div class="section" id="dependency-graph">
  <div class="section-title"><span class="icon dep">&#9672;</span> Table Dependency Graph</div>
  <div class="card mermaid-card">
    <pre class="mermaid">
graph LR
""")

    # Style definitions
    style_lines: list[str] = []
    for t in tables:
        tname = t["table_name"]
        short = t["short_name"]
        cls = classifications.get(tname, "regular")
        parts.append(f"    {short}\n")
        if cls == "root":
            style_lines.append(f"    style {short} fill:#2e7d32,stroke:#4caf50,color:#fff\n")
        elif cls == "leaf":
            style_lines.append(f"    style {short} fill:#e65100,stroke:#ff9800,color:#fff\n")
        elif cls == "junction":
            style_lines.append(f"    style {short} fill:#1565c0,stroke:#42a5f5,color:#fff\n")

    # Edges with FK column label
    for rel in relationships:
        src_short = rel["source_table"].split(".")[-1]
        tgt_short = rel["target_table"].split(".")[-1]
        parts.append(f'    {src_short} -->|{rel["source_column"]}| {tgt_short}\n')

    for sl in style_lines:
        parts.append(sl)

    parts.append("""    </pre>
    <div style="margin-top:16px;display:flex;gap:20px;flex-wrap:wrap;font-size:0.8rem;">
      <span><span style="display:inline-block;width:14px;height:14px;background:#2e7d32;border-radius:3px;vertical-align:middle;margin-right:4px;"></span> Root Table</span>
      <span><span style="display:inline-block;width:14px;height:14px;background:#e65100;border-radius:3px;vertical-align:middle;margin-right:4px;"></span> Leaf Table</span>
      <span><span style="display:inline-block;width:14px;height:14px;background:#1565c0;border-radius:3px;vertical-align:middle;margin-right:4px;"></span> Junction Table</span>
      <span><span style="display:inline-block;width:14px;height:14px;background:#1f2937;border:1px solid #555;border-radius:3px;vertical-align:middle;margin-right:4px;"></span> Regular</span>
    </div>
  </div>
</div>
""")

    # ===================================================================
    # SECTION 3: Data Quality Dashboard
    # ===================================================================
    # Prepare chart data
    table_names_sorted = sorted(tables, key=lambda t: t["table_name"])
    qs_labels = json.dumps([t["short_name"] for t in table_names_sorted])
    qs_values = json.dumps([quality_scores[t["table_name"]] for t in table_names_sorted])
    qs_colors = json.dumps([
        '#4caf50' if quality_scores[t["table_name"]] >= 80
        else '#ff9800' if quality_scores[t["table_name"]] >= 60
        else '#e94560'
        for t in table_names_sorted
    ])
    sev_labels = json.dumps(list(severity_counts.keys()))
    sev_values = json.dumps(list(severity_counts.values()))
    sev_colors = json.dumps(["#b71c1c", "#e94560", "#ff9800", "#fdd835"])

    parts.append(f"""
<div class="section" id="data-quality">
  <div class="section-title"><span class="icon quality">&#9888;</span> Data Quality Dashboard</div>
  <div class="chart-grid">
    <div class="chart-card">
      <h3>Quality Score by Table</h3>
      <canvas id="qualityBarChart"></canvas>
    </div>
    <div class="chart-card">
      <h3>Issue Severity Distribution</h3>
      <canvas id="severityDoughnut"></canvas>
    </div>
  </div>
""")

    # Null Percentage Heatmap
    parts.append("""
  <div class="card" style="margin-top:20px;">
    <h3 style="color:#8892b0;font-size:1rem;font-weight:500;margin-bottom:16px;">Null Percentage Heatmap</h3>
    <div class="table-wrapper">
      <table class="data-table">
        <thead><tr><th>Table</th><th>Column</th><th>Null %</th><th>Visual</th></tr></thead>
        <tbody>
""")

    for t in table_names_sorted:
        tname = t["short_name"]
        for col, pct in sorted(t["null_percentages"].items()):
            if pct == 0:
                continue  # Only show columns with some nulls for readability
            if pct <= 10:
                cls = "null-green"
            elif pct <= 20:
                cls = "null-yellow"
            elif pct <= 50:
                cls = "null-orange"
            else:
                cls = "null-red"
            bar_width = min(pct, 100)
            parts.append(
                f'<tr><td>{_h(tname)}</td><td>{_h(col)}</td>'
                f'<td><span class="null-cell {cls}">{pct:.1f}%</span></td>'
                f'<td><div style="background:rgba(255,255,255,0.06);border-radius:4px;height:18px;width:200px;position:relative;">'
                f'<div style="background:{"#4caf50" if pct <= 10 else "#ffeb3b" if pct <= 20 else "#ff9800" if pct <= 50 else "#e94560"};'
                f'height:100%;width:{bar_width}%;border-radius:4px;"></div></div></td></tr>\n'
            )

    # If all columns have 0% nulls, show a nice message
    any_nulls = any(
        pct > 0 for t in tables for pct in t["null_percentages"].values()
    )
    if not any_nulls:
        parts.append(
            '<tr><td colspan="4" style="text-align:center;color:#4caf50;padding:20px;">'
            'All columns have 0% null values</td></tr>\n'
        )

    parts.append("""
        </tbody>
      </table>
    </div>
  </div>
</div>
""")

    # ===================================================================
    # SECTION 4: Table Profiles (Expandable)
    # ===================================================================
    parts.append("""
<div class="section" id="table-profiles">
  <div class="section-title"><span class="icon table">&#9783;</span> Table Profiles</div>
""")

    for t in table_names_sorted:
        tname = t["table_name"]
        short = t["short_name"]
        qs = quality_scores[tname]
        qs_color = '#4caf50' if qs >= 80 else '#ff9800' if qs >= 60 else '#e94560'

        parts.append(f"""
  <details>
    <summary>
      <span>{_h(tname)} <span style="font-weight:400;color:#8892b0;font-size:0.85rem;">({t["column_count"]} cols, {t["row_count"]:,} rows, {_h(t["total_size"])})</span></span>
      <span style="color:{qs_color};font-size:0.85rem;font-weight:600;">Score: {qs}/100</span>
    </summary>
    <div class="detail-body">
""")

        # Meta grid
        parts.append(f"""
      <div class="table-meta">
        <div class="meta-item"><span class="meta-val">{t["column_count"]}</span><span class="meta-lbl">Columns</span></div>
        <div class="meta-item"><span class="meta-val">{t["row_count"]:,}</span><span class="meta-lbl">Rows</span></div>
        <div class="meta-item"><span class="meta-val">{_h(t["total_size"])}</span><span class="meta-lbl">Size</span></div>
        <div class="meta-item"><span class="meta-val">{t["index_count"]}</span><span class="meta-lbl">Indexes</span></div>
        <div class="meta-item"><span class="meta-val">{len(t["fk_outgoing"])}</span><span class="meta-lbl">FK Out</span></div>
        <div class="meta-item"><span class="meta-val">{len(t["fk_incoming"])}</span><span class="meta-lbl">FK In</span></div>
      </div>
""")

        # Columns table
        parts.append("""
      <h4>Columns</h4>
      <div class="table-wrapper">
        <table class="data-table">
          <thead><tr><th>#</th><th>Column</th><th>Data Type</th><th>Nullable</th><th>Default</th><th>Null %</th></tr></thead>
          <tbody>
""")
        for i, cd in enumerate(t["column_details"], 1):
            npct = t["null_percentages"].get(cd["name"], 0)
            is_pk = cd["name"] in t["pk_columns"]
            pk_badge = ' <span style="background:#e94560;color:#fff;padding:1px 6px;border-radius:3px;font-size:0.65rem;font-weight:700;vertical-align:middle;">PK</span>' if is_pk else ''
            is_fk = cd["name"] in {fk["column"] for fk in t["fk_outgoing"]}
            fk_badge = ' <span style="background:#0f3460;color:#fff;padding:1px 6px;border-radius:3px;font-size:0.65rem;font-weight:700;vertical-align:middle;">FK</span>' if is_fk else ''
            parts.append(
                f'<tr><td>{i}</td><td>{_h(cd["name"])}{pk_badge}{fk_badge}</td>'
                f'<td>{_h(cd["data_type"])}</td><td>{_h(cd["nullable"])}</td>'
                f'<td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{_h(cd["default"])}</td>'
                f'<td>{npct:.1f}%</td></tr>\n'
            )
        parts.append("</tbody></table></div>\n")

        # Constraints
        if t["pk_columns"] or t["unique_constraints"] or t["check_constraints"]:
            parts.append("<h4>Constraints</h4>\n<div class='table-wrapper'><table class='data-table'>"
                         "<thead><tr><th>Type</th><th>Name</th><th>Details</th></tr></thead><tbody>\n")
            if t["pk_columns"]:
                parts.append(
                    f'<tr><td>Primary Key</td><td>{_h(", ".join(t["pk_columns"]))}</td>'
                    f'<td>-</td></tr>\n'
                )
            for uc in t["unique_constraints"]:
                parts.append(
                    f'<tr><td>Unique</td><td>{_h(uc["name"])}</td>'
                    f'<td>{_h(uc["columns"])}</td></tr>\n'
                )
            for cc in t["check_constraints"]:
                parts.append(
                    f'<tr><td>Check</td><td>{_h(cc["name"])}</td>'
                    f'<td>{_h(cc["clause"])}</td></tr>\n'
                )
            parts.append("</tbody></table></div>\n")

        # Indexes
        if t["indexes"]:
            parts.append("<h4>Indexes</h4>\n<div class='table-wrapper'><table class='data-table'>"
                         "<thead><tr><th>Name</th><th>Unique</th><th>Primary</th><th>Type</th></tr></thead><tbody>\n")
            for idx in t["indexes"]:
                parts.append(
                    f'<tr><td>{_h(idx["name"])}</td><td>{_h(idx["unique"])}</td>'
                    f'<td>{_h(idx["primary"])}</td><td>{_h(idx["type"])}</td></tr>\n'
                )
            parts.append("</tbody></table></div>\n")

        # FK Relationships
        if t["fk_outgoing"] or t["fk_incoming"]:
            parts.append("<h4>Foreign Key Relationships</h4>\n<div class='table-wrapper'><table class='data-table'>"
                         "<thead><tr><th>Direction</th><th>Constraint</th><th>Column</th><th>Related Table</th><th>Related Column</th></tr></thead><tbody>\n")
            for fk in t["fk_outgoing"]:
                parts.append(
                    f'<tr><td style="color:#ff9800;">Outgoing</td><td>{_h(fk["constraint"])}</td>'
                    f'<td>{_h(fk["column"])}</td><td>{_h(fk["ref_schema"])}.{_h(fk["ref_table"])}</td>'
                    f'<td>{_h(fk["ref_column"])}</td></tr>\n'
                )
            for fk in t["fk_incoming"]:
                parts.append(
                    f'<tr><td style="color:#4caf50;">Incoming</td><td>{_h(fk["constraint"])}</td>'
                    f'<td>-</td><td>{_h(fk["src_schema"])}.{_h(fk["src_table"])}</td>'
                    f'<td>{_h(fk["src_column"])}</td></tr>\n'
                )
            parts.append("</tbody></table></div>\n")

        # Sample Data
        if t["sample_data_rows"]:
            parts.append(f'<h4>Sample Data ({len(t["sample_data_rows"])} rows)</h4>\n'
                         '<div class="table-wrapper"><table class="data-table"><thead><tr>')
            for h in t["sample_data_header"]:
                parts.append(f"<th>{_h(h)}</th>")
            parts.append("</tr></thead><tbody>\n")
            for row in t["sample_data_rows"]:
                parts.append("<tr>")
                for cell in row:
                    parts.append(f'<td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{_h(cell)}</td>')
                parts.append("</tr>\n")
            parts.append("</tbody></table></div>\n")

        parts.append("</div></details>\n")

    parts.append("</div>\n")

    # ===================================================================
    # SECTION 5: Relationship Summary Table
    # ===================================================================
    parts.append("""
<div class="section" id="relationships">
  <div class="section-title"><span class="icon rel">&#8644;</span> Relationship Summary</div>
  <input type="text" class="search-box" id="relSearch" placeholder="Search relationships..." onkeyup="filterRelTable()">
  <div class="card">
    <div class="table-wrapper">
      <table class="data-table" id="relTable">
        <thead><tr><th>#</th><th>Source Table</th><th>Column</th><th>Target Table</th><th>Column</th><th>Constraint</th></tr></thead>
        <tbody>
""")
    for i, rel in enumerate(sorted(relationships, key=lambda r: r["source_table"]), 1):
        parts.append(
            f'<tr><td>{i}</td><td>{_h(rel["source_table"])}</td><td>{_h(rel["source_column"])}</td>'
            f'<td>{_h(rel["target_table"])}</td><td>{_h(rel["target_column"])}</td>'
            f'<td style="font-size:0.78rem;color:#8892b0;">{_h(rel["constraint"])}</td></tr>\n'
        )
    if not relationships:
        parts.append('<tr><td colspan="6" style="text-align:center;color:#555;padding:20px;">No foreign key relationships found</td></tr>\n')
    parts.append("""
        </tbody>
      </table>
    </div>
  </div>
</div>
""")

    # ===================================================================
    # SECTION 6: Data Volume Charts
    # ===================================================================
    vol_labels = json.dumps([t["short_name"] for t in table_names_sorted])
    vol_rows = json.dumps([t["row_count"] for t in table_names_sorted])
    vol_sizes = json.dumps([round(t["total_size_bytes"] / 1024, 1) for t in table_names_sorted])
    vol_colors = json.dumps([
        '#e94560', '#0f3460', '#4caf50', '#ff9800',
        '#9c27b0', '#00bcd4', '#3f51b5', '#ff5722',
        '#8bc34a', '#607d8b', '#e91e63', '#009688',
    ][:len(tables)])

    parts.append(f"""
<div class="section" id="data-volume">
  <div class="section-title"><span class="icon vol">&#9638;</span> Data Volume</div>
  <div class="chart-grid">
    <div class="chart-card">
      <h3>Row Count by Table</h3>
      <canvas id="rowCountChart"></canvas>
    </div>
    <div class="chart-card">
      <h3>Size Distribution (kB)</h3>
      <canvas id="sizeDistChart"></canvas>
    </div>
  </div>
</div>
""")

    # ===================================================================
    # SECTION 7: Schema Lineage Flow (Mermaid)
    # ===================================================================
    # Build dependency chains from root tables to leaf tables
    # using topological order based on FK references.
    adjacency: dict[str, set[str]] = defaultdict(set)
    for rel in relationships:
        src_short = rel["source_table"].split(".")[-1]
        tgt_short = rel["target_table"].split(".")[-1]
        adjacency[src_short].add(tgt_short)

    # Compute depth levels via BFS from roots
    all_shorts = {t["short_name"] for t in tables}
    referenced_by_others = set()
    for rel in relationships:
        referenced_by_others.add(rel["source_table"].split(".")[-1])
    roots = all_shorts - referenced_by_others  # tables that are never the "source" side of an FK
    # Actually roots = tables with no outgoing FK (they don't reference anyone)
    roots = set()
    for t in tables:
        if not t["fk_outgoing"]:
            roots.add(t["short_name"])

    parts.append("""
<div class="section" id="schema-lineage">
  <div class="section-title"><span class="icon flow">&#8615;</span> Schema Lineage Flow</div>
  <div class="card mermaid-card">
    <pre class="mermaid">
graph TD
""")

    # Style root nodes
    for r in sorted(roots):
        parts.append(f'    {r}["{r}"]\n')
        parts.append(f'    style {r} fill:#2e7d32,stroke:#4caf50,color:#fff\n')

    # Build reverse adjacency: for each table, which tables reference it?
    # We want flow: root -> tables that reference root -> tables that reference those
    reverse_adj: dict[str, set[str]] = defaultdict(set)
    for rel in relationships:
        src_short = rel["source_table"].split(".")[-1]
        tgt_short = rel["target_table"].split(".")[-1]
        # src references tgt, so data flows tgt -> src in lineage
        reverse_adj[tgt_short].add(src_short)

    # Emit edges following data lineage (from referenced to referencer)
    emitted_edges: set[tuple[str, str]] = set()
    for rel in relationships:
        src_short = rel["source_table"].split(".")[-1]
        tgt_short = rel["target_table"].split(".")[-1]
        edge = (tgt_short, src_short)
        if edge not in emitted_edges:
            emitted_edges.add(edge)
            parts.append(f'    {tgt_short} --> {src_short}\n')

    # Style leaf nodes (no incoming FK = nobody references them)
    leaves = set()
    for t in tables:
        if not t["fk_incoming"]:
            if t["fk_outgoing"]:  # it references others but nobody references it
                leaves.add(t["short_name"])
    for lf in sorted(leaves):
        parts.append(f'    style {lf} fill:#e65100,stroke:#ff9800,color:#fff\n')

    parts.append("""    </pre>
  </div>
</div>
""")

    # ===================================================================
    # SECTION 8: Recommendations
    # ===================================================================
    parts.append("""
<div class="section" id="recommendations">
  <div class="section-title"><span class="icon rec">&#9998;</span> Recommendations</div>
  <div class="card">
""")
    if recommendations:
        for rec in recommendations:
            parts.append(f'    <div class="rec-item">{rec}</div>\n')
    else:
        parts.append('    <div style="text-align:center;color:#4caf50;padding:20px;font-size:1rem;">No issues detected. Schema quality is excellent.</div>\n')
    parts.append("  </div>\n</div>\n")

    # ===================================================================
    # SECTION 9: Footer
    # ===================================================================
    parts.append(f"""
<div class="footer">
  <p>Generated by <strong>SchemaAnalyzer</strong> v{_TOOL_VERSION}</p>
  <p>Source: <strong>{_h(source_name)}</strong> &bull; Tables: {total_tables} &bull; Timestamp: {_h(timestamp)}</p>
  <p style="margin-top:8px;">Report is self-contained. Visualisations powered by Mermaid and Chart.js (loaded from CDN).</p>
</div>
""")

    # ===================================================================
    # JavaScript: Mermaid init + Chart.js charts + search
    # ===================================================================
    parts.append(f"""
<script>
// Mermaid init
mermaid.initialize({{
    startOnLoad: true,
    theme: 'dark',
    themeVariables: {{
        primaryColor: '#0f3460',
        primaryTextColor: '#e0e0e0',
        primaryBorderColor: '#e94560',
        lineColor: '#e94560',
        secondaryColor: '#16213e',
        tertiaryColor: '#1a1a2e',
        fontFamily: 'Segoe UI, sans-serif',
        fontSize: '14px',
        edgeLabelBackground: '#16213e'
    }},
    er: {{
        useMaxWidth: true,
        layoutDirection: 'TB'
    }},
    flowchart: {{
        useMaxWidth: true,
        htmlLabels: true,
        curve: 'basis'
    }}
}});

// Chart.js defaults
Chart.defaults.color = '#8892b0';
Chart.defaults.borderColor = 'rgba(255,255,255,0.06)';
Chart.defaults.font.family = "'Segoe UI', sans-serif";

// Quality Score Bar Chart
new Chart(document.getElementById('qualityBarChart'), {{
    type: 'bar',
    data: {{
        labels: {qs_labels},
        datasets: [{{
            label: 'Quality Score',
            data: {qs_values},
            backgroundColor: {qs_colors},
            borderRadius: 6,
            maxBarThickness: 60
        }}]
    }},
    options: {{
        responsive: true,
        plugins: {{
            legend: {{ display: false }},
            tooltip: {{
                callbacks: {{
                    label: function(ctx) {{ return ctx.parsed.y + '/100'; }}
                }}
            }}
        }},
        scales: {{
            y: {{
                beginAtZero: true,
                max: 100,
                grid: {{ color: 'rgba(255,255,255,0.04)' }},
                ticks: {{ stepSize: 20 }}
            }},
            x: {{
                grid: {{ display: false }}
            }}
        }}
    }}
}});

// Severity Doughnut
new Chart(document.getElementById('severityDoughnut'), {{
    type: 'doughnut',
    data: {{
        labels: {sev_labels},
        datasets: [{{
            data: {sev_values},
            backgroundColor: {sev_colors},
            borderWidth: 0,
            hoverOffset: 8
        }}]
    }},
    options: {{
        responsive: true,
        cutout: '60%',
        plugins: {{
            legend: {{
                position: 'bottom',
                labels: {{ padding: 16, usePointStyle: true, pointStyle: 'circle' }}
            }}
        }}
    }}
}});

// Row Count Horizontal Bar
new Chart(document.getElementById('rowCountChart'), {{
    type: 'bar',
    data: {{
        labels: {vol_labels},
        datasets: [{{
            label: 'Rows',
            data: {vol_rows},
            backgroundColor: '#e94560',
            borderRadius: 6,
            maxBarThickness: 40
        }}]
    }},
    options: {{
        indexAxis: 'y',
        responsive: true,
        plugins: {{
            legend: {{ display: false }},
            tooltip: {{
                callbacks: {{
                    label: function(ctx) {{ return ctx.parsed.x.toLocaleString() + ' rows'; }}
                }}
            }}
        }},
        scales: {{
            x: {{
                beginAtZero: true,
                grid: {{ color: 'rgba(255,255,255,0.04)' }},
                ticks: {{
                    callback: function(v) {{ return v.toLocaleString(); }}
                }}
            }},
            y: {{
                grid: {{ display: false }}
            }}
        }}
    }}
}});

// Size Distribution Pie
new Chart(document.getElementById('sizeDistChart'), {{
    type: 'pie',
    data: {{
        labels: {vol_labels},
        datasets: [{{
            data: {vol_sizes},
            backgroundColor: {vol_colors},
            borderWidth: 0,
            hoverOffset: 10
        }}]
    }},
    options: {{
        responsive: true,
        plugins: {{
            legend: {{
                position: 'bottom',
                labels: {{ padding: 14, usePointStyle: true, pointStyle: 'circle' }}
            }},
            tooltip: {{
                callbacks: {{
                    label: function(ctx) {{ return ctx.label + ': ' + ctx.parsed + ' kB'; }}
                }}
            }}
        }}
    }}
}});

// Relationship search filter
function filterRelTable() {{
    var input = document.getElementById('relSearch').value.toLowerCase();
    var rows = document.querySelectorAll('#relTable tbody tr');
    rows.forEach(function(row) {{
        var text = row.textContent.toLowerCase();
        row.style.display = text.includes(input) ? '' : 'none';
    }});
}}
</script>
""")

    # Close
    parts.append("</div><!-- /.container -->\n</body>\n</html>\n")

    html_content = "".join(parts)

    # Write to disk
    if output_path is None:
        output_path = source_dir / "report.html"
    else:
        output_path = Path(output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html_content, encoding="utf-8")
    print(f"[report] Wrote HTML report to {output_path} ({len(html_content):,} bytes)", file=sys.stderr)

    return html_content


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    source_dir = sys.argv[1] if len(sys.argv) > 1 else "output/sources/jhonson_pharma"
    source_name = sys.argv[2] if len(sys.argv) > 2 else "jhonson_pharma"
    output = sys.argv[3] if len(sys.argv) > 3 else None
    generate_html_report(source_dir, source_name, output)
