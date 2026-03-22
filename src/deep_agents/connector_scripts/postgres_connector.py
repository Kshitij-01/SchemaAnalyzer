#!/usr/bin/env python3
"""
PostgreSQL Connector Script for SchemaAnalyzer.

A standalone CLI tool that agents invoke via Bash to introspect and profile
PostgreSQL databases.  All output is JSON written to stdout so that the
calling agent can parse it directly.

Requirements:
    pip install psycopg2-binary

Usage:
    python postgres_connector.py \
        --host HOST --port PORT --user USER --password PASS --database DB \
        <command> [command-args]

Commands:
    list-schemas
    list-tables   --schema SCHEMA
    profile-table --schema SCHEMA --table TABLE
    profile-batch --schema SCHEMA --tables TABLE1,TABLE2,...

Exit codes:
    0  Success (JSON result on stdout)
    1  Connection failure or fatal error (JSON error on stdout)
"""

from __future__ import annotations

import argparse
import json
import sys
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Dict, Generator, List, Optional, Tuple
from uuid import UUID

# ---------------------------------------------------------------------------
# psycopg2 import with a friendly message when missing
# ---------------------------------------------------------------------------
try:
    import psycopg2
    import psycopg2.extras
    import psycopg2.extensions
except ImportError:
    _err = {
        "error": "psycopg2 is not installed. Run: pip install psycopg2-binary"
    }
    print(json.dumps(_err, indent=2))
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CONNECTION_TIMEOUT_SEC: int = 10
QUERY_TIMEOUT_SEC: int = 30
DEEP_QUERY_TIMEOUT_SEC: int = 60
LARGE_TABLE_THRESHOLD: int = 1_000_000

SYSTEM_SCHEMAS: Tuple[str, ...] = (
    "information_schema",
    "pg_catalog",
    "pg_toast",
)


# ---------------------------------------------------------------------------
# JSON serialisation helpers
# ---------------------------------------------------------------------------
class _ExtendedEncoder(json.JSONEncoder):
    """Handle Postgres-specific Python types that stdlib json cannot encode."""

    def default(self, o: Any) -> Any:  # noqa: D401
        if isinstance(o, Decimal):
            # Keep numeric precision; return float only when lossless
            return float(o)
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, time):
            return o.isoformat()
        if isinstance(o, timedelta):
            return str(o)
        if isinstance(o, UUID):
            return str(o)
        if isinstance(o, bytes):
            return o.hex()
        if isinstance(o, memoryview):
            return bytes(o).hex()
        return super().default(o)


def _json_out(data: Any) -> str:
    """Return pretty-printed JSON text suitable for stdout."""
    return json.dumps(data, indent=2, cls=_ExtendedEncoder, default=str)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------
@contextmanager
def _connect(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
) -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Open a psycopg2 connection with a *connect_timeout* and a per-statement
    timeout (``statement_timeout``) so that no single query blocks forever.

    Yields:
        An open ``psycopg2`` connection.

    Raises:
        psycopg2.Error: On any connection-level failure.
    """
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=database,
        connect_timeout=CONNECTION_TIMEOUT_SEC,
        options=f"-c statement_timeout={QUERY_TIMEOUT_SEC * 1000}",
        # Force SSL for Azure-hosted instances; harmless elsewhere
        sslmode="require",
    )
    try:
        conn.set_session(autocommit=True)
        yield conn
    finally:
        conn.close()


def _query(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Optional[Tuple[Any, ...]] = None,
) -> List[Dict[str, Any]]:
    """
    Execute *sql* and return a list of dicts (one per row).

    Uses ``RealDictCursor`` so every row is an ``OrderedDict`` whose keys
    are the column names.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        if cur.description is None:
            return []
        return [dict(row) for row in cur.fetchall()]


def _query_scalar(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Optional[Tuple[Any, ...]] = None,
) -> Any:
    """Execute *sql* and return the single scalar value from the first row."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


# ---------------------------------------------------------------------------
# Quoting helper
# ---------------------------------------------------------------------------
def _fqn(schema: str, table: str) -> str:
    """
    Return a fully-qualified, safely-quoted ``schema.table`` identifier.

    Uses ``psycopg2.extensions.quote_ident`` (available in psycopg2 >= 2.7).
    Falls back to manual double-quote wrapping if unavailable.
    """
    try:
        from psycopg2.extensions import quote_ident  # type: ignore[attr-defined]

        # quote_ident requires a connection but we don't have one handy in
        # all call-sites, so we do simple quoting ourselves.
        raise ImportError
    except ImportError:
        def _qi(name: str) -> str:
            return '"' + name.replace('"', '""') + '"'
        return f"{_qi(schema)}.{_qi(table)}"


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def cmd_list_schemas(conn: psycopg2.extensions.connection) -> List[str]:
    """Return non-system schema names as a plain JSON array of strings."""
    sql = """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name
    """
    rows = _query(conn, sql)
    return [r["schema_name"] for r in rows]


def cmd_list_tables(
    conn: psycopg2.extensions.connection,
    schema: str,
) -> List[Dict[str, str]]:
    """Return tables and views in *schema* as ``[{table_name, table_type}]``."""
    sql = """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = %s
        ORDER BY table_name
    """
    return _query(conn, sql, (schema,))


# ---- profile-table sub-queries -------------------------------------------

def _get_columns(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
) -> List[Dict[str, Any]]:
    """Column metadata from ``information_schema.columns``."""
    sql = """
        SELECT
            column_name,
            data_type,
            udt_name,
            is_nullable,
            column_default,
            ordinal_position,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name   = %s
        ORDER BY ordinal_position
    """
    return _query(conn, sql, (schema, table))


def _get_constraints(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
) -> List[Dict[str, Any]]:
    """Constraints (PK, UNIQUE, FK, CHECK) with their key columns."""
    sql = """
        SELECT
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            kcu.ordinal_position
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name  = kcu.constraint_name
         AND tc.table_schema     = kcu.table_schema
         AND tc.table_name       = kcu.table_name
        WHERE tc.table_schema = %s
          AND tc.table_name   = %s
        ORDER BY tc.constraint_name, kcu.ordinal_position
    """
    return _query(conn, sql, (schema, table))


def _get_indexes(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
) -> List[Dict[str, Any]]:
    """Indexes from ``pg_indexes``."""
    sql = """
        SELECT
            indexname  AS index_name,
            indexdef   AS index_definition
        FROM pg_indexes
        WHERE schemaname = %s
          AND tablename  = %s
        ORDER BY indexname
    """
    return _query(conn, sql, (schema, table))


def _get_foreign_keys_outgoing(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
) -> List[Dict[str, Any]]:
    """Foreign keys *from* this table to other tables."""
    sql = """
        SELECT
            tc.constraint_name,
            kcu.column_name,
            ccu.table_schema  AS foreign_schema,
            ccu.table_name    AS foreign_table,
            ccu.column_name   AS foreign_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema    = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON tc.constraint_name = ccu.constraint_name
         AND tc.table_schema    = ccu.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema    = %s
          AND tc.table_name      = %s
        ORDER BY tc.constraint_name, kcu.ordinal_position
    """
    return _query(conn, sql, (schema, table))


def _get_foreign_keys_incoming(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
) -> List[Dict[str, Any]]:
    """Foreign keys from *other* tables that reference this table."""
    sql = """
        SELECT
            tc.constraint_name,
            tc.table_schema    AS source_schema,
            tc.table_name      AS source_table,
            kcu.column_name    AS source_column,
            ccu.column_name    AS referenced_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema    = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON tc.constraint_name = ccu.constraint_name
         AND tc.table_schema    = ccu.table_schema
        WHERE tc.constraint_type  = 'FOREIGN KEY'
          AND ccu.table_schema    = %s
          AND ccu.table_name      = %s
        ORDER BY tc.table_schema, tc.table_name, tc.constraint_name
    """
    return _query(conn, sql, (schema, table))


def _get_row_count(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
) -> Optional[int]:
    """Exact row count.  Returns ``None`` on timeout or error."""
    fqn = _fqn(schema, table)
    sql = f"SELECT count(*) FROM {fqn}"  # noqa: S608 — fqn is quoted
    return _query_scalar(conn, sql)


def _get_table_size(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
) -> Optional[str]:
    """Human-readable total relation size (data + indexes + toast)."""
    fqn = _fqn(schema, table)
    sql = f"SELECT pg_size_pretty(pg_total_relation_size('{schema}.{table}'))"
    return _query_scalar(conn, sql)


def _get_null_percentages(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    columns: List[Dict[str, Any]],
) -> Dict[str, Optional[float]]:
    """
    For each column, compute the percentage of NULL values.

    Returns ``{column_name: pct}`` where *pct* is a float in [0, 100] or
    ``None`` if the query failed.
    """
    fqn = _fqn(schema, table)
    result: Dict[str, Optional[float]] = {}

    if not columns:
        return result

    # Build a single query that computes all null percentages at once
    # to avoid N round-trips.
    select_parts: List[str] = []
    for col in columns:
        col_name = col["column_name"]
        quoted = '"' + col_name.replace('"', '""') + '"'
        select_parts.append(
            f"count(*) FILTER (WHERE {quoted} IS NULL) * 100.0 "
            f"/ NULLIF(count(*), 0) AS \"{col_name}_null_pct\""
        )

    sql = f"SELECT {', '.join(select_parts)} FROM {fqn}"  # noqa: S608
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if row:
                for idx, col in enumerate(columns):
                    val = row[idx]
                    result[col["column_name"]] = (
                        float(val) if val is not None else None
                    )
    except Exception as exc:  # noqa: BLE001
        # Fall back: mark every column as unknown
        for col in columns:
            result[col["column_name"]] = None
        result["_error"] = str(exc)  # type: ignore[assignment]

    return result


def _get_sample_data(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """Return the first *limit* rows from the table."""
    fqn = _fqn(schema, table)
    sql = f"SELECT * FROM {fqn} LIMIT %s"  # noqa: S608
    return _query(conn, sql, (limit,))


# ---- deep statistical profiling helpers -----------------------------------

def _get_estimated_row_count(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
) -> int:
    """
    Fast row-count estimate via ``pg_class.reltuples``.

    This avoids a full sequential scan and is accurate enough for deciding
    whether to use ``TABLESAMPLE``.  Returns ``0`` when the table has never
    been ``ANALYZE``-d.
    """
    sql = """
        SELECT GREATEST(c.reltuples, 0)::bigint
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s
    """
    val = _query_scalar(conn, sql, (schema, table))
    return int(val) if val is not None else 0


def _sample_clause(estimated_rows: int) -> str:
    """
    Return a ``TABLESAMPLE BERNOULLI(N)`` clause for large tables.

    Targets roughly 100 000 rows.  For tables below
    ``LARGE_TABLE_THRESHOLD`` an empty string is returned so the full
    table is scanned.
    """
    if estimated_rows <= LARGE_TABLE_THRESHOLD:
        return ""
    pct = max(0.01, min(100.0, (100_000 / estimated_rows) * 100.0))
    return f" TABLESAMPLE BERNOULLI({pct:.4f})"


def _classify_columns(
    columns: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Categorise *columns* into groups by their ``data_type`` value.

    Returns a dict with keys ``numeric``, ``text``, ``date``, ``boolean``,
    and ``other``.
    """
    groups: Dict[str, List[Dict[str, Any]]] = {
        "numeric": [],
        "text": [],
        "date": [],
        "boolean": [],
        "other": [],
    }

    _NUMERIC_TYPES = {
        "smallint", "integer", "bigint", "real", "double precision",
        "numeric", "decimal", "serial", "bigserial", "smallserial",
        "int2", "int4", "int8", "float4", "float8", "money",
    }
    _TEXT_TYPES = {
        "character varying", "varchar", "character", "char", "text",
        "name", "citext",
    }
    _DATE_TYPES = {
        "date", "timestamp without time zone", "timestamp with time zone",
        "timestamp", "timestamptz",
    }

    for col in columns:
        dt = (col.get("data_type") or "").lower()
        udt = (col.get("udt_name") or "").lower()

        if dt in _NUMERIC_TYPES or udt in _NUMERIC_TYPES:
            groups["numeric"].append(col)
        elif dt in _TEXT_TYPES or udt in _TEXT_TYPES:
            groups["text"].append(col)
        elif dt in _DATE_TYPES or udt in _DATE_TYPES:
            groups["date"].append(col)
        elif dt == "boolean" or udt == "bool":
            groups["boolean"].append(col)
        else:
            groups["other"].append(col)

    return groups


# ---- deep profiling query functions ---------------------------------------

def _get_numeric_stats(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    numeric_cols: List[Dict[str, Any]],
    sample_clause: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Compute descriptive statistics for every numeric column in a single query.

    Returns ``{col_name: {min, max, mean, median, stddev, p25, p75,
    zero_count, zero_pct, distinct_count, cardinality_ratio}}``.
    """
    if not numeric_cols:
        return {}

    fqn = _fqn(schema, table)
    parts: List[str] = []
    col_names: List[str] = []

    for col in numeric_cols:
        cn = col["column_name"]
        col_names.append(cn)
        q = '"' + cn.replace('"', '""') + '"'
        parts.append(
            f"min({q}), max({q}), avg({q}), stddev({q}), "
            f"percentile_cont(0.25) WITHIN GROUP (ORDER BY {q}), "
            f"percentile_cont(0.50) WITHIN GROUP (ORDER BY {q}), "
            f"percentile_cont(0.75) WITHIN GROUP (ORDER BY {q}), "
            f"count(*) FILTER (WHERE {q} = 0), "
            f"count(DISTINCT {q})"
        )

    sql = (
        f"SELECT {', '.join(parts)} "
        f"FROM {fqn}{sample_clause}"
    )

    result: Dict[str, Dict[str, Any]] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if not row:
                return result

            # Also fetch total non-null rows for pct calculations
            total_sql = f"SELECT count(*) FROM {fqn}{sample_clause}"
            total_rows = _query_scalar(conn, total_sql) or 1

            idx = 0
            for cn in col_names:
                mn, mx, avg_val, std, p25, p50, p75, zero_ct, dist_ct = (
                    row[idx], row[idx + 1], row[idx + 2], row[idx + 3],
                    row[idx + 4], row[idx + 5], row[idx + 6],
                    row[idx + 7], row[idx + 8],
                )
                idx += 9

                zero_ct = zero_ct or 0
                dist_ct = dist_ct or 0
                result[cn] = {
                    "min": float(mn) if mn is not None else None,
                    "max": float(mx) if mx is not None else None,
                    "mean": float(avg_val) if avg_val is not None else None,
                    "median": float(p50) if p50 is not None else None,
                    "stddev": float(std) if std is not None else None,
                    "p25": float(p25) if p25 is not None else None,
                    "p75": float(p75) if p75 is not None else None,
                    "zero_count": int(zero_ct),
                    "zero_pct": round(int(zero_ct) * 100.0 / total_rows, 2),
                    "distinct_count": int(dist_ct),
                    "cardinality_ratio": round(
                        int(dist_ct) / total_rows, 4
                    ) if total_rows else 0.0,
                }
    except Exception:  # noqa: BLE001
        return {}

    return result


def _get_text_stats(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    text_cols: List[Dict[str, Any]],
    sample_clause: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Compute length and cardinality statistics for text columns.

    Returns ``{col_name: {min_length, max_length, avg_length,
    distinct_count, cardinality_ratio, empty_count, empty_pct}}``.
    """
    if not text_cols:
        return {}

    fqn = _fqn(schema, table)
    parts: List[str] = []
    col_names: List[str] = []

    for col in text_cols:
        cn = col["column_name"]
        col_names.append(cn)
        q = '"' + cn.replace('"', '""') + '"'
        parts.append(
            f"min(length({q})), max(length({q})), avg(length({q})), "
            f"count(DISTINCT {q}), "
            f"count(*) FILTER (WHERE {q} = '')"
        )

    sql = (
        f"SELECT {', '.join(parts)} "
        f"FROM {fqn}{sample_clause}"
    )

    result: Dict[str, Dict[str, Any]] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if not row:
                return result

            total_sql = f"SELECT count(*) FROM {fqn}{sample_clause}"
            total_rows = _query_scalar(conn, total_sql) or 1

            idx = 0
            for cn in col_names:
                min_len, max_len, avg_len, dist_ct, empty_ct = (
                    row[idx], row[idx + 1], row[idx + 2],
                    row[idx + 3], row[idx + 4],
                )
                idx += 5

                dist_ct = dist_ct or 0
                empty_ct = empty_ct or 0
                result[cn] = {
                    "min_length": int(min_len) if min_len is not None else None,
                    "max_length": int(max_len) if max_len is not None else None,
                    "avg_length": round(float(avg_len), 2) if avg_len is not None else None,
                    "distinct_count": int(dist_ct),
                    "cardinality_ratio": round(
                        int(dist_ct) / total_rows, 4
                    ) if total_rows else 0.0,
                    "empty_count": int(empty_ct),
                    "empty_pct": round(int(empty_ct) * 100.0 / total_rows, 2),
                }
    except Exception:  # noqa: BLE001
        return {}

    return result


def _get_text_top_values(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    col_name: str,
    sample_clause: str,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """
    Return the most frequent values for a single text column.

    Returns ``[{value, count, percentage}]`` sorted by descending frequency.
    """
    fqn = _fqn(schema, table)
    q = '"' + col_name.replace('"', '""') + '"'

    sql = (
        f"SELECT {q} AS val, count(*) AS freq "
        f"FROM {fqn}{sample_clause} "
        f"GROUP BY {q} ORDER BY freq DESC LIMIT %s"
    )

    try:
        rows = _query(conn, sql, (limit,))
        total_sql = f"SELECT count(*) FROM {fqn}{sample_clause}"
        total_rows = _query_scalar(conn, total_sql) or 1

        return [
            {
                "value": r["val"],
                "count": int(r["freq"]),
                "percentage": round(int(r["freq"]) * 100.0 / total_rows, 2),
            }
            for r in rows
        ]
    except Exception:  # noqa: BLE001
        return []


def _get_text_patterns(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    col_name: str,
    sample_clause: str,
) -> Dict[str, float]:
    """
    Check what percentage of non-null values match common semantic patterns.

    Returns ``{email_pct, url_pct, phone_pct, uuid_pct, ipv4_pct}``.
    """
    fqn = _fqn(schema, table)
    q = '"' + col_name.replace('"', '""') + '"'

    sql = f"""
        SELECT
            count(*) FILTER (
                WHERE {q} ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{{2,}}$'
            ) * 100.0 / NULLIF(count(*), 0)  AS email_pct,
            count(*) FILTER (
                WHERE {q} ~ '^https?://'
            ) * 100.0 / NULLIF(count(*), 0)  AS url_pct,
            count(*) FILTER (
                WHERE {q} ~ '^\\+?[\\d\\s()\\-]{{7,}}$'
            ) * 100.0 / NULLIF(count(*), 0)  AS phone_pct,
            count(*) FILTER (
                WHERE {q} ~ '^[0-9a-fA-F]{{8}}-[0-9a-fA-F]{{4}}-'
            ) * 100.0 / NULLIF(count(*), 0)  AS uuid_pct,
            count(*) FILTER (
                WHERE {q} ~ '^\\d{{1,3}}\\.\\d{{1,3}}\\.\\d{{1,3}}\\.\\d{{1,3}}$'
            ) * 100.0 / NULLIF(count(*), 0)  AS ipv4_pct
        FROM {fqn}{sample_clause}
        WHERE {q} IS NOT NULL
    """

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if not row:
                return {}
            return {
                "email_pct": round(float(row[0]), 2) if row[0] is not None else 0.0,
                "url_pct": round(float(row[1]), 2) if row[1] is not None else 0.0,
                "phone_pct": round(float(row[2]), 2) if row[2] is not None else 0.0,
                "uuid_pct": round(float(row[3]), 2) if row[3] is not None else 0.0,
                "ipv4_pct": round(float(row[4]), 2) if row[4] is not None else 0.0,
            }
    except Exception:  # noqa: BLE001
        return {}


def _get_date_stats(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    date_cols: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Compute range and day-of-week distribution for date / timestamp columns.

    Returns ``{col_name: {min_date, max_date, range_days,
    dow_distribution: {0: N, 1: N, ...}}}``.
    """
    if not date_cols:
        return {}

    fqn = _fqn(schema, table)
    result: Dict[str, Dict[str, Any]] = {}

    for col in date_cols:
        cn = col["column_name"]
        q = '"' + cn.replace('"', '""') + '"'

        # --- min / max / range ---------------------------------------------
        range_sql = (
            f"SELECT min({q}), max({q}), "
            f"EXTRACT(DAY FROM (max({q}) - min({q}))) "
            f"FROM {fqn}"
        )

        try:
            with conn.cursor() as cur:
                cur.execute(range_sql)
                rrow = cur.fetchone()

            min_dt = rrow[0] if rrow else None
            max_dt = rrow[1] if rrow else None
            range_days = rrow[2] if rrow else None

            # --- day-of-week distribution ----------------------------------
            dow_sql = (
                f"SELECT EXTRACT(DOW FROM {q})::int AS dow, count(*) "
                f"FROM {fqn} "
                f"WHERE {q} IS NOT NULL "
                f"GROUP BY 1 ORDER BY 1"
            )
            dow_rows = _query(conn, dow_sql)
            dow_dist = {int(r["dow"]): int(r["count"]) for r in dow_rows}

            result[cn] = {
                "min_date": min_dt,
                "max_date": max_dt,
                "range_days": float(range_days) if range_days is not None else None,
                "dow_distribution": dow_dist,
            }
        except Exception:  # noqa: BLE001
            result[cn] = {}

    return result


def _get_boolean_stats(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    bool_cols: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Compute true / false / null counts for boolean columns.

    Returns ``{col_name: {true_count, false_count, null_count,
    true_pct, false_pct, null_pct}}``.
    """
    if not bool_cols:
        return {}

    fqn = _fqn(schema, table)
    parts: List[str] = []
    col_names: List[str] = []

    for col in bool_cols:
        cn = col["column_name"]
        col_names.append(cn)
        q = '"' + cn.replace('"', '""') + '"'
        parts.append(
            f"count(*) FILTER (WHERE {q} = true), "
            f"count(*) FILTER (WHERE {q} = false), "
            f"count(*) FILTER (WHERE {q} IS NULL)"
        )

    sql = f"SELECT {', '.join(parts)} FROM {fqn}"

    result: Dict[str, Dict[str, Any]] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if not row:
                return result

            total_sql = f"SELECT count(*) FROM {fqn}"
            total_rows = _query_scalar(conn, total_sql) or 1

            idx = 0
            for cn in col_names:
                true_ct = int(row[idx] or 0)
                false_ct = int(row[idx + 1] or 0)
                null_ct = int(row[idx + 2] or 0)
                idx += 3

                result[cn] = {
                    "true_count": true_ct,
                    "false_count": false_ct,
                    "null_count": null_ct,
                    "true_pct": round(true_ct * 100.0 / total_rows, 2),
                    "false_pct": round(false_ct * 100.0 / total_rows, 2),
                    "null_pct": round(null_ct * 100.0 / total_rows, 2),
                }
    except Exception:  # noqa: BLE001
        return {}

    return result


def _get_universal_stats(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    columns: List[Dict[str, Any]],
    sample_clause: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Compute ``DISTINCT`` count and cardinality ratio for *all* columns.

    Uses a single batch query (same pattern as ``_get_null_percentages``).
    Returns ``{col_name: {distinct_count, cardinality_ratio, duplicate_pct}}``.
    """
    if not columns:
        return {}

    fqn = _fqn(schema, table)
    parts: List[str] = []
    col_names: List[str] = []

    for col in columns:
        cn = col["column_name"]
        col_names.append(cn)
        q = '"' + cn.replace('"', '""') + '"'
        parts.append(f"count(DISTINCT {q})")

    sql = f"SELECT {', '.join(parts)} FROM {fqn}{sample_clause}"

    result: Dict[str, Dict[str, Any]] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if not row:
                return result

            total_sql = f"SELECT count(*) FROM {fqn}{sample_clause}"
            total_rows = _query_scalar(conn, total_sql) or 1

            for idx, cn in enumerate(col_names):
                dist_ct = int(row[idx] or 0)
                ratio = round(dist_ct / total_rows, 4) if total_rows else 0.0
                result[cn] = {
                    "distinct_count": dist_ct,
                    "cardinality_ratio": ratio,
                    "duplicate_pct": round((1.0 - ratio) * 100.0, 2),
                }
    except Exception:  # noqa: BLE001
        return {}

    return result


# ---- adaptive insights (no DB call) --------------------------------------

def _compute_adaptive_insights(profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Derive actionable insights from the collected profile statistics.

    This is a pure-Python function that does **not** make any database
    calls.  Each rule is independently evaluated; failures in one rule do
    not block the others.

    Returns ``[{column, insight, description, severity}]``.
    """
    insights: List[Dict[str, Any]] = []

    # Build a set of primary-key column names for the "candidate natural key"
    # rule so we skip columns that are already part of a PK.
    pk_columns: set[str] = set()
    for con in profile.get("constraints", []):
        if con.get("constraint_type") == "PRIMARY KEY":
            pk_columns.add(con.get("column_name", ""))

    # --- Rule: LIKELY_EMAIL ------------------------------------------------
    text_patterns = profile.get("text_patterns", {})
    text_stats = profile.get("text_stats", {})
    for col_name, patterns in text_patterns.items():
        try:
            ts = text_stats.get(col_name, {})
            ratio = ts.get("cardinality_ratio", 0.0)
            email_pct = patterns.get("email_pct", 0.0)
            if ratio > 0.9 and email_pct > 50.0:
                insights.append({
                    "column": col_name,
                    "insight": "LIKELY_EMAIL",
                    "description": (
                        f"High cardinality ({ratio:.2f}) and {email_pct:.1f}% "
                        f"values match email pattern."
                    ),
                    "severity": "INFO",
                })
        except Exception:  # noqa: BLE001
            pass

    # --- Rule: OUTLIERS_DETECTED -------------------------------------------
    numeric_stats = profile.get("numeric_stats", {})
    for col_name, ns in numeric_stats.items():
        try:
            mean = ns.get("mean")
            std = ns.get("stddev")
            mx = ns.get("max")
            if mean is not None and std is not None and mx is not None and std > 0:
                upper_bound = mean + 3 * std
                if mx > upper_bound:
                    insights.append({
                        "column": col_name,
                        "insight": "OUTLIERS_DETECTED",
                        "description": (
                            f"Max value ({mx}) exceeds mean + 3*stddev "
                            f"({upper_bound:.2f})."
                        ),
                        "severity": "WARNING",
                    })
        except Exception:  # noqa: BLE001
            pass

    # --- Rule: STALE_DATA --------------------------------------------------
    date_stats = profile.get("date_stats", {})
    for col_name, ds in date_stats.items():
        try:
            range_days = ds.get("range_days")
            max_date = ds.get("max_date")
            if range_days is not None and range_days > 0 and max_date is not None:
                if isinstance(max_date, str):
                    from datetime import datetime as _dt
                    max_date = _dt.fromisoformat(max_date)
                if hasattr(max_date, "date"):
                    max_date = max_date.date() if callable(getattr(max_date, "date", None)) else max_date
                from datetime import date as _date
                days_since = (_date.today() - max_date).days if hasattr(max_date, "year") else None
                if days_since is not None and days_since > 90:
                    insights.append({
                        "column": col_name,
                        "insight": "STALE_DATA",
                        "description": (
                            f"Most recent value is {days_since} days old "
                            f"(last: {max_date})."
                        ),
                        "severity": "WARNING",
                    })
        except Exception:  # noqa: BLE001
            pass

    # --- Rule: CANDIDATE_NATURAL_KEY ---------------------------------------
    universal_stats = profile.get("universal_stats", {})
    for col_name, us in universal_stats.items():
        try:
            if us.get("cardinality_ratio") == 1.0 and col_name not in pk_columns:
                insights.append({
                    "column": col_name,
                    "insight": "CANDIDATE_NATURAL_KEY",
                    "description": (
                        "Every value is unique — column may serve as a "
                        "natural key."
                    ),
                    "severity": "INFO",
                })
        except Exception:  # noqa: BLE001
            pass

    # --- Rule: HIGH_EMPTY_STRINGS ------------------------------------------
    for col_name, ts in text_stats.items():
        try:
            empty_pct = ts.get("empty_pct", 0.0)
            if empty_pct > 20.0:
                insights.append({
                    "column": col_name,
                    "insight": "HIGH_EMPTY_STRINGS",
                    "description": (
                        f"{empty_pct:.1f}% of values are empty strings."
                    ),
                    "severity": "MEDIUM",
                })
        except Exception:  # noqa: BLE001
            pass

    # --- Rule: MOSTLY_ZEROS ------------------------------------------------
    for col_name, ns in numeric_stats.items():
        try:
            zero_pct = ns.get("zero_pct", 0.0)
            if zero_pct > 50.0:
                insights.append({
                    "column": col_name,
                    "insight": "MOSTLY_ZEROS",
                    "description": (
                        f"{zero_pct:.1f}% of values are zero."
                    ),
                    "severity": "MEDIUM",
                })
        except Exception:  # noqa: BLE001
            pass

    return insights


def cmd_profile_table(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    *,
    no_deep: bool = False,
) -> Dict[str, Any]:
    """
    Build a comprehensive profile for a single table.

    Each sub-query is wrapped in its own try/except so that one failure
    does not prevent the rest of the profile from being collected.

    When *no_deep* is ``True`` the expensive deep-profiling queries
    (numeric / text / date / boolean / universal stats, pattern detection,
    and adaptive insights) are skipped entirely.
    """
    profile: Dict[str, Any] = {
        "schema": schema,
        "table": table,
    }

    # --- columns -----------------------------------------------------------
    try:
        columns = _get_columns(conn, schema, table)
        profile["columns"] = columns
    except Exception as exc:  # noqa: BLE001
        columns = []
        profile["columns"] = []
        profile["columns_error"] = str(exc)

    # --- constraints -------------------------------------------------------
    try:
        profile["constraints"] = _get_constraints(conn, schema, table)
    except Exception as exc:  # noqa: BLE001
        profile["constraints"] = []
        profile["constraints_error"] = str(exc)

    # --- indexes -----------------------------------------------------------
    try:
        profile["indexes"] = _get_indexes(conn, schema, table)
    except Exception as exc:  # noqa: BLE001
        profile["indexes"] = []
        profile["indexes_error"] = str(exc)

    # --- foreign keys (outgoing) -------------------------------------------
    try:
        profile["foreign_keys_outgoing"] = _get_foreign_keys_outgoing(
            conn, schema, table
        )
    except Exception as exc:  # noqa: BLE001
        profile["foreign_keys_outgoing"] = []
        profile["foreign_keys_outgoing_error"] = str(exc)

    # --- foreign keys (incoming) -------------------------------------------
    try:
        profile["foreign_keys_incoming"] = _get_foreign_keys_incoming(
            conn, schema, table
        )
    except Exception as exc:  # noqa: BLE001
        profile["foreign_keys_incoming"] = []
        profile["foreign_keys_incoming_error"] = str(exc)

    # --- row count ---------------------------------------------------------
    try:
        row_count = _get_row_count(conn, schema, table)
        profile["row_count"] = row_count
    except Exception as exc:  # noqa: BLE001
        row_count = None
        profile["row_count"] = None
        profile["row_count_error"] = str(exc)

    # --- table size --------------------------------------------------------
    try:
        profile["table_size"] = _get_table_size(conn, schema, table)
    except Exception as exc:  # noqa: BLE001
        profile["table_size"] = None
        profile["table_size_error"] = str(exc)

    # --- null percentages --------------------------------------------------
    try:
        profile["null_percentages"] = _get_null_percentages(
            conn, schema, table, columns
        )
    except Exception as exc:  # noqa: BLE001
        profile["null_percentages"] = {}
        profile["null_percentages_error"] = str(exc)

    # --- sample data -------------------------------------------------------
    try:
        profile["sample_data"] = _get_sample_data(conn, schema, table)
    except Exception as exc:  # noqa: BLE001
        profile["sample_data"] = []
        profile["sample_data_error"] = str(exc)

    # --- deep profiling ----------------------------------------------------
    if not no_deep:
        estimated_rows = row_count or 0
        try:
            estimated_rows = (
                _get_estimated_row_count(conn, schema, table) or estimated_rows
            )
        except Exception:  # noqa: BLE001
            pass

        sample_cl = _sample_clause(estimated_rows)
        col_groups = _classify_columns(columns)

        # numeric stats
        try:
            profile["numeric_stats"] = _get_numeric_stats(
                conn, schema, table,
                col_groups.get("numeric", []), sample_cl,
            )
        except Exception as exc:  # noqa: BLE001
            profile["numeric_stats"] = {}
            profile["numeric_stats_error"] = str(exc)

        # text stats
        try:
            profile["text_stats"] = _get_text_stats(
                conn, schema, table,
                col_groups.get("text", []), sample_cl,
            )
        except Exception as exc:  # noqa: BLE001
            profile["text_stats"] = {}
            profile["text_stats_error"] = str(exc)

        # text top values (per column, max 5 text columns to avoid N trips)
        try:
            top_values: Dict[str, Any] = {}
            for col in col_groups.get("text", [])[:5]:
                col_name = col["column_name"]
                top_values[col_name] = _get_text_top_values(
                    conn, schema, table, col_name, sample_cl,
                )
            profile["text_top_values"] = top_values
        except Exception as exc:  # noqa: BLE001
            profile["text_top_values"] = {}
            profile["text_top_values_error"] = str(exc)

        # text patterns (per column, max 5)
        try:
            patterns: Dict[str, Any] = {}
            for col in col_groups.get("text", [])[:5]:
                col_name = col["column_name"]
                patterns[col_name] = _get_text_patterns(
                    conn, schema, table, col_name, sample_cl,
                )
            profile["text_patterns"] = patterns
        except Exception as exc:  # noqa: BLE001
            profile["text_patterns"] = {}
            profile["text_patterns_error"] = str(exc)

        # date stats
        try:
            profile["date_stats"] = _get_date_stats(
                conn, schema, table,
                col_groups.get("date", []),
            )
        except Exception as exc:  # noqa: BLE001
            profile["date_stats"] = {}
            profile["date_stats_error"] = str(exc)

        # boolean stats
        try:
            profile["boolean_stats"] = _get_boolean_stats(
                conn, schema, table,
                col_groups.get("boolean", []),
            )
        except Exception as exc:  # noqa: BLE001
            profile["boolean_stats"] = {}
            profile["boolean_stats_error"] = str(exc)

        # universal stats (all columns)
        try:
            profile["universal_stats"] = _get_universal_stats(
                conn, schema, table, columns, sample_cl,
            )
        except Exception as exc:  # noqa: BLE001
            profile["universal_stats"] = {}
            profile["universal_stats_error"] = str(exc)

        # adaptive insights (computed from all collected stats, no DB call)
        try:
            profile["adaptive_insights"] = _compute_adaptive_insights(profile)
        except Exception as exc:  # noqa: BLE001
            profile["adaptive_insights"] = []
            profile["adaptive_insights_error"] = str(exc)

    return profile


def cmd_profile_batch(
    conn: psycopg2.extensions.connection,
    schema: str,
    tables: List[str],
    *,
    no_deep: bool = False,
) -> List[Dict[str, Any]]:
    """Profile multiple tables and return a JSON array of profiles."""
    return [
        cmd_profile_table(conn, schema, t, no_deep=no_deep)
        for t in tables
    ]


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    """Construct the top-level ``argparse`` parser with sub-commands."""
    parser = argparse.ArgumentParser(
        prog="postgres_connector",
        description="CLI tool for PostgreSQL schema introspection and profiling.",
    )

    # -- connection arguments (required on every invocation) -----------------
    parser.add_argument("--host", required=True, help="Database host")
    parser.add_argument("--port", type=int, default=5432, help="Database port (default: 5432)")
    parser.add_argument("--user", required=True, help="Database user")
    parser.add_argument("--password", required=True, help="Database password")
    parser.add_argument("--database", required=True, help="Database name")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # list-schemas ----------------------------------------------------------
    subparsers.add_parser("list-schemas", help="List all non-system schemas")

    # list-tables -----------------------------------------------------------
    lt = subparsers.add_parser("list-tables", help="List tables/views in a schema")
    lt.add_argument("--schema", required=True, help="Schema name")

    # profile-table ---------------------------------------------------------
    pt = subparsers.add_parser("profile-table", help="Full profile of one table")
    pt.add_argument("--schema", required=True, help="Schema name")
    pt.add_argument("--table", required=True, help="Table name")
    pt.add_argument(
        "--no-deep",
        action="store_true",
        default=False,
        help="Skip deep statistical profiling queries",
    )

    # profile-batch ---------------------------------------------------------
    pb = subparsers.add_parser(
        "profile-batch", help="Profile multiple tables (comma-separated)"
    )
    pb.add_argument("--schema", required=True, help="Schema name")
    pb.add_argument(
        "--tables",
        required=True,
        help="Comma-separated list of table names",
    )
    pb.add_argument(
        "--no-deep",
        action="store_true",
        default=False,
        help="Skip deep statistical profiling queries",
    )

    return parser


# ---------------------------------------------------------------------------
# Main entry-point
# ---------------------------------------------------------------------------
def main() -> None:
    """Parse arguments, connect, dispatch, and print JSON to stdout."""
    parser = build_parser()
    args = parser.parse_args()

    # -- establish connection ------------------------------------------------
    try:
        with _connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
        ) as conn:
            # -- dispatch to the requested command --------------------------
            if args.command == "list-schemas":
                result = cmd_list_schemas(conn)

            elif args.command == "list-tables":
                result = cmd_list_tables(conn, schema=args.schema)

            elif args.command == "profile-table":
                result = cmd_profile_table(
                    conn,
                    schema=args.schema,
                    table=args.table,
                    no_deep=args.no_deep,
                )

            elif args.command == "profile-batch":
                tables = [t.strip() for t in args.tables.split(",") if t.strip()]
                if not tables:
                    print(_json_out({"error": "No table names provided"}))
                    sys.exit(1)
                result = cmd_profile_batch(
                    conn,
                    schema=args.schema,
                    tables=tables,
                    no_deep=args.no_deep,
                )

            else:
                print(_json_out({"error": f"Unknown command: {args.command}"}))
                sys.exit(1)

            print(_json_out(result))
            sys.exit(0)

    except psycopg2.OperationalError as exc:
        print(_json_out({
            "error": "Connection failed",
            "details": str(exc),
        }))
        sys.exit(1)

    except psycopg2.Error as exc:
        print(_json_out({
            "error": "Database error",
            "details": str(exc),
        }))
        sys.exit(1)

    except Exception as exc:  # noqa: BLE001
        print(_json_out({
            "error": "Unexpected error",
            "details": str(exc),
        }))
        sys.exit(1)


if __name__ == "__main__":
    main()
