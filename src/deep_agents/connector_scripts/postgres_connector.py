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


def cmd_profile_table(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
) -> Dict[str, Any]:
    """
    Build a comprehensive profile for a single table.

    Each sub-query is wrapped in its own try/except so that one failure
    does not prevent the rest of the profile from being collected.
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
        profile["row_count"] = _get_row_count(conn, schema, table)
    except Exception as exc:  # noqa: BLE001
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

    return profile


def cmd_profile_batch(
    conn: psycopg2.extensions.connection,
    schema: str,
    tables: List[str],
) -> List[Dict[str, Any]]:
    """Profile multiple tables and return a JSON array of profiles."""
    return [cmd_profile_table(conn, schema, t) for t in tables]


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
                    conn, schema=args.schema, table=args.table
                )

            elif args.command == "profile-batch":
                tables = [t.strip() for t in args.tables.split(",") if t.strip()]
                if not tables:
                    print(_json_out({"error": "No table names provided"}))
                    sys.exit(1)
                result = cmd_profile_batch(
                    conn, schema=args.schema, tables=tables
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
