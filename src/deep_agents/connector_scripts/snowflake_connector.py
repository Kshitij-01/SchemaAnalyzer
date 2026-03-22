#!/usr/bin/env python3
"""
Snowflake Connector Script for SchemaAnalyzer.

A standalone CLI tool that agents invoke via Bash to introspect and profile
Snowflake databases.  All output is JSON written to stdout so that the
calling agent can parse it directly.

Requirements:
    pip install snowflake-connector-python

Usage:
    python snowflake_connector.py \
        --account ACCOUNT --user USER --password PASS \
        --warehouse WH --database DB \
        <command> [command-args]

Commands:
    list-schemas
    list-tables   --schema SCHEMA
    profile-table --schema SCHEMA --table TABLE
    profile-batch --schema SCHEMA --tables TABLE1,TABLE2,...

Exit codes:
    0  Success (JSON result on stdout)
    1  Connection failure or fatal error (JSON error on stdout)

Snowflake-specific notes:
    - INFORMATION_SCHEMA identifiers are UPPERCASE in Snowflake.
    - Snowflake uses micro-partitioning, not B-tree indexes.
    - Constraints are retrieved via SHOW PRIMARY KEYS / SHOW IMPORTED KEYS.
    - Semi-structured types (VARIANT, OBJECT, ARRAY) are handled by the
      custom JSON encoder.
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
# snowflake-connector-python import with a friendly message when missing
# ---------------------------------------------------------------------------
try:
    import snowflake.connector
    from snowflake.connector import DictCursor
    from snowflake.connector.connection import SnowflakeConnection
except ImportError:
    _err = {
        "error": (
            "snowflake-connector-python is not installed. "
            "Run: pip install snowflake-connector-python"
        )
    }
    print(json.dumps(_err, indent=2))
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CONNECTION_TIMEOUT_SEC: int = 10
QUERY_TIMEOUT_SEC: int = 30

EXCLUDED_SCHEMAS: Tuple[str, ...] = (
    "INFORMATION_SCHEMA",
)


# ---------------------------------------------------------------------------
# JSON serialisation helpers
# ---------------------------------------------------------------------------
class _ExtendedEncoder(json.JSONEncoder):
    """Handle Snowflake-specific Python types that stdlib json cannot encode.

    Snowflake may return ``Decimal``, ``datetime``, ``date``, ``time``,
    ``timedelta``, ``bytes``, and semi-structured blobs (which the Python
    connector usually deserialises to ``dict`` / ``list`` / ``str``
    already).  This encoder acts as a safety-net for anything that slips
    through.
    """

    def default(self, o: Any) -> Any:  # noqa: D401
        if isinstance(o, Decimal):
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
        if isinstance(o, bytearray):
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
    account: str,
    user: str,
    password: str,
    warehouse: str,
    database: str,
) -> Generator[SnowflakeConnection, None, None]:
    """
    Open a Snowflake connection with a *login_timeout* and a per-statement
    *network_timeout* so that no single query blocks forever.

    Yields:
        An open ``SnowflakeConnection``.

    Raises:
        snowflake.connector.errors.Error: On any connection-level failure.
    """
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        login_timeout=CONNECTION_TIMEOUT_SEC,
        network_timeout=QUERY_TIMEOUT_SEC,
    )
    try:
        # Set statement-level timeout (seconds) via session parameter
        conn.cursor().execute(
            f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {QUERY_TIMEOUT_SEC}"
        )
        yield conn
    finally:
        conn.close()


def _query(
    conn: SnowflakeConnection,
    sql: str,
    params: Optional[Tuple[Any, ...]] = None,
) -> List[Dict[str, Any]]:
    """
    Execute *sql* and return a list of dicts (one per row).

    Uses ``DictCursor`` so every row is a ``dict`` whose keys are the
    column names.
    """
    cur = conn.cursor(DictCursor)
    try:
        cur.execute(sql, params)
        if cur.description is None:
            return []
        return [dict(row) for row in cur.fetchall()]
    finally:
        cur.close()


def _query_scalar(
    conn: SnowflakeConnection,
    sql: str,
    params: Optional[Tuple[Any, ...]] = None,
) -> Any:
    """Execute *sql* and return the single scalar value from the first row."""
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()


def _show_query(
    conn: SnowflakeConnection,
    sql: str,
) -> List[Dict[str, Any]]:
    """
    Execute a SHOW command and return the results as a list of dicts.

    SHOW commands in Snowflake do not support bind parameters, so *sql*
    must be a fully-formed string.  The result columns vary by SHOW
    command; the DictCursor returns them as dictionaries.
    """
    cur = conn.cursor(DictCursor)
    try:
        cur.execute(sql)
        if cur.description is None:
            return []
        return [dict(row) for row in cur.fetchall()]
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Quoting helper
# ---------------------------------------------------------------------------
def _qi(name: str) -> str:
    """Quote a Snowflake identifier (double-quote wrapping)."""
    return '"' + name.replace('"', '""') + '"'


def _fqn(schema: str, table: str) -> str:
    """
    Return a fully-qualified, safely-quoted ``schema.table`` identifier.

    Snowflake identifiers are case-insensitive when unquoted but
    case-preserving when quoted.  We always quote to be safe.
    """
    return f"{_qi(schema)}.{_qi(table)}"


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def cmd_list_schemas(conn: SnowflakeConnection) -> List[str]:
    """Return non-system schema names as a plain JSON array of strings."""
    sql = """
        SELECT SCHEMA_NAME
        FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
        ORDER BY SCHEMA_NAME
    """
    rows = _query(conn, sql)
    return [r["SCHEMA_NAME"] for r in rows]


def cmd_list_tables(
    conn: SnowflakeConnection,
    schema: str,
) -> List[Dict[str, str]]:
    """Return tables and views in *schema* as ``[{TABLE_NAME, TABLE_TYPE}]``."""
    sql = """
        SELECT TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME
    """
    return _query(conn, sql, (schema,))


# ---- profile-table sub-queries -------------------------------------------

def _get_columns(
    conn: SnowflakeConnection,
    schema: str,
    table: str,
) -> List[Dict[str, Any]]:
    """Column metadata from ``INFORMATION_SCHEMA.COLUMNS``."""
    sql = """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            ORDINAL_POSITION,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME   = %s
        ORDER BY ORDINAL_POSITION
    """
    return _query(conn, sql, (schema, table))


def _get_primary_keys(
    conn: SnowflakeConnection,
    schema: str,
    table: str,
) -> List[Dict[str, Any]]:
    """
    Primary key columns via ``SHOW PRIMARY KEYS``.

    Snowflake's INFORMATION_SCHEMA has limited constraint support, so we
    rely on SHOW commands instead.
    """
    sql = f"SHOW PRIMARY KEYS IN TABLE {_fqn(schema, table)}"
    try:
        rows = _show_query(conn, sql)
        return [
            {
                "constraint_name": r.get("constraint_name", ""),
                "column_name": r.get("column_name", ""),
                "key_sequence": r.get("key_sequence", ""),
            }
            for r in rows
        ]
    except Exception:  # noqa: BLE001
        # SHOW commands may fail on views or transient objects
        return []


def _get_imported_keys(
    conn: SnowflakeConnection,
    schema: str,
    table: str,
) -> List[Dict[str, Any]]:
    """
    Foreign keys *from* this table to other tables via ``SHOW IMPORTED KEYS``.

    Each row describes one column mapping in a foreign-key relationship.
    """
    sql = f"SHOW IMPORTED KEYS IN TABLE {_fqn(schema, table)}"
    try:
        rows = _show_query(conn, sql)
        return [
            {
                "fk_constraint_name": r.get("fk_name", ""),
                "fk_column_name": r.get("fk_column_name", ""),
                "pk_schema_name": r.get("pk_schema_name", ""),
                "pk_table_name": r.get("pk_table_name", ""),
                "pk_column_name": r.get("pk_column_name", ""),
            }
            for r in rows
        ]
    except Exception:  # noqa: BLE001
        return []


def _get_row_count(
    conn: SnowflakeConnection,
    schema: str,
    table: str,
) -> Optional[int]:
    """Exact row count.  Returns ``None`` on timeout or error."""
    fqn = _fqn(schema, table)
    sql = f"SELECT COUNT(*) FROM {fqn}"  # noqa: S608 — fqn is quoted
    return _query_scalar(conn, sql)


def _get_table_size(
    conn: SnowflakeConnection,
    schema: str,
    table: str,
) -> Optional[Dict[str, Any]]:
    """
    Approximate table storage size from ``INFORMATION_SCHEMA.TABLE_STORAGE_METRICS``.

    Returns a dict with ``active_bytes``, ``time_travel_bytes``,
    ``failsafe_bytes``, and ``retained_for_clone_bytes`` when available.
    Falls back to ``INFORMATION_SCHEMA.TABLES.BYTES`` if the storage
    metrics view is inaccessible.
    """
    # Primary approach: TABLE_STORAGE_METRICS
    sql_metrics = """
        SELECT
            ACTIVE_BYTES,
            TIME_TRAVEL_BYTES,
            FAILSAFE_BYTES,
            RETAINED_FOR_CLONE_BYTES
        FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME   = %s
    """
    try:
        rows = _query(conn, sql_metrics, (schema, table))
        if rows:
            row = rows[0]
            total = sum(
                v for v in row.values() if isinstance(v, (int, float))
            )
            row["total_bytes"] = total
            row["total_human"] = _bytes_human(total)
            return row
    except Exception:  # noqa: BLE001
        pass

    # Fallback: BYTES column in INFORMATION_SCHEMA.TABLES
    sql_fallback = """
        SELECT BYTES
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME   = %s
    """
    try:
        rows = _query(conn, sql_fallback, (schema, table))
        if rows and rows[0].get("BYTES") is not None:
            total = rows[0]["BYTES"]
            return {
                "total_bytes": total,
                "total_human": _bytes_human(total),
                "source": "INFORMATION_SCHEMA.TABLES.BYTES",
            }
    except Exception:  # noqa: BLE001
        pass

    return None


def _bytes_human(num_bytes: int) -> str:
    """Convert a byte count to a human-readable string (KB / MB / GB / TB)."""
    if num_bytes is None:
        return "unknown"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0  # type: ignore[assignment]
    return f"{num_bytes:.1f} PB"


def _get_null_percentages(
    conn: SnowflakeConnection,
    schema: str,
    table: str,
    columns: List[Dict[str, Any]],
) -> Dict[str, Optional[float]]:
    """
    For each column, compute the percentage of NULL values.

    Returns ``{column_name: pct}`` where *pct* is a float in [0, 100] or
    ``None`` if the query failed.

    Note: Snowflake does not support ``FILTER (WHERE ...)`` aggregate
    syntax, so we use ``SUM(CASE ...)`` instead.
    """
    fqn = _fqn(schema, table)
    result: Dict[str, Optional[float]] = {}

    if not columns:
        return result

    # Build a single query that computes all null percentages at once
    # to avoid N round-trips.
    select_parts: List[str] = []
    for col in columns:
        col_name = col["COLUMN_NAME"]
        quoted = _qi(col_name)
        select_parts.append(
            f"SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END) * 100.0 "
            f"/ NULLIF(COUNT(*), 0)"
        )

    sql = f"SELECT {', '.join(select_parts)} FROM {fqn}"  # noqa: S608
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            if row:
                for idx, col in enumerate(columns):
                    val = row[idx]
                    result[col["COLUMN_NAME"]] = (
                        float(val) if val is not None else None
                    )
        finally:
            cur.close()
    except Exception as exc:  # noqa: BLE001
        # Fall back: mark every column as unknown
        for col in columns:
            result[col["COLUMN_NAME"]] = None
        result["_error"] = str(exc)  # type: ignore[assignment]

    return result


def _get_sample_data(
    conn: SnowflakeConnection,
    schema: str,
    table: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """Return the first *limit* rows from the table."""
    fqn = _fqn(schema, table)
    sql = f"SELECT * FROM {fqn} LIMIT %s"  # noqa: S608
    return _query(conn, sql, (limit,))


def cmd_profile_table(
    conn: SnowflakeConnection,
    schema: str,
    table: str,
) -> Dict[str, Any]:
    """
    Build a comprehensive profile for a single table.

    Each sub-query is wrapped in its own try/except so that one failure
    does not prevent the rest of the profile from being collected.

    Snowflake-specific differences from Postgres:
    - Constraints use SHOW PRIMARY KEYS / SHOW IMPORTED KEYS
    - No traditional B-tree indexes (Snowflake uses micro-partitioning)
    - Table size from TABLE_STORAGE_METRICS or TABLES.BYTES
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

    # --- primary keys (via SHOW) -------------------------------------------
    try:
        profile["primary_keys"] = _get_primary_keys(conn, schema, table)
    except Exception as exc:  # noqa: BLE001
        profile["primary_keys"] = []
        profile["primary_keys_error"] = str(exc)

    # --- indexes -----------------------------------------------------------
    # Snowflake does not use traditional B-tree indexes.  It relies on
    # automatic micro-partitioning and clustering keys instead.
    profile["indexes"] = []
    profile["indexes_note"] = (
        "Snowflake does not use traditional indexes. It relies on "
        "automatic micro-partitioning and optional clustering keys."
    )

    # --- foreign keys (outgoing via SHOW IMPORTED KEYS) --------------------
    try:
        profile["foreign_keys_outgoing"] = _get_imported_keys(
            conn, schema, table
        )
    except Exception as exc:  # noqa: BLE001
        profile["foreign_keys_outgoing"] = []
        profile["foreign_keys_outgoing_error"] = str(exc)

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
    conn: SnowflakeConnection,
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
        prog="snowflake_connector",
        description="CLI tool for Snowflake schema introspection and profiling.",
    )

    # -- connection arguments (required on every invocation) -----------------
    parser.add_argument("--account", required=True, help="Snowflake account identifier (e.g. gia52592.east-us-2.azure)")
    parser.add_argument("--user", required=True, help="Snowflake user")
    parser.add_argument("--password", required=True, help="Snowflake password")
    parser.add_argument("--warehouse", required=True, help="Snowflake warehouse")
    parser.add_argument("--database", required=True, help="Snowflake database")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # list-schemas ----------------------------------------------------------
    subparsers.add_parser("list-schemas", help="List all non-system schemas")

    # list-tables -----------------------------------------------------------
    lt = subparsers.add_parser("list-tables", help="List tables/views in a schema")
    lt.add_argument("--schema", required=True, help="Schema name (UPPERCASE)")

    # profile-table ---------------------------------------------------------
    pt = subparsers.add_parser("profile-table", help="Full profile of one table")
    pt.add_argument("--schema", required=True, help="Schema name (UPPERCASE)")
    pt.add_argument("--table", required=True, help="Table name (UPPERCASE)")

    # profile-batch ---------------------------------------------------------
    pb = subparsers.add_parser(
        "profile-batch", help="Profile multiple tables (comma-separated)"
    )
    pb.add_argument("--schema", required=True, help="Schema name (UPPERCASE)")
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
            account=args.account,
            user=args.user,
            password=args.password,
            warehouse=args.warehouse,
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

    except snowflake.connector.errors.DatabaseError as exc:
        print(_json_out({
            "error": "Connection failed",
            "details": str(exc),
        }))
        sys.exit(1)

    except snowflake.connector.errors.Error as exc:
        print(_json_out({
            "error": "Snowflake error",
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
