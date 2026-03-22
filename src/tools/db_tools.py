"""In-process MCP tools for database access.

These tools let Claude agents query PostgreSQL databases as native tool calls,
removing the need to shell out to connector scripts.  They wrap the existing
functions in ``src/deep_agents/connector_scripts/postgres_connector.py`` and
expose them through the Claude Agent SDK's ``@tool`` / ``create_sdk_mcp_server``
API.

Tools provided:
    * ``query_postgres``         -- Execute arbitrary SQL against a Postgres DB.
    * ``list_postgres_schemas``  -- List non-system schemas.
    * ``list_postgres_tables``   -- List tables/views in a given schema.
    * ``profile_postgres_table`` -- Full deep profile of a single table.

Usage with the SDK::

    from src.tools.db_tools import db_server

    options = ClaudeAgentOptions(
        mcp_servers={"database": db_server},
        allowed_tools=[
            "mcp__database__query_postgres",
            "mcp__database__list_postgres_schemas",
            "mcp__database__list_postgres_tables",
            "mcp__database__profile_postgres_table",
        ],
    )
"""

from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path
from typing import Any

# Ensure the project root is importable so we can reach the connector module.
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_CONNECTOR_DIR = _PROJECT_ROOT / "src" / "deep_agents" / "connector_scripts"
if str(_CONNECTOR_DIR) not in sys.path:
    sys.path.insert(0, str(_CONNECTOR_DIR))

# Import the low-level connector helpers.  We use a lazy-import style so that
# this module can still be *imported* even when psycopg2 is missing (the error
# will surface at tool-call time instead).
try:
    from postgres_connector import (
        _connect,
        _json_out,
        _query,
        cmd_list_schemas,
        cmd_list_tables,
        cmd_profile_table,
    )

    _CONNECTOR_AVAILABLE = True
    _CONNECTOR_ERROR: str | None = None
except Exception as _exc:  # noqa: BLE001
    _CONNECTOR_AVAILABLE = False
    _CONNECTOR_ERROR = str(_exc)

from claude_agent_sdk import create_sdk_mcp_server, tool

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _text_result(data: Any) -> dict[str, Any]:
    """Wrap *data* as a successful MCP text-content response."""
    if isinstance(data, str):
        text = data
    else:
        text = json.dumps(data, indent=2, default=str)
    return {"content": [{"type": "text", "text": text}]}


def _error_result(message: str) -> dict[str, Any]:
    """Wrap an error string as an MCP error response."""
    return {
        "content": [{"type": "text", "text": f"ERROR: {message}"}],
        "is_error": True,
    }


def _check_connector() -> dict[str, Any] | None:
    """Return an error result if the connector module is unavailable."""
    if not _CONNECTOR_AVAILABLE:
        return _error_result(
            f"postgres_connector is not available: {_CONNECTOR_ERROR}. "
            "Ensure psycopg2-binary is installed."
        )
    return None


# ---------------------------------------------------------------------------
# Tool: query_postgres
# ---------------------------------------------------------------------------

@tool(
    "query_postgres",
    "Execute an arbitrary SQL query against a PostgreSQL database and return "
    "results as JSON.  Use this for ad-hoc investigation queries like "
    "SELECT, SHOW, EXPLAIN, or aggregation queries.",
    {
        "host": str,
        "port": int,
        "user": str,
        "password": str,
        "database": str,
        "sql": str,
    },
)
async def query_postgres(args: dict[str, Any]) -> dict[str, Any]:
    """Execute arbitrary SQL and return JSON rows."""
    err = _check_connector()
    if err is not None:
        return err

    try:
        with _connect(
            host=args["host"],
            port=int(args["port"]),
            user=args["user"],
            password=args["password"],
            database=args["database"],
        ) as conn:
            rows = _query(conn, args["sql"])
            return _text_result({
                "status": "ok",
                "row_count": len(rows),
                "rows": rows,
            })
    except Exception as exc:  # noqa: BLE001
        return _error_result(
            f"Query failed: {exc}\n{traceback.format_exc()}"
        )


# ---------------------------------------------------------------------------
# Tool: list_postgres_schemas
# ---------------------------------------------------------------------------

@tool(
    "list_postgres_schemas",
    "List all non-system schemas in a PostgreSQL database.  Returns an array "
    "of schema names (excludes pg_catalog, information_schema, pg_toast).",
    {
        "host": str,
        "port": int,
        "user": str,
        "password": str,
        "database": str,
    },
)
async def list_postgres_schemas(args: dict[str, Any]) -> dict[str, Any]:
    """List schemas in the target database."""
    err = _check_connector()
    if err is not None:
        return err

    try:
        with _connect(
            host=args["host"],
            port=int(args["port"]),
            user=args["user"],
            password=args["password"],
            database=args["database"],
        ) as conn:
            schemas = cmd_list_schemas(conn)
            return _text_result({
                "status": "ok",
                "schemas": schemas,
                "count": len(schemas),
            })
    except Exception as exc:  # noqa: BLE001
        return _error_result(
            f"Failed to list schemas: {exc}\n{traceback.format_exc()}"
        )


# ---------------------------------------------------------------------------
# Tool: list_postgres_tables
# ---------------------------------------------------------------------------

@tool(
    "list_postgres_tables",
    "List all tables and views in a specific schema of a PostgreSQL database. "
    "Returns an array of objects with table_name and table_type fields.",
    {
        "host": str,
        "port": int,
        "user": str,
        "password": str,
        "database": str,
        "schema": str,
    },
)
async def list_postgres_tables(args: dict[str, Any]) -> dict[str, Any]:
    """List tables in a specific schema."""
    err = _check_connector()
    if err is not None:
        return err

    try:
        with _connect(
            host=args["host"],
            port=int(args["port"]),
            user=args["user"],
            password=args["password"],
            database=args["database"],
        ) as conn:
            tables = cmd_list_tables(conn, args["schema"])
            return _text_result({
                "status": "ok",
                "schema": args["schema"],
                "tables": tables,
                "count": len(tables),
            })
    except Exception as exc:  # noqa: BLE001
        return _error_result(
            f"Failed to list tables: {exc}\n{traceback.format_exc()}"
        )


# ---------------------------------------------------------------------------
# Tool: profile_postgres_table
# ---------------------------------------------------------------------------

@tool(
    "profile_postgres_table",
    "Get a comprehensive deep profile of a PostgreSQL table including column "
    "metadata, constraints, indexes, foreign keys, row counts, null "
    "percentages, sample data, and deep statistics (numeric stats, text "
    "patterns, date ranges, boolean distributions, cardinality analysis, "
    "and adaptive insights).",
    {
        "host": str,
        "port": int,
        "user": str,
        "password": str,
        "database": str,
        "schema": str,
        "table": str,
    },
)
async def profile_postgres_table(args: dict[str, Any]) -> dict[str, Any]:
    """Profile a single table with full deep statistics."""
    err = _check_connector()
    if err is not None:
        return err

    try:
        with _connect(
            host=args["host"],
            port=int(args["port"]),
            user=args["user"],
            password=args["password"],
            database=args["database"],
        ) as conn:
            profile = cmd_profile_table(
                conn,
                args["schema"],
                args["table"],
                no_deep=False,
            )
            # Use the connector's encoder for Postgres-specific types
            text = _json_out({"status": "ok", "profile": profile})
            return {"content": [{"type": "text", "text": text}]}
    except Exception as exc:  # noqa: BLE001
        return _error_result(
            f"Failed to profile table {args.get('schema', '?')}."
            f"{args.get('table', '?')}: {exc}\n{traceback.format_exc()}"
        )


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

db_server = create_sdk_mcp_server(
    name="database",
    version="1.0.0",
    tools=[
        query_postgres,
        list_postgres_schemas,
        list_postgres_tables,
        profile_postgres_table,
    ],
)
