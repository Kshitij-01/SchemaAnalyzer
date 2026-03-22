#!/usr/bin/env python3
"""
SchemaAnalyzer — Entry Point

Usage:
    # Interactive: pass credentials as argument
    python src/main.py "My postgres is at host.com, user admin, password secret, db mydb"

    # From env_info_clean.json (all configured sources)
    python src/main.py --from-env

    # Specific source from env_info
    python src/main.py --from-env --source postgres

This script:
  1. Builds the orchestrator task prompt from user input
  2. Creates a ClaudeSDKClient with the orchestrator system prompt and agent definitions
  3. Sends the task and streams the response
  4. The orchestrator autonomously spawns discovery/analysis/report agents
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

# Ensure project root is on path
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_PROJECT_ROOT))


async def run_with_sdk(task_prompt: str, system_prompt: str) -> None:
    """Run the orchestrator via Claude Agent SDK.

    If claude-agent-sdk is installed, use ClaudeSDKClient for full agent
    capabilities. Otherwise fall back to a simple informational message.
    """
    try:
        from claude_agent_sdk import (
            ClaudeSDKClient,
            ClaudeAgentOptions,
            AssistantMessage,
            ResultMessage,
            TextBlock,
        )
    except ImportError:
        print(
            "\n[SchemaAnalyzer] claude-agent-sdk is not installed.\n"
            "Install it with: pip install claude-agent-sdk\n"
            "\nFalling back to displaying the task prompt that would be sent:\n"
        )
        print("=" * 80)
        print(task_prompt)
        print("=" * 80)
        print(
            "\nTo run the profiler directly without the SDK, use:\n"
            "  python src/deep_agents/table_profiler.py --help\n"
        )
        return

    from src.orchestrator import get_agent_definitions

    agent_defs = get_agent_definitions()

    options = ClaudeAgentOptions(
        system_prompt=system_prompt,
        agents=agent_defs,
        allowed_tools=[
            "Read", "Write", "Edit", "Bash", "Glob", "Grep", "Agent",
        ],
        permission_mode="bypassPermissions",
        max_turns=50,
        cwd=str(_PROJECT_ROOT),
    )

    print("[SchemaAnalyzer] Starting orchestrator...\n")

    async with ClaudeSDKClient(options=options) as client:
        await client.query(task_prompt)

        async for message in client.receive_response():
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        print(block.text, end="", flush=True)
            elif isinstance(message, ResultMessage):
                print(f"\n\n[SchemaAnalyzer] Done.")
                print(f"  Cost: ${message.total_cost_usd:.4f}")
                print(f"  Duration: {message.duration_ms / 1000:.1f}s")
                print(f"  Turns: {message.num_turns}")


async def run_direct(
    source_type: str,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    schema: str = "public",
    source_name: str | None = None,
) -> None:
    """Run the profiler directly without Claude Agent SDK.

    This is a simpler path that:
      1. Lists tables via connector
      2. Profiles them via table_profiler (direct mode)
      3. Prints a summary

    Useful for testing or when Claude Agent SDK is not available.
    """
    from src.deep_agents.table_profiler import profile_tables_direct, _run_connector

    src_name = source_name or f"{source_type}_{database}"
    output_dir = _PROJECT_ROOT / "output" / "sources" / src_name / "tables"

    print(f"[SchemaAnalyzer] Direct mode — profiling {source_type}://{host}:{port}/{database}")
    print(f"[SchemaAnalyzer] Output directory: {output_dir}")
    print()

    # Step 1: List schemas
    print("[1/4] Listing schemas...")
    schemas = _run_connector(source_type, host, port, database, user, password, "list-schemas")
    if isinstance(schemas, dict) and "error" in schemas:
        print(f"  ERROR: {schemas}")
        return
    print(f"  Found schemas: {schemas}")

    # Step 2: List tables
    target_schema = schema
    if target_schema not in schemas and schemas:
        target_schema = schemas[0]
        print(f"  Schema '{schema}' not found; using '{target_schema}'")

    print(f"\n[2/4] Listing tables in '{target_schema}'...")
    tables_info = _run_connector(source_type, host, port, database, user, password, "list-tables", ["--schema", target_schema])
    if isinstance(tables_info, dict) and "error" in tables_info:
        print(f"  ERROR: {tables_info}")
        return
    table_names = [t["table_name"] for t in tables_info if t.get("table_type") == "BASE TABLE"]
    print(f"  Found {len(table_names)} tables: {table_names[:10]}{'...' if len(table_names) > 10 else ''}")

    if not table_names:
        print("  No tables found. Exiting.")
        return

    # Step 3: Profile tables
    print(f"\n[3/4] Profiling {len(table_names)} tables...")
    result = profile_tables_direct(
        source_type=source_type,
        host=host,
        port=port,
        db=database,
        user=user,
        password=password,
        schema=target_schema,
        tables=table_names,
        output_dir=str(output_dir),
        source_name=src_name,
        model_name="direct-profiler",
    )
    print(f"  Result: {json.dumps(result, indent=2)}")

    # Step 4: Summary
    print(f"\n[4/4] Profiling complete!")
    print(f"  Tables profiled: {result.get('tables_profiled', 0)}")
    print(f"  Tables failed: {result.get('tables_failed', 0)}")
    print(f"  Output: {output_dir}")
    print(f"\n  MD files written:")
    for md_file in sorted(output_dir.glob("*.md")):
        size = md_file.stat().st_size
        print(f"    {md_file.name} ({size:,} bytes)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="schema-analyzer",
        description="SchemaAnalyzer — Agentic schema discovery & analysis",
    )

    parser.add_argument(
        "input",
        nargs="?",
        help="Natural language input with credentials and task description",
    )
    parser.add_argument(
        "--from-env",
        action="store_true",
        help="Load credentials from env_info_clean.json",
    )
    parser.add_argument(
        "--source",
        default=None,
        help="Specific source to profile (e.g., 'postgres', 'snowflake')",
    )
    parser.add_argument(
        "--schema",
        default="public",
        help="Schema to profile (default: public)",
    )
    parser.add_argument(
        "--direct",
        action="store_true",
        help="Run in direct mode (no Claude Agent SDK, just connector + formatter)",
    )
    parser.add_argument(
        "--env-info",
        default=str(_PROJECT_ROOT / "env_info_clean.json"),
        help="Path to env_info_clean.json",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # ---- Direct mode (no SDK) ----
    if args.direct or args.from_env:
        from src.utils.config_parser import load_from_env_info

        configs = load_from_env_info(args.env_info)

        if args.source:
            configs = [c for c in configs if c.source_type.value == args.source]

        if not configs:
            print(f"[SchemaAnalyzer] No matching source found. Available sources in env_info.")
            sys.exit(1)

        for config in configs:
            print(f"\n{'='*60}")
            print(f"Processing: {config.source_type.value}")
            print(f"{'='*60}\n")

            asyncio.run(run_direct(
                source_type=config.source_type.value,
                host=config.host or "",
                port=config.port or 5432,
                user=config.user or "",
                password=config.password or "",
                database=config.database or "",
                schema=args.schema,
                source_name=f"{config.source_type.value}_{config.database or 'default'}",
            ))
        return

    # ---- SDK mode ----
    if not args.input:
        parser.print_help()
        print("\n[SchemaAnalyzer] Please provide input text or use --from-env")
        sys.exit(1)

    from src.orchestrator import load_orchestrator_system_prompt, build_orchestrator_task

    system_prompt = load_orchestrator_system_prompt()
    task_prompt = build_orchestrator_task(args.input)

    asyncio.run(run_with_sdk(task_prompt, system_prompt))


if __name__ == "__main__":
    main()
