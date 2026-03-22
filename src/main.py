#!/usr/bin/env python3
"""
SchemaAnalyzer — Entry Point

Usage:
    # Profile a specific database with full pipeline + run management
    python src/main.py --from-env --source postgres --direct --analyze --report

    # Profile with custom run name
    python src/main.py --from-env --source postgres --direct --analyze --report --run-name pharma_deep

    # Profile a specific database by name
    python src/main.py --from-env --database "jhonson pharma" --direct --analyze --report

    # Run analysis/report/summarize on existing run
    python src/main.py --analyze-only
    python src/main.py --report-only
    python src/main.py --summarize-only

    # SDK mode (when claude-agent-sdk is installed)
    python src/main.py "My postgres is at host.com, user admin, password secret, db mydb"

All output goes to output/runs/<run_id>/ for proper run isolation.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

# Ensure project root is on path
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_PROJECT_ROOT))


# ---------------------------------------------------------------------------
# SDK mode (future — when claude-agent-sdk is installed)
# ---------------------------------------------------------------------------

async def run_with_sdk(task_prompt: str, system_prompt: str) -> None:
    """Run the orchestrator via Claude Agent SDK."""
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
        return

    from src.orchestrator import get_agent_definitions

    agent_defs = get_agent_definitions()
    options = ClaudeAgentOptions(
        system_prompt=system_prompt,
        agents=agent_defs,
        allowed_tools=["Read", "Write", "Edit", "Bash", "Glob", "Grep", "Agent"],
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


# ---------------------------------------------------------------------------
# Direct mode — profiles sources without Claude Agent SDK
# ---------------------------------------------------------------------------

async def run_direct(
    source_type: str,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    schema: str = "public",
    source_name: str | None = None,
    output_dir: str | Path | None = None,
    run_logger=None,
) -> dict:
    """Run the profiler directly. Returns profiling result dict."""
    from src.deep_agents.table_profiler import profile_tables_direct, _run_connector

    src_name = source_name or f"{source_type}_{database}"

    # Use provided output_dir or fallback to legacy path
    if output_dir:
        tables_dir = Path(output_dir) / src_name / "tables"
    else:
        tables_dir = _PROJECT_ROOT / "output" / "sources" / src_name / "tables"

    if run_logger:
        run_logger.log_agent_action("Discovery Agent", f"Connecting to {source_type}://{host}:{port}/{database}")

    print(f"[SchemaAnalyzer] Profiling {source_type}://{host}:{port}/{database}")
    print(f"[SchemaAnalyzer] Output: {tables_dir}")
    print()

    # Step 1: List schemas
    print("[1/4] Listing schemas...")
    schemas = _run_connector(source_type, host, port, database, user, password, "list-schemas")
    if isinstance(schemas, dict) and "error" in schemas:
        print(f"  ERROR: {schemas}")
        if run_logger:
            run_logger.log_error("Discovery Agent", f"Connection failed: {schemas}")
        return {"status": "error", "error": schemas}
    print(f"  Found schemas: {schemas}")
    if run_logger:
        run_logger.log_agent_action("Discovery Agent", f"Found {len(schemas)} schemas: {schemas}")

    # Step 2: List tables
    target_schema = schema
    if target_schema not in schemas and schemas:
        target_schema = schemas[0]
        print(f"  Schema '{schema}' not found; using '{target_schema}'")
        if run_logger:
            run_logger.log_decision(f"Schema '{schema}' not found, using '{target_schema}'")

    print(f"\n[2/4] Listing tables in '{target_schema}'...")
    tables_info = _run_connector(source_type, host, port, database, user, password, "list-tables", ["--schema", target_schema])
    if isinstance(tables_info, dict) and "error" in tables_info:
        print(f"  ERROR: {tables_info}")
        if run_logger:
            run_logger.log_error("Discovery Agent", f"Failed to list tables: {tables_info}")
        return {"status": "error", "error": tables_info}
    table_names = [t["table_name"] for t in tables_info if t.get("table_type") == "BASE TABLE"]
    print(f"  Found {len(table_names)} tables: {table_names[:10]}{'...' if len(table_names) > 10 else ''}")
    if run_logger:
        run_logger.log_agent_action("Discovery Agent", f"Found {len(table_names)} tables in {target_schema}")
        run_logger.log_decision(f"Single batch ({len(table_names)} tables < 25 threshold)")

    if not table_names:
        print("  No tables found. Exiting.")
        return {"status": "empty", "tables_profiled": 0}

    # Step 3: Profile tables (with deep stats)
    print(f"\n[3/4] Profiling {len(table_names)} tables (with deep statistics)...")
    if run_logger:
        run_logger.log_agent_action("Table Profiler", f"Starting batch profiling of {len(table_names)} tables")

    t_start = time.time()
    result = profile_tables_direct(
        source_type=source_type,
        host=host,
        port=port,
        db=database,
        user=user,
        password=password,
        schema=target_schema,
        tables=table_names,
        output_dir=str(tables_dir),
        source_name=src_name,
        model_name="direct-profiler",
        run_logger=run_logger,
    )
    t_elapsed = time.time() - t_start
    print(f"  Profiling took {t_elapsed:.1f}s")

    if run_logger:
        run_logger.log_metric("tables_profiled", result.get("tables_profiled", 0))
        run_logger.log_metric("tables_failed", result.get("tables_failed", 0))
        run_logger.log_metric("profiling_duration_seconds", round(t_elapsed, 1))

    # Step 4: Summary
    print(f"\n[4/4] Profiling complete!")
    print(f"  Tables profiled: {result.get('tables_profiled', 0)}")
    print(f"  Tables failed: {result.get('tables_failed', 0)}")
    print(f"  Output: {tables_dir}")
    print(f"\n  MD files written:")
    for md_file in sorted(tables_dir.glob("*.md")):
        size = md_file.stat().st_size
        print(f"    {md_file.name} ({size:,} bytes)")

    return result


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def run_summarize(output_dir: str = "output", run_logger=None) -> None:
    """Generate source summaries and master schema from existing table MDs."""
    from src.utils.summary_generator import generate_source_summary
    from src.utils.master_generator import generate_master_schema

    if run_logger:
        run_logger.log_phase("Summarization")

    output_path = Path(output_dir)
    sources_dir = output_path / "sources"

    if not sources_dir.exists():
        print("[SchemaAnalyzer] No sources directory found. Run profiling first.")
        return

    for source_dir in sorted(sources_dir.iterdir()):
        if not source_dir.is_dir():
            continue
        tables_dir = source_dir / "tables"
        if not tables_dir.exists() or not list(tables_dir.glob("*.md")):
            continue
        source_name = source_dir.name
        print(f"[summarize] Generating summary for {source_name}...")
        generate_source_summary(
            source_dir=str(source_dir),
            source_name=source_name,
            source_type="postgres",
            connection_info={"host": "***", "database": source_name},
        )
        if run_logger:
            run_logger.log_agent_action("Summary Generator", f"Generated _summary.md for {source_name}")

    print(f"[summarize] Generating master_schema.md...")
    generate_master_schema(output_dir)
    if run_logger:
        run_logger.log_agent_action("Master Generator", "Generated master_schema.md")
    print(f"[summarize] Done!")


def run_analyze(output_dir: str = "output", run_logger=None) -> dict:
    """Run the full analysis pipeline on existing output."""
    if run_logger:
        run_logger.log_phase("Analysis")

    from src.agents.analysis import run_analysis_direct
    results = run_analysis_direct(output_dir)

    if run_logger:
        for phase, result in results.items():
            if result and result.get("status") == "completed":
                run_logger.log_agent_action("Analysis Agent", f"{phase}: completed")
            elif result:
                run_logger.log_error("Analysis Agent", f"{phase}: {result.get('error', 'unknown')}")

    print(f"\n[SchemaAnalyzer] Analysis Results:")
    print(json.dumps(results, indent=2))
    return results


def run_report(output_dir: str = "output", run_logger=None) -> None:
    """Generate HTML reports for all profiled sources."""
    if run_logger:
        run_logger.log_phase("Report Generation")

    from src.utils.report_generator import generate_html_report

    output_path = Path(output_dir)
    sources_dir = output_path / "sources"

    if not sources_dir.exists():
        print("[SchemaAnalyzer] No sources directory found. Run profiling first.")
        return

    generated = 0
    for source_dir in sorted(sources_dir.iterdir()):
        if not source_dir.is_dir():
            continue
        tables_dir = source_dir / "tables"
        if not tables_dir.exists() or not list(tables_dir.glob("*.md")):
            continue
        source_name = source_dir.name
        report_path = source_dir / "report.html"
        print(f"[report] Generating HTML report for {source_name}...", flush=True)
        generate_html_report(
            source_dir=str(source_dir),
            source_name=source_name,
            output_path=str(report_path),
        )
        print(f"[report]   -> {report_path}")
        if run_logger:
            run_logger.log_agent_action("Report Generator", f"Generated report.html for {source_name}")
        generated += 1

    if generated:
        print(f"[report] Generated {generated} HTML report(s).")
    else:
        print("[report] No sources with table profiles found.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="schema-analyzer",
        description="SchemaAnalyzer — Agentic schema discovery & analysis",
    )

    parser.add_argument("input", nargs="?", help="Natural language input with credentials")
    parser.add_argument("--from-env", action="store_true", help="Load credentials from env_info_clean.json")
    parser.add_argument("--source", default=None, help="Source type filter (e.g., 'postgres')")
    parser.add_argument("--database", default=None, help="Specific database name to profile")
    parser.add_argument("--schema", default="public", help="Schema to profile (default: public)")
    parser.add_argument("--direct", action="store_true", help="Direct mode (no Claude Agent SDK)")
    parser.add_argument("--analyze", action="store_true", help="Run analysis after profiling")
    parser.add_argument("--analyze-only", action="store_true", help="Analysis only (use existing MDs)")
    parser.add_argument("--summarize-only", action="store_true", help="Summaries + master only")
    parser.add_argument("--report", action="store_true", help="Generate HTML reports")
    parser.add_argument("--report-only", action="store_true", help="Reports only (use existing MDs)")
    parser.add_argument("--run-name", default=None, help="Custom name suffix for this run")
    parser.add_argument("--env-info", default=str(_PROJECT_ROOT / "env_info_clean.json"), help="Path to env_info_clean.json")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # ---- Standalone modes (no run context needed) ----
    if args.report_only:
        run_report()
        return
    if args.summarize_only:
        run_summarize()
        return
    if args.analyze_only:
        run_analyze()
        return

    # ---- Direct mode with full run management ----
    if args.direct or args.from_env:
        from src.utils.config_parser import load_from_env_info

        # Load and filter configs
        configs = load_from_env_info(args.env_info)
        if args.source:
            configs = [c for c in configs if c.source_type.value == args.source]
        if args.database:
            configs = [c for c in configs if c.database and args.database.lower() in c.database.lower()]

        if not configs:
            print(f"[SchemaAnalyzer] No matching source found.")
            sys.exit(1)

        # Create run context
        try:
            from src.utils.run_manager import create_run, complete_run
            from src.utils.run_logger import RunLogger

            run_config = {
                "sources": [c.masked_repr() for c in configs],
                "schema": args.schema,
                "analyze": args.analyze,
                "report": args.report,
                "args": {k: v for k, v in vars(args).items() if v is not None and v is not False},
            }
            ctx = create_run(config=run_config, run_name=args.run_name)
            logger = RunLogger(ctx.run_dir, ctx.run_id)

            logger.log_phase("Initialization")
            logger.log_agent_action("Run Manager", f"Created run: {ctx.run_id}")
            logger.log_agent_action("Config", f"Sources: {len(configs)}, Schema: {args.schema}")
            logger.log_metric("run_id", ctx.run_id)
            logger.log_metric("run_dir", str(ctx.run_dir))

            output_dir = str(ctx.run_dir)
            sources_dir = str(ctx.sources_dir)

        except ImportError:
            # run_manager not available yet — fallback to legacy
            print("[SchemaAnalyzer] Warning: run_manager not available, using legacy output paths.")
            ctx = None
            logger = None
            output_dir = "output"
            sources_dir = None

        # Profile each source
        if logger:
            logger.log_phase("Discovery & Profiling")

        pipeline_start = time.time()
        total_profiled = 0
        total_failed = 0

        for config in configs:
            src_name = f"{config.source_type.value}_{config.database or 'default'}"
            print(f"\n{'='*60}")
            print(f"Processing: {config.source_type.value} / {config.database}")
            print(f"{'='*60}\n")

            if logger:
                logger.log_agent_action("Orchestrator", f"Starting source: {src_name}")

            result = asyncio.run(run_direct(
                source_type=config.source_type.value,
                host=config.host or "",
                port=config.port or 5432,
                user=config.user or "",
                password=config.password or "",
                database=config.database or "",
                schema=args.schema,
                source_name=src_name,
                output_dir=sources_dir,
                run_logger=logger,
            ))

            total_profiled += result.get("tables_profiled", 0) if isinstance(result, dict) else 0
            total_failed += result.get("tables_failed", 0) if isinstance(result, dict) else 0

        # Summarize
        run_summarize(output_dir, run_logger=logger)

        # Analyze
        analysis_results = None
        if args.analyze:
            analysis_results = run_analyze(output_dir, run_logger=logger)

        # Report
        if args.report:
            run_report(output_dir, run_logger=logger)

        # Complete run
        pipeline_elapsed = time.time() - pipeline_start
        if ctx and logger:
            logger.log_phase("Run Complete")
            logger.log_metric("total_tables_profiled", total_profiled)
            logger.log_metric("total_tables_failed", total_failed)
            logger.log_metric("total_duration_seconds", round(pipeline_elapsed, 1))

            run_metadata = {
                "tables_profiled": total_profiled,
                "tables_failed": total_failed,
                "duration_seconds": round(pipeline_elapsed, 1),
                "sources_profiled": len(configs),
                "analysis_run": args.analyze,
                "report_generated": args.report,
            }
            if analysis_results and "quality_audit" in analysis_results:
                qa = analysis_results.get("quality_audit", {})
                run_metadata["quality_audit_status"] = qa.get("status", "unknown")

            logger.finalize(summary=run_metadata)
            complete_run(ctx, run_metadata)

            print(f"\n{'='*60}")
            print(f"[SchemaAnalyzer] Run complete: {ctx.run_id}")
            print(f"  Run directory: {ctx.run_dir}")
            print(f"  Run log: {ctx.run_dir / 'run_log.md'}")
            print(f"  Duration: {pipeline_elapsed:.1f}s")
            print(f"  Tables: {total_profiled} profiled, {total_failed} failed")
            print(f"{'='*60}")

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
