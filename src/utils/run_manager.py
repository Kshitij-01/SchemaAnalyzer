"""Run lifecycle management for SchemaAnalyzer.

Creates isolated run directories, tracks metadata, and provides
utilities for listing and retrieving past runs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class RunContext:
    """Immutable context describing a single analysis run.

    Every run gets its own directory tree under ``output/runs/``.
    Sub-directories partition artefacts by purpose so that downstream
    consumers (reporters, agents, auditors) always know where to look.
    """

    run_id: str          # e.g. "run_20260322_193000_pharma_deep"
    run_dir: Path        # output/runs/run_20260322_193000_pharma_deep/
    sources_dir: Path    # .../<run>/sources/
    analysis_dir: Path   # .../<run>/analysis/
    context_dir: Path    # .../<run>/context/
    reports_dir: Path    # .../<run>/reports/
    config: dict         # original run config
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # -- convenience accessors --------------------------------------------------

    def source_dir(self, source_name: str) -> Path:
        """Get the source-specific directory."""
        return self.sources_dir / source_name

    def tables_dir(self, source_name: str) -> Path:
        """Get the tables directory for a source."""
        return self.sources_dir / source_name / "tables"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_run(
    config: dict,
    run_name: str | None = None,
    base_dir: str | Path = "output/runs",
) -> RunContext:
    """Create a new run directory tree and return its *RunContext*.

    Parameters
    ----------
    config:
        The full run configuration dictionary.  Persisted as
        ``run_config.json`` inside the run directory.
    run_name:
        Optional human-friendly suffix appended to the generated run id
        (e.g. ``"pharma_deep"`` -> ``run_20260322_193000_pharma_deep``).
    base_dir:
        Parent directory that holds all run folders.

    Returns
    -------
    RunContext
        A fully-initialised context pointing at the newly created
        directory tree.
    """
    base = Path(base_dir)
    now = datetime.now(timezone.utc)

    # Build run id -----------------------------------------------------------
    timestamp_part = now.strftime("%Y%m%d_%H%M%S")
    run_id = f"run_{timestamp_part}"
    if run_name:
        # Sanitise: replace spaces / slashes with underscores
        safe_name = run_name.replace(" ", "_").replace("/", "_").replace("\\", "_")
        run_id = f"{run_id}_{safe_name}"

    run_dir = base / run_id

    # Sub-directories --------------------------------------------------------
    sources_dir = run_dir / "sources"
    analysis_dir = run_dir / "analysis"
    context_dir = run_dir / "context"
    reports_dir = run_dir / "reports"

    for d in [
        sources_dir,
        analysis_dir,
        context_dir / "discovery",
        context_dir / "agent_comms",
        context_dir / "feedback",
        reports_dir,
    ]:
        d.mkdir(parents=True, exist_ok=True)

    # Persist config ---------------------------------------------------------
    config_path = run_dir / "run_config.json"
    config_path.write_text(json.dumps(config, indent=2, default=str), encoding="utf-8")

    return RunContext(
        run_id=run_id,
        run_dir=run_dir,
        sources_dir=sources_dir,
        analysis_dir=analysis_dir,
        context_dir=context_dir,
        reports_dir=reports_dir,
        config=config,
        start_time=now,
    )


def complete_run(ctx: RunContext, metadata: dict) -> None:
    """Finalise a run: write metadata and update the global index.

    Parameters
    ----------
    ctx:
        The *RunContext* returned by :func:`create_run`.
    metadata:
        A dictionary that **must** contain at least the keys
        ``sources_profiled``, ``tables_profiled``, ``tables_failed``,
        ``quality_score``, and ``status``.  Additional keys are
        preserved as-is.
    """
    end_time = datetime.now(timezone.utc)
    duration = (end_time - ctx.start_time).total_seconds()

    run_meta: dict[str, Any] = {
        "run_id": ctx.run_id,
        "start_time": ctx.start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": round(duration, 2),
        "sources_profiled": metadata.get("sources_profiled", 0),
        "tables_profiled": metadata.get("tables_profiled", 0),
        "tables_failed": metadata.get("tables_failed", 0),
        "quality_score": metadata.get("quality_score"),
        "status": metadata.get("status", "completed"),
    }
    # Preserve any extra keys the caller passed in
    for k, v in metadata.items():
        if k not in run_meta:
            run_meta[k] = v

    # Write per-run metadata -------------------------------------------------
    meta_path = ctx.run_dir / "run_metadata.json"
    meta_path.write_text(json.dumps(run_meta, indent=2, default=str), encoding="utf-8")

    # Update global index ----------------------------------------------------
    index_path = ctx.run_dir.parent / "runs_index.json"
    if index_path.exists():
        index: list[dict] = json.loads(index_path.read_text(encoding="utf-8"))
    else:
        index = []
    index.append(run_meta)
    index_path.write_text(json.dumps(index, indent=2, default=str), encoding="utf-8")

    # Update latest pointer (plain text, Windows-safe, no symlinks) ----------
    latest_path = ctx.run_dir.parent / "latest.txt"
    latest_path.write_text(ctx.run_id, encoding="utf-8")


def list_runs(base_dir: str | Path = "output/runs") -> list[dict]:
    """Return the list of all recorded run summaries.

    Reads ``runs_index.json`` from *base_dir*.  Returns an empty list
    when the index file does not exist yet.
    """
    index_path = Path(base_dir) / "runs_index.json"
    if not index_path.exists():
        return []
    return json.loads(index_path.read_text(encoding="utf-8"))


def get_latest_run(base_dir: str | Path = "output/runs") -> RunContext | None:
    """Reconstruct the *RunContext* for the most recent run.

    Uses ``latest.txt`` to determine the run id, then delegates to
    :func:`get_run`.  Returns ``None`` when no run has been recorded
    yet.
    """
    latest_path = Path(base_dir) / "latest.txt"
    if not latest_path.exists():
        return None
    run_id = latest_path.read_text(encoding="utf-8").strip()
    return get_run(run_id, base_dir)


def get_run(run_id: str, base_dir: str | Path = "output/runs") -> RunContext | None:
    """Load a specific run by its *run_id*.

    Returns ``None`` if the run directory or its config file does not
    exist.
    """
    run_dir = Path(base_dir) / run_id
    config_path = run_dir / "run_config.json"
    if not config_path.exists():
        return None

    config = json.loads(config_path.read_text(encoding="utf-8"))

    # Attempt to recover start_time from metadata if available
    meta_path = run_dir / "run_metadata.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        start_time = datetime.fromisoformat(meta["start_time"])
    else:
        start_time = datetime.now(timezone.utc)

    return RunContext(
        run_id=run_id,
        run_dir=run_dir,
        sources_dir=run_dir / "sources",
        analysis_dir=run_dir / "analysis",
        context_dir=run_dir / "context",
        reports_dir=run_dir / "reports",
        config=config,
        start_time=start_time,
    )


# ---------------------------------------------------------------------------
# Quick smoke-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp) / "runs"

        # Create a run
        ctx = create_run(
            config={"sources": ["postgres://localhost/test"]},
            run_name="demo",
            base_dir=base,
        )
        print(f"Created run: {ctx.run_id}")
        print(f"  run_dir   : {ctx.run_dir}")
        print(f"  sources   : {ctx.sources_dir}")
        print(f"  analysis  : {ctx.analysis_dir}")
        print(f"  context   : {ctx.context_dir}")
        print(f"  reports   : {ctx.reports_dir}")
        print(f"  tables_dir: {ctx.tables_dir('my_db')}")

        # Complete the run
        complete_run(ctx, {
            "sources_profiled": 1,
            "tables_profiled": 12,
            "tables_failed": 0,
            "quality_score": 0.95,
            "status": "completed",
        })
        print(f"\nRun completed.  Metadata written.")

        # List runs
        runs = list_runs(base)
        print(f"\nRuns in index: {len(runs)}")
        for r in runs:
            print(f"  - {r['run_id']}  ({r['status']}, {r['duration_seconds']}s)")

        # Get latest
        latest = get_latest_run(base)
        print(f"\nLatest run: {latest.run_id if latest else 'None'}")

        # Get by id
        reloaded = get_run(ctx.run_id, base)
        print(f"Reloaded  : {reloaded.run_id if reloaded else 'None'}")
