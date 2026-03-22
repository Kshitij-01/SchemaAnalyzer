"""Human-readable markdown trace log for SchemaAnalyzer runs.

Every significant event during a run -- phase transitions, agent
actions, decisions, metrics, errors -- is appended to a single
``run_log.md`` file inside the run directory.  The result is an
audit-friendly, grep-friendly narrative that can be read in any
markdown viewer.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class RunLogger:
    """Writes a human-readable run trace to ``run_log.md``.

    Usage::

        logger = RunLogger(run_dir=ctx.run_dir, run_id=ctx.run_id)
        logger.log_phase("Discovery")
        logger.log_agent_action("Profiler", "Scanning tables", "found 42 tables")
        logger.log_metric("tables_found", 42)
        logger.finalize({"tables": 42, "quality": 0.95})
    """

    def __init__(self, run_dir: Path, run_id: str = "") -> None:
        self._path: Path = run_dir / "run_log.md"
        self._run_id: str = run_id
        self._phase_count: int = 0
        self._action_count: int = 0
        # Write header immediately so the file exists from the start
        self._write(f"# Run Log: {run_id}\n\n**Started**: {self._now()}\n\n")

    # ------------------------------------------------------------------
    # Public logging methods
    # ------------------------------------------------------------------

    def log_phase(self, name: str) -> None:
        """Start a new phase (rendered as a ``##`` heading)."""
        self._phase_count += 1
        self._write(f"\n---\n\n## Phase {self._phase_count}: {name}\n\n")

    def log_agent_action(
        self, agent: str, action: str, details: str = ""
    ) -> None:
        """Log an agent action as a timestamped bullet point."""
        self._action_count += 1
        line = f"- [{self._time()}] **{agent}**: {action}"
        if details:
            line += f"\n  - {details}"
        self._write(line + "\n")

    def log_decision(self, description: str) -> None:
        """Log a decision made by the system (italic text)."""
        self._write(f"  _Decision: {description}_\n")

    def log_metric(self, name: str, value: Any) -> None:
        """Log a single metric value (code-formatted name)."""
        self._write(f"  - `{name}`: {value}\n")

    def log_error(self, agent: str, error: str) -> None:
        """Log an error with a warning marker."""
        self._write(
            f"- [{self._time()}] **{agent}** \u26a0\ufe0f ERROR: {error}\n"
        )

    def log_table(self, headers: list[str], rows: list[list[str]]) -> None:
        """Log a markdown table.

        Parameters
        ----------
        headers:
            Column names.
        rows:
            Each inner list must have the same length as *headers*.
        """
        header_line = "| " + " | ".join(headers) + " |"
        sep_line = "| " + " | ".join(["---"] * len(headers)) + " |"
        lines = [header_line, sep_line]
        for row in rows:
            lines.append("| " + " | ".join(str(c) for c in row) + " |")
        self._write("\n" + "\n".join(lines) + "\n\n")

    def finalize(self, summary: dict[str, Any] | None = None) -> None:
        """Write the closing summary section.

        Parameters
        ----------
        summary:
            Optional key/value pairs rendered as a bulleted list under
            a *Summary* sub-heading.
        """
        self._write("\n---\n\n## Run Complete\n\n")
        self._write(f"**Finished**: {self._now()}\n")
        self._write(f"**Total Phases**: {self._phase_count}\n")
        self._write(f"**Total Actions**: {self._action_count}\n")
        if summary:
            self._write("\n### Summary\n\n")
            for k, v in summary.items():
                self._write(f"- **{k}**: {v}\n")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _write(self, text: str) -> None:
        """Append *text* to the log file."""
        with self._path.open("a", encoding="utf-8") as f:
            f.write(text)

    @staticmethod
    def _now() -> str:
        """Return the current UTC time as an ISO-8601 string."""
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _time() -> str:
        """Return the current UTC time as ``HH:MM:SS``."""
        return datetime.now(timezone.utc).strftime("%H:%M:%S")


# ---------------------------------------------------------------------------
# Quick smoke-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        run_dir = Path(tmp)
        logger = RunLogger(run_dir=run_dir, run_id="run_20260322_193000_demo")

        logger.log_phase("Discovery")
        logger.log_agent_action("Profiler", "Connecting to source", "host=localhost db=test")
        logger.log_agent_action("Profiler", "Enumerating tables")
        logger.log_metric("tables_found", 42)
        logger.log_decision("Skipping empty tables (threshold < 1 row)")

        logger.log_phase("Profiling")
        logger.log_agent_action("Profiler", "Profiling table", "users (12 columns)")
        logger.log_agent_action("Profiler", "Profiling table", "orders (8 columns)")
        logger.log_error("Profiler", "Timeout on legacy_archive (>60s)")

        logger.log_table(
            headers=["Table", "Columns", "Rows", "Status"],
            rows=[
                ["users", "12", "1500", "OK"],
                ["orders", "8", "45000", "OK"],
                ["legacy_archive", "31", "?", "TIMEOUT"],
            ],
        )

        logger.log_phase("Reporting")
        logger.log_agent_action("Reporter", "Generating summary report")

        logger.finalize(summary={
            "Tables profiled": "2 / 3",
            "Quality score": 0.87,
            "Duration": "4.2s",
        })

        # Print the resulting log (errors="replace" for Windows consoles
        # that cannot render all Unicode characters)
        import sys
        log_path = run_dir / "run_log.md"
        text = log_path.read_text(encoding="utf-8")
        sys.stdout.buffer.write(text.encode("utf-8", errors="replace"))
        sys.stdout.buffer.flush()
