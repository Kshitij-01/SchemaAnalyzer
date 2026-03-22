"""Parse user input (natural text, JSON, or .env files) to extract database credentials.

This module provides utilities for converting free-form user descriptions of
database connections into validated ``SourceConfig`` Pydantic models.  It also
supports loading credentials directly from the project's ``env_info_clean.json``.
"""

from __future__ import annotations

import json
import logging
import re
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_PORTS: dict[str, int] = {
    "postgres": 5432,
    "mysql": 3306,
    "snowflake": 443,
    "bigquery": 443,
    "azure_blob": 443,
    "s3": 443,
    "teradata": 1025,
}

# Regex helpers for natural-language extraction
_KV_PATTERNS: dict[str, re.Pattern[str]] = {
    "host": re.compile(
        r"(?:host(?:name)?|server|endpoint)\s*(?:is|=|:)\s*['\"]?([^\s,;'\"]+)",
        re.IGNORECASE,
    ),
    "port": re.compile(
        r"(?:port)\s*(?:is|=|:)\s*['\"]?(\d+)",
        re.IGNORECASE,
    ),
    "user": re.compile(
        r"(?:user(?:name)?|login)\s*(?:is|=|:)\s*['\"]?([^\s,;'\"]+)",
        re.IGNORECASE,
    ),
    "password": re.compile(
        r"(?:password|pass|pwd|secret)\s*(?:is|=|:)\s*['\"]?([^\s,;'\"]+)",
        re.IGNORECASE,
    ),
    "database": re.compile(
        r"(?:database|db|dbname|catalog)\s*(?:is|=|:)\s*['\"]?([^\s,;'\"]+)",
        re.IGNORECASE,
    ),
    "schema": re.compile(
        r"(?:schema)\s*(?:is|=|:)\s*['\"]?([^\s,;'\"]+)",
        re.IGNORECASE,
    ),
    "warehouse": re.compile(
        r"(?:warehouse|wh)\s*(?:is|=|:)\s*['\"]?([^\s,;'\"]+)",
        re.IGNORECASE,
    ),
    "account": re.compile(
        r"(?:account)\s*(?:is|=|:)\s*['\"]?([^\s,;'\"]+)",
        re.IGNORECASE,
    ),
    "container": re.compile(
        r"(?:container|bucket)\s*(?:is|=|:)\s*['\"]?([^\s,;'\"]+)",
        re.IGNORECASE,
    ),
}

_SOURCE_TYPE_ALIASES: dict[str, str] = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "pg": "postgres",
    "mysql": "mysql",
    "mariadb": "mysql",
    "snowflake": "snowflake",
    "sf": "snowflake",
    "bigquery": "bigquery",
    "bq": "bigquery",
    "azure_blob": "azure_blob",
    "azureblob": "azure_blob",
    "azure blob": "azure_blob",
    "blob": "azure_blob",
    "s3": "s3",
    "aws_s3": "s3",
    "teradata": "teradata",
    "td": "teradata",
}


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class SourceType(str, Enum):
    """Supported database / storage source types."""

    postgres = "postgres"
    snowflake = "snowflake"
    mysql = "mysql"
    azure_blob = "azure_blob"
    s3 = "s3"
    bigquery = "bigquery"
    teradata = "teradata"


class SourceConfig(BaseModel):
    """Validated configuration for a single data-source connection.

    Fields that are only relevant for certain source types (e.g. ``warehouse``
    for Snowflake) are optional and default to ``None``.
    """

    source_type: SourceType
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    database: str | None = None
    schema_name: str | None = Field(None, alias="schema")
    warehouse: str | None = None          # Snowflake
    account: str | None = None            # Snowflake
    container: str | None = None          # Azure Blob
    extra: dict[str, Any] = Field(default_factory=dict)

    model_config = {"populate_by_name": True}

    # ----- validators -----

    @model_validator(mode="after")
    def _apply_defaults(self) -> SourceConfig:
        """Fill in sensible defaults where possible."""
        # Default port based on source type
        if self.port is None:
            self.port = _DEFAULT_PORTS.get(self.source_type.value)

        # Snowflake requires account
        if self.source_type == SourceType.snowflake and not self.account:
            logger.warning(
                "Snowflake source is missing 'account'. Connection will likely fail."
            )

        return self

    # ----- convenience -----

    def masked_repr(self) -> str:
        """Return a human-readable string with the password masked."""
        parts = [f"type={self.source_type.value}"]
        if self.host:
            parts.append(f"host={self.host}")
        if self.port:
            parts.append(f"port={self.port}")
        if self.user:
            parts.append(f"user={self.user}")
        if self.password:
            parts.append("password=****")
        if self.database:
            parts.append(f"database={self.database}")
        if self.schema_name:
            parts.append(f"schema={self.schema_name}")
        if self.warehouse:
            parts.append(f"warehouse={self.warehouse}")
        if self.account:
            parts.append(f"account={self.account}")
        return f"SourceConfig({', '.join(parts)})"

    def __repr__(self) -> str:  # noqa: D105
        return self.masked_repr()


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _detect_source_type(text: str) -> SourceType | None:
    """Try to identify the source type from a chunk of text."""
    lower = text.lower()
    for alias, canonical in _SOURCE_TYPE_ALIASES.items():
        if alias in lower:
            return SourceType(canonical)
    return None


def _extract_fields(text: str) -> dict[str, str]:
    """Extract key-value credential fields from natural text."""
    extracted: dict[str, str] = {}
    for field_name, pattern in _KV_PATTERNS.items():
        match = pattern.search(text)
        if match:
            extracted[field_name] = match.group(1).strip().rstrip(".,;")
    return extracted


def _fields_to_source_config(
    source_type: SourceType,
    fields: dict[str, Any],
) -> SourceConfig:
    """Build a ``SourceConfig`` from a field dict and source type."""
    known_keys = {
        "host", "port", "user", "password", "database",
        "schema", "warehouse", "account", "container",
    }
    config_kwargs: dict[str, Any] = {"source_type": source_type}
    extra: dict[str, Any] = {}

    for key, value in fields.items():
        if key in known_keys:
            if key == "port":
                try:
                    value = int(value)
                except (ValueError, TypeError):
                    logger.warning("Non-integer port value '%s'; skipping.", value)
                    continue
            config_kwargs[key] = value
        else:
            extra[key] = value

    config_kwargs["extra"] = extra
    return SourceConfig(**config_kwargs)


def _split_source_blocks(text: str) -> list[str]:
    """Split input text into per-source blocks.

    Heuristic: if the text mentions multiple source type keywords separated by
    newlines or punctuation, split on those boundaries.  Otherwise return the
    whole text as a single block.
    """
    type_keywords = "|".join(re.escape(a) for a in _SOURCE_TYPE_ALIASES)
    pattern = re.compile(
        rf"(?:^|\n)(?=.*?\b(?:{type_keywords})\b)",
        re.IGNORECASE | re.MULTILINE,
    )
    positions = [m.start() for m in pattern.finditer(text)]
    if len(positions) <= 1:
        return [text]

    blocks: list[str] = []
    for i, pos in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else len(text)
        block = text[pos:end].strip()
        if block:
            blocks.append(block)
    return blocks


# ---------------------------------------------------------------------------
# Structured input parsing (JSON / .env)
# ---------------------------------------------------------------------------


def _parse_json_input(raw: str) -> list[SourceConfig]:
    """Parse JSON text that is either a list of configs or a single config dict."""
    data = json.loads(raw)

    if isinstance(data, list):
        configs: list[SourceConfig] = []
        for item in data:
            st = _detect_source_type(json.dumps(item))
            if "source_type" in item:
                st = SourceType(
                    _SOURCE_TYPE_ALIASES.get(item["source_type"].lower(), item["source_type"].lower())
                )
            if st is None:
                logger.warning("Could not determine source type for JSON item: %s", item)
                continue
            configs.append(_fields_to_source_config(st, item))
        return configs

    if isinstance(data, dict):
        # Could be keyed by source type (like env_info_clean.json database_credentials)
        configs = []
        for key, value in data.items():
            if isinstance(value, dict):
                st_alias = _SOURCE_TYPE_ALIASES.get(key.lower())
                if st_alias:
                    configs.append(
                        _fields_to_source_config(SourceType(st_alias), value)
                    )
        if configs:
            return configs

        # Single flat object
        st = None
        if "source_type" in data:
            st = SourceType(
                _SOURCE_TYPE_ALIASES.get(data["source_type"].lower(), data["source_type"].lower())
            )
        if st is None:
            st = _detect_source_type(json.dumps(data))
        if st is None:
            raise ValueError("Cannot determine source type from JSON input.")
        return [_fields_to_source_config(st, data)]

    raise ValueError(f"Unexpected JSON structure: {type(data).__name__}")


def _parse_env_text(text: str) -> list[SourceConfig]:
    """Parse .env file contents (KEY=VALUE lines) into source configs.

    Groups variables by common prefixes like ``POSTGRES_``, ``SNOWFLAKE_``.
    """
    env_vars: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        env_vars[key.strip()] = value.strip().strip("'\"")

    # Group by prefix
    prefix_groups: dict[str, dict[str, str]] = {}
    for key, value in env_vars.items():
        parts = key.split("_", 1)
        prefix = parts[0].lower()
        field = parts[1].lower() if len(parts) > 1 else key.lower()
        prefix_groups.setdefault(prefix, {})[field] = value

    configs: list[SourceConfig] = []
    for prefix, fields in prefix_groups.items():
        st_alias = _SOURCE_TYPE_ALIASES.get(prefix)
        if st_alias is None:
            # Try the prefix as-is in source types
            continue
        configs.append(_fields_to_source_config(SourceType(st_alias), fields))

    return configs


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_credentials(user_input: str) -> list[SourceConfig]:
    """Parse user input text into a list of validated ``SourceConfig`` objects.

    The input can be:
    * **Natural language** -- e.g. ``"My postgres is at host.com, user admin,
      password secret123, database mydb"``
    * **JSON** -- a JSON object or array describing one or more sources.
    * **.env contents** -- ``KEY=VALUE`` lines grouped by prefix.

    Multiple sources can be described in a single text block; the parser will
    attempt to split them automatically.

    Parameters
    ----------
    user_input:
        Free-form text, JSON, or .env content describing database credentials.

    Returns
    -------
    list[SourceConfig]
        Validated source configurations.  May be empty if nothing could be
        parsed.

    Examples
    --------
    >>> configs = parse_credentials(
    ...     "My postgres is at db.example.com, user admin, password s3cret, database analytics"
    ... )
    >>> configs[0].source_type
    <SourceType.postgres: 'postgres'>
    >>> configs[0].host
    'db.example.com'
    """
    text = user_input.strip()
    if not text:
        return []

    # ---- Try JSON first ----
    if text.startswith(("{", "[")):
        try:
            return _parse_json_input(text)
        except (json.JSONDecodeError, ValueError):
            logger.debug("Input looks like JSON but failed to parse; falling back.")

    # ---- Try .env format ----
    env_like_lines = sum(
        1
        for line in text.splitlines()
        if "=" in line and not line.strip().startswith("#")
    )
    total_lines = max(len(text.splitlines()), 1)
    if env_like_lines / total_lines > 0.5:
        configs = _parse_env_text(text)
        if configs:
            return configs

    # ---- Natural language parsing ----
    blocks = _split_source_blocks(text)
    configs: list[SourceConfig] = []

    for block in blocks:
        source_type = _detect_source_type(block)
        if source_type is None:
            logger.warning("Could not identify source type in block: %s", block[:80])
            continue
        fields = _extract_fields(block)
        if not fields:
            logger.warning("No credential fields found in block: %s", block[:80])
            continue
        configs.append(_fields_to_source_config(source_type, fields))

    return configs


def load_from_env_info(path: str | Path) -> list[SourceConfig]:
    """Load database credentials from ``env_info_clean.json``.

    Reads the ``database_credentials`` section and returns a ``SourceConfig``
    for each entry.

    Parameters
    ----------
    path:
        Path to ``env_info_clean.json``.

    Returns
    -------
    list[SourceConfig]
        One config per credential block found in the file.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    KeyError
        If the file does not contain a ``database_credentials`` key.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"env_info file not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    creds = data.get("database_credentials")
    if creds is None:
        raise KeyError(
            f"'database_credentials' key not found in {path}. "
            f"Top-level keys: {list(data.keys())}"
        )

    configs: list[SourceConfig] = []
    for key, value in creds.items():
        if not isinstance(value, dict):
            logger.warning("Skipping non-dict credential entry: %s", key)
            continue
        st_alias = _SOURCE_TYPE_ALIASES.get(key.lower())
        if st_alias is None:
            logger.warning(
                "Unknown source type '%s' in database_credentials; skipping.", key
            )
            continue
        configs.append(_fields_to_source_config(SourceType(st_alias), value))

    logger.info("Loaded %d source config(s) from %s", len(configs), path)
    return configs
