"""Route LLM requests to the cheapest working model from env_info_clean.json.

The :class:`ModelRouter` loads the project's ``env_info_clean.json`` once on
initialisation and exposes helpers that return validated :class:`ModelConfig`
objects for different use-cases (cheap chat, reasoning, Claude re-profiling).
It includes automatic fallback logic and a lightweight health-check.
"""

from __future__ import annotations

import json
import logging
from enum import Enum
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants -- priority lists (cheapest / fastest first)
# ---------------------------------------------------------------------------

_CHEAP_CHAT_PRIORITY: list[str] = [
    "deepseek_v3_1",
    "deepseek_v3_2_speciale",
    "kimi_k2_5",
    "gpt_oss_120b",
    "deepseek_r1",
]

_REASONING_PRIORITY: list[str] = [
    "kimi_k2_5",
    "gpt_oss_120b",
    "deepseek_r1",
    "kimi_k2_thinking",
]

_CLAUDE_TIER_MAP: dict[str, str] = {
    "haiku": "claude_haiku_4_5",
    "sonnet": "claude_sonnet_4_5",
    "opus": "claude_opus_4_5",
}

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class ModelType(str, Enum):
    """Whether the model returns a reasoning chain or a direct response."""

    chat = "chat"
    reasoning = "reasoning"


class ModelConfig(BaseModel):
    """Fully-resolved configuration for a single LLM endpoint.

    Carries everything needed to instantiate an HTTP call or a LangChain
    ``init_chat_model`` invocation.
    """

    provider: str
    model_name: str
    deployment_name: str
    endpoint: str
    api_key: str
    auth_header: str
    model_type: ModelType
    api_version: str | None = None
    max_tokens: int | None = None
    required_headers: dict[str, str] = Field(default_factory=dict)
    notes: str | None = None


# ---------------------------------------------------------------------------
# ModelRouter
# ---------------------------------------------------------------------------


class ModelRouter:
    """Select the best available model for a given task.

    Parameters
    ----------
    env_info_path:
        Path to ``env_info_clean.json``.  Defaults to the canonical project
        location.
    """

    def __init__(
        self,
        env_info_path: str | Path = Path(__file__).resolve().parents[2] / "env_info_clean.json",
    ) -> None:
        self._path = Path(env_info_path)
        if not self._path.exists():
            raise FileNotFoundError(f"env_info_clean.json not found at {self._path}")

        self._raw: dict[str, Any] = json.loads(
            self._path.read_text(encoding="utf-8")
        )
        self._llm: dict[str, Any] = self._raw.get("llm_config", {})

        # Pre-build lookup: model_key -> ModelConfig
        self._models: dict[str, ModelConfig] = {}
        self._build_model_index()

        logger.info(
            "ModelRouter initialised with %d working models from %s",
            len(self._models),
            self._path,
        )

    # ----- internal builders -----

    def _build_model_index(self) -> None:
        """Walk the llm_config tree and index every *working* model."""
        self._index_ai_foundry()
        self._index_azure_openai()
        self._index_anthropic_azure()

    def _index_ai_foundry(self) -> None:
        """Index models under ``azure_ai_foundry``."""
        section = self._llm.get("azure_ai_foundry", {})
        endpoint = section.get("endpoint", "")
        api_key = section.get("api_key", "")
        auth_header = section.get("auth_header", "")

        for key, info in section.get("models", {}).items():
            if info.get("status") != "working":
                continue
            self._models[key] = ModelConfig(
                provider="azure_ai_foundry",
                model_name=info["model_name"],
                deployment_name=info["deployment_name"],
                endpoint=endpoint,
                api_key=api_key,
                auth_header=auth_header,
                model_type=ModelType(info.get("type", "chat")),
                notes=info.get("notes"),
            )

    def _index_azure_openai(self) -> None:
        """Index models under ``azure_openai``."""
        section = self._llm.get("azure_openai", {})
        endpoint = section.get("endpoint", "")
        api_key = section.get("api_key", "")
        auth_header = section.get("auth_header", "")
        api_version = section.get("api_version")

        for key, info in section.get("models", {}).items():
            if info.get("status") != "working":
                continue
            self._models[key] = ModelConfig(
                provider="azure_openai",
                model_name=info.get("model_id", info["deployment_name"]),
                deployment_name=info["deployment_name"],
                endpoint=endpoint,
                api_key=api_key,
                auth_header=auth_header,
                model_type=ModelType(info.get("type", "chat")),
                api_version=api_version,
                max_tokens=info.get("max_tokens"),
                notes=info.get("notes"),
            )

    def _index_anthropic_azure(self) -> None:
        """Index models under ``anthropic_azure``."""
        section = self._llm.get("anthropic_azure", {})
        endpoint = section.get("endpoint", "")
        api_key = section.get("api_key", "")
        auth_header = section.get("auth_header", "")
        required_headers = section.get("required_headers", {})

        for key, info in section.get("models", {}).items():
            if info.get("status") != "working":
                continue
            self._models[key] = ModelConfig(
                provider="anthropic_azure",
                model_name=info.get("model_id", info["model_name"]),
                deployment_name=info["deployment_name"],
                endpoint=endpoint,
                api_key=api_key,
                auth_header=auth_header,
                model_type=ModelType(info.get("type", "chat")),
                max_tokens=info.get("max_tokens"),
                required_headers=required_headers,
                notes=info.get("notes"),
            )

    # ----- private helpers -----

    def _resolve_first_working(self, priority: list[str]) -> ModelConfig:
        """Return the first model in *priority* that exists in the index.

        Parameters
        ----------
        priority:
            Ordered list of model keys (cheapest first).

        Raises
        ------
        RuntimeError
            If none of the models in the priority list are available.
        """
        for key in priority:
            if key in self._models:
                return self._models[key]
        available = list(self._models.keys())
        raise RuntimeError(
            f"No working model found in priority list {priority}. "
            f"Available models: {available}"
        )

    # ----- public API: model selection -----

    def get_cheapest_chat_model(self) -> ModelConfig:
        """Return the cheapest working chat-oriented model.

        Priority: DeepSeek-V3.1 > DeepSeek-V3.2-Speciale > Kimi-K2.5 >
        gpt-oss-120b > DeepSeek-R1.
        """
        return self._resolve_first_working(_CHEAP_CHAT_PRIORITY)

    def get_reasoning_model(self) -> ModelConfig:
        """Return the cheapest reasoning model for complex multi-step tasks.

        Priority: Kimi-K2.5 > gpt-oss-120b > DeepSeek-R1 > Kimi-K2-Thinking.
        """
        return self._resolve_first_working(_REASONING_PRIORITY)

    def get_claude_model(self, tier: str = "sonnet") -> ModelConfig:
        """Return a Claude model for re-profiling or high-quality tasks.

        Parameters
        ----------
        tier:
            One of ``"haiku"``, ``"sonnet"``, or ``"opus"``.

        Raises
        ------
        ValueError
            If *tier* is not recognised.
        RuntimeError
            If the requested Claude model is not available.
        """
        tier_lower = tier.lower()
        key = _CLAUDE_TIER_MAP.get(tier_lower)
        if key is None:
            raise ValueError(
                f"Unknown Claude tier '{tier}'. Choose from: {list(_CLAUDE_TIER_MAP.keys())}"
            )
        if key not in self._models:
            raise RuntimeError(
                f"Claude {tier} (key={key}) not found in working models. "
                f"Available: {list(self._models.keys())}"
            )
        return self._models[key]

    def get_model_by_key(self, key: str) -> ModelConfig:
        """Return a specific model by its internal key.

        Raises
        ------
        KeyError
            If the key is not in the working-model index.
        """
        if key not in self._models:
            raise KeyError(
                f"Model key '{key}' not found. Available: {list(self._models.keys())}"
            )
        return self._models[key]

    def list_models(self) -> dict[str, ModelConfig]:
        """Return the full dict of available model keys to ``ModelConfig``."""
        return dict(self._models)

    # ----- public API: LangChain integration -----

    @staticmethod
    def get_langchain_model_string(config: ModelConfig) -> str:
        """Return the string suitable for ``langchain.chat_models.init_chat_model()``.

        The format depends on the provider:

        * **azure_ai_foundry** -- ``"azure_ai_foundry/<deployment_name>"``
          (uses the OpenAI-compatible endpoint via ``langchain-openai``).
        * **azure_openai** -- ``"azure_openai/<deployment_name>"``
        * **anthropic_azure** -- ``"anthropic/<deployment_name>"``

        The caller must also pass the relevant ``api_key``, ``base_url``,
        and ``api_version`` kwargs when calling ``init_chat_model``.
        """
        provider = config.provider

        if provider == "azure_ai_foundry":
            # LangChain's AzureOpenAI / OpenAI wrappers accept this
            return f"openai/{config.deployment_name}"

        if provider == "azure_openai":
            return f"azure_openai/{config.deployment_name}"

        if provider == "anthropic_azure":
            return f"anthropic/{config.deployment_name}"

        # Fallback -- generic
        return f"{provider}/{config.deployment_name}"

    # ----- public API: health check -----

    def test_model(self, config: ModelConfig) -> bool:
        """Send a lightweight *ping* request and verify the model responds.

        Parameters
        ----------
        config:
            The :class:`ModelConfig` to test.

        Returns
        -------
        bool
            ``True`` if the model returned a non-error response, ``False``
            otherwise.  Errors are logged but never raised.
        """
        try:
            headers = _build_headers(config)
            payload = _build_ping_payload(config)
            url = _build_url(config)

            with httpx.Client(timeout=30.0) as client:
                resp = client.post(url, headers=headers, json=payload)

            if resp.status_code < 400:
                logger.info(
                    "Model %s (%s) is healthy (HTTP %d).",
                    config.model_name,
                    config.provider,
                    resp.status_code,
                )
                return True

            logger.warning(
                "Model %s returned HTTP %d: %s",
                config.model_name,
                resp.status_code,
                resp.text[:300],
            )
            return False

        except Exception:
            logger.exception("Health check failed for %s", config.model_name)
            return False

    def get_cheapest_chat_model_with_fallback(self) -> ModelConfig:
        """Like :meth:`get_cheapest_chat_model` but validates with a health check.

        Walks the priority list, running :meth:`test_model` on each candidate.
        Returns the first model that passes.

        Raises
        ------
        RuntimeError
            If every model in the priority list fails the health check.
        """
        for key in _CHEAP_CHAT_PRIORITY:
            cfg = self._models.get(key)
            if cfg is None:
                continue
            if self.test_model(cfg):
                return cfg
            logger.warning(
                "Model %s failed health check; trying next.", cfg.model_name
            )
        raise RuntimeError(
            "All chat models in the priority list failed health checks."
        )

    def get_reasoning_model_with_fallback(self) -> ModelConfig:
        """Like :meth:`get_reasoning_model` but validates with a health check.

        Raises
        ------
        RuntimeError
            If every model in the reasoning priority list fails.
        """
        for key in _REASONING_PRIORITY:
            cfg = self._models.get(key)
            if cfg is None:
                continue
            if self.test_model(cfg):
                return cfg
            logger.warning(
                "Model %s failed health check; trying next.", cfg.model_name
            )
        raise RuntimeError(
            "All reasoning models in the priority list failed health checks."
        )


# ---------------------------------------------------------------------------
# Module-level helpers for HTTP construction
# ---------------------------------------------------------------------------


def _build_headers(config: ModelConfig) -> dict[str, str]:
    """Construct the HTTP headers required by the model endpoint."""
    headers: dict[str, str] = {"Content-Type": "application/json"}

    # Parse auth_header template -- e.g. "Authorization: Bearer <api_key>"
    if config.auth_header and config.api_key:
        auth_value = config.auth_header.replace("<api_key>", config.api_key)
        if ":" in auth_value:
            header_name, _, header_val = auth_value.partition(":")
            headers[header_name.strip()] = header_val.strip()
        else:
            headers["Authorization"] = f"Bearer {config.api_key}"

    # Add any required extra headers (e.g. anthropic-version)
    headers.update(config.required_headers)

    return headers


def _build_url(config: ModelConfig) -> str:
    """Build the full request URL for the model."""
    if config.provider == "azure_openai":
        # Azure OpenAI uses: {endpoint}openai/deployments/{deployment}/chat/completions?api-version=...
        base = config.endpoint.rstrip("/")
        version = config.api_version or "2024-12-01-preview"
        return (
            f"{base}/openai/deployments/{config.deployment_name}"
            f"/chat/completions?api-version={version}"
        )

    if config.provider == "anthropic_azure":
        # Anthropic via Azure uses the endpoint directly (already includes /anthropic/v1/messages)
        return config.endpoint

    # AI Foundry (and generic) -- the endpoint already points at /chat/completions
    return config.endpoint


def _build_ping_payload(config: ModelConfig) -> dict[str, Any]:
    """Build a minimal chat-completion payload for the health check."""
    if config.provider == "anthropic_azure":
        return {
            "model": config.deployment_name,
            "max_tokens": 16,
            "messages": [{"role": "user", "content": "ping"}],
        }

    payload: dict[str, Any] = {
        "model": config.deployment_name,
        "messages": [{"role": "user", "content": "ping"}],
    }

    # Reasoning models on Azure OpenAI use max_completion_tokens
    if config.provider == "azure_openai":
        payload["max_completion_tokens"] = 16
    else:
        payload["max_tokens"] = 16

    return payload
