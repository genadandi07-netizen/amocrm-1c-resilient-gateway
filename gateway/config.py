"""Runtime configuration for the gateway.

Every tunable value (endpoints, timeouts, token-bucket limits, database paths,
leases, retry budgets) is declared here exactly once and validated by
``pydantic-settings`` at process start. Business modules never embed literals:
they receive a :class:`Settings` instance (or a value copied from it).

Loading precedence, highest first:

1. explicit ``env`` mapping passed to :meth:`Settings.from_env` (tests);
2. process environment (``GATEWAY_*``);
3. ``config/gateway.json`` for the four resilience sections;
4. safe defaults declared in this module.

The module is intentionally side-effect free: importing it never opens a
database or a socket.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_CONFIG_PATH = str(PROJECT_ROOT / "config" / "gateway.json")

# --- safe, documented defaults -------------------------------------------
# These are fallbacks for local development only. Production values belong in
# ``.env`` (see .env.example) and are validated when the process starts.
DEFAULT_HMAC_SECRET = "change-me-in-production"
DEFAULT_ONEC_API_TOKEN = ""
DEFAULT_DB_PATH = str(PROJECT_ROOT / "gateway.db")
DEFAULT_ONEC_BASE_URL = "https://1c.example"
DEFAULT_AMOCRM_BASE_URL = "https://amocrm.example"
DEFAULT_AMOCRM_ACCESS_TOKEN = ""
DEFAULT_AMOCRM_REQUESTS_PER_SECOND = 100
DEFAULT_N8N_DLQ_URL = ""
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000
DEFAULT_BUSY_TIMEOUT_MS = 5000
DEFAULT_DB_CONNECT_TIMEOUT_SECONDS = 5.0
DEFAULT_DISPATCH_BATCH_LIMIT = 50
DEFAULT_ONEC_RATE_LIMIT_RPS = 2.0
DEFAULT_ONEC_RATE_LIMIT_BURST = 5
DEFAULT_AMOCRM_RATE_LIMIT_RPS = 2.0
DEFAULT_AMOCRM_RATE_LIMIT_BURST = 5

# Endpoints / transport tuning. Kept in configuration so a 1C publication or a
# new amoCRM API version can be adopted without a code change.
DEFAULT_ONEC_ORDER_PATH = "/odata/standard.odata/Document_ЗаказПокупателя"
DEFAULT_ONEC_COUNTERPARTY_PATH = "/odata/standard.odata/Catalog_Контрагенты"
DEFAULT_AMOCRM_LEADS_PATH = "/api/v4/leads"
DEFAULT_ONEC_TIMEOUT_SECONDS = 15.0
DEFAULT_POLL_INTERVAL_SECONDS = 2.0
DEFAULT_DISPATCH_LEASE_SECONDS = 300.0
DEFAULT_RATE_LIMIT_MAX_TRACKED_IPS = 10_000
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_JSON = True

# SQLite lock contention policy (see state_store.connect/transaction).
DEFAULT_LOCK_RETRY_ATTEMPTS = 6
DEFAULT_LOCK_RETRY_BASE_SECONDS = 0.05
DEFAULT_LOCK_RETRY_MAX_SECONDS = 0.5

# Mirror of config/gateway.json, used as the base for from_env().
DEFAULT_CONFIG_DATA: dict[str, dict[str, Any]] = {
    "circuit_breaker": {
        "failure_threshold": 5,
        "cooldown_seconds": 30.0,
        "half_open_probe_interval": 5.0,
    },
    "replay_guard": {
        "timestamp_window_seconds": 300,
        "nonce_journal_size": 10000,
    },
    "backoff": {
        "base_seconds": 0.5,
        "multiplier": 2.0,
        "max_seconds": 30.0,
        "jitter_ratio": 1.0,
        "max_attempts": 5,
    },
    "rate_limit": {
        "algorithm": "token_bucket",
        "requests_per_window": 100,
        "window_seconds": 60,
    },
}


class CircuitBreakerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    failure_threshold: int = Field(gt=0)
    cooldown_seconds: float = Field(gt=0)
    half_open_probe_interval: float = Field(gt=0)


class ReplayGuardConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    timestamp_window_seconds: int = Field(gt=0)
    nonce_journal_size: int = Field(gt=0)


class BackoffConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    base_seconds: float = Field(gt=0)
    multiplier: float = Field(gt=1)
    max_seconds: float = Field(gt=0)
    jitter_ratio: float = Field(ge=0, le=1)
    max_attempts: int = Field(gt=0)

    @field_validator("max_seconds")
    @classmethod
    def _max_not_below_base(cls, value: float, info):
        base = info.data.get("base_seconds")
        if base is not None and value < base:
            raise ValueError("max_seconds must be >= base_seconds")
        return value


class RateLimitConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    algorithm: Literal["token_bucket", "sliding_window"]
    requests_per_window: int = Field(gt=0)
    window_seconds: int = Field(gt=0)


def _coerce(default: Any, raw: str) -> Any:
    if isinstance(default, bool):
        return str(raw).strip().lower() in ("1", "true", "yes", "on")
    if isinstance(default, int):
        return int(raw)
    if isinstance(default, float):
        return float(raw)
    return str(raw)


# Scalar settings: (environment variable, safe default). The same table drives
# ``from_env`` and the documentation in ``.env.example``.
SCALAR_ENV: dict[str, tuple[str, Any]] = {
    "hmac_secret": ("GATEWAY_HMAC_SECRET", DEFAULT_HMAC_SECRET),
    "one_c_api_token": ("GATEWAY_ONEC_API_TOKEN", DEFAULT_ONEC_API_TOKEN),
    "db_path": ("GATEWAY_DB_PATH", DEFAULT_DB_PATH),
    "one_c_base_url": ("GATEWAY_ONEC_BASE_URL", DEFAULT_ONEC_BASE_URL),
    "onec_order_path": ("GATEWAY_ONEC_ORDER_PATH", DEFAULT_ONEC_ORDER_PATH),
    "onec_counterparty_path": (
        "GATEWAY_ONEC_COUNTERPARTY_PATH",
        DEFAULT_ONEC_COUNTERPARTY_PATH,
    ),
    "onec_user": ("GATEWAY_ONEC_USER", ""),
    "onec_password": ("GATEWAY_ONEC_PASSWORD", ""),
    "onec_timeout_seconds": (
        "GATEWAY_ONEC_TIMEOUT_SECONDS",
        DEFAULT_ONEC_TIMEOUT_SECONDS,
    ),
    "amocrm_base_url": ("GATEWAY_AMOCRM_BASE_URL", DEFAULT_AMOCRM_BASE_URL),
    "amocrm_leads_path": ("GATEWAY_AMOCRM_LEADS_PATH", DEFAULT_AMOCRM_LEADS_PATH),
    "amocrm_access_token": (
        "GATEWAY_AMOCRM_ACCESS_TOKEN",
        DEFAULT_AMOCRM_ACCESS_TOKEN,
    ),
    "amocrm_requests_per_second": (
        "GATEWAY_AMOCRM_REQUESTS_PER_SECOND",
        DEFAULT_AMOCRM_REQUESTS_PER_SECOND,
    ),
    "n8n_dlq_url": ("GATEWAY_N8N_DLQ_URL", DEFAULT_N8N_DLQ_URL),
    "host": ("GATEWAY_HOST", DEFAULT_HOST),
    "port": ("GATEWAY_PORT", DEFAULT_PORT),
    "busy_timeout_ms": ("GATEWAY_BUSY_TIMEOUT_MS", DEFAULT_BUSY_TIMEOUT_MS),
    "db_connect_timeout_seconds": (
        "GATEWAY_DB_CONNECT_TIMEOUT_SECONDS",
        DEFAULT_DB_CONNECT_TIMEOUT_SECONDS,
    ),
    "db_lock_retry_attempts": (
        "GATEWAY_DB_LOCK_RETRY_ATTEMPTS",
        DEFAULT_LOCK_RETRY_ATTEMPTS,
    ),
    "db_lock_retry_base_seconds": (
        "GATEWAY_DB_LOCK_RETRY_BASE_SECONDS",
        DEFAULT_LOCK_RETRY_BASE_SECONDS,
    ),
    "db_lock_retry_max_seconds": (
        "GATEWAY_DB_LOCK_RETRY_MAX_SECONDS",
        DEFAULT_LOCK_RETRY_MAX_SECONDS,
    ),
    "dispatch_batch_limit": (
        "GATEWAY_DISPATCH_BATCH_LIMIT",
        DEFAULT_DISPATCH_BATCH_LIMIT,
    ),
    "dispatch_lease_seconds": (
        "GATEWAY_DISPATCH_LEASE_SECONDS",
        DEFAULT_DISPATCH_LEASE_SECONDS,
    ),
    "poll_interval_seconds": (
        "GATEWAY_POLL_INTERVAL",
        DEFAULT_POLL_INTERVAL_SECONDS,
    ),
    "rate_limit_max_tracked_ips": (
        "GATEWAY_RATE_LIMIT_MAX_TRACKED_IPS",
        DEFAULT_RATE_LIMIT_MAX_TRACKED_IPS,
    ),
    "log_level": ("GATEWAY_LOG_LEVEL", DEFAULT_LOG_LEVEL),
    "log_json": ("GATEWAY_LOG_JSON", DEFAULT_LOG_JSON),
    "onec_rate_limit_rps": (
        "GATEWAY_ONEC_RATE_LIMIT_RPS",
        DEFAULT_ONEC_RATE_LIMIT_RPS,
    ),
    "onec_rate_limit_burst": (
        "GATEWAY_ONEC_RATE_LIMIT_BURST",
        DEFAULT_ONEC_RATE_LIMIT_BURST,
    ),
    "amocrm_rate_limit_rps": (
        "GATEWAY_AMOCRM_RATE_LIMIT_RPS",
        DEFAULT_AMOCRM_RATE_LIMIT_RPS,
    ),
    "amocrm_rate_limit_burst": (
        "GATEWAY_AMOCRM_RATE_LIMIT_BURST",
        DEFAULT_AMOCRM_RATE_LIMIT_BURST,
    ),
}


class Settings(BaseSettings):
    """Validated, immutable gateway configuration.

    The canonical loaders are :meth:`from_env` (flat ``GATEWAY_*`` variables,
    used by the app and the worker) and :meth:`from_file` (``config/gateway.json``
    plus safe scalar defaults). ``BaseSettings`` also supports the nested
    ``GATEWAY_<SECTION>__<PARAM>`` form for deployments that provide the four
    resilience sections through environment variables.
    """

    model_config = SettingsConfigDict(
        env_prefix="GATEWAY_",
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="forbid",
        case_sensitive=False,
        validate_default=True,
        frozen=True,
    )

    circuit_breaker: CircuitBreakerConfig
    replay_guard: ReplayGuardConfig
    backoff: BackoffConfig
    rate_limit: RateLimitConfig

    hmac_secret: str = DEFAULT_HMAC_SECRET
    one_c_api_token: str = DEFAULT_ONEC_API_TOKEN
    db_path: str = DEFAULT_DB_PATH
    one_c_base_url: str = DEFAULT_ONEC_BASE_URL
    onec_order_path: str = DEFAULT_ONEC_ORDER_PATH
    onec_counterparty_path: str = DEFAULT_ONEC_COUNTERPARTY_PATH
    onec_user: str = ""
    onec_password: str = ""
    onec_timeout_seconds: float = Field(default=DEFAULT_ONEC_TIMEOUT_SECONDS, gt=0)
    amocrm_base_url: str = DEFAULT_AMOCRM_BASE_URL
    amocrm_leads_path: str = DEFAULT_AMOCRM_LEADS_PATH
    amocrm_access_token: str = DEFAULT_AMOCRM_ACCESS_TOKEN
    amocrm_requests_per_second: int = Field(
        default=DEFAULT_AMOCRM_REQUESTS_PER_SECOND, gt=0
    )
    n8n_dlq_url: str = DEFAULT_N8N_DLQ_URL
    host: str = DEFAULT_HOST
    port: int = Field(default=DEFAULT_PORT, gt=0, le=65535)
    busy_timeout_ms: int = Field(default=DEFAULT_BUSY_TIMEOUT_MS, ge=0)
    db_connect_timeout_seconds: float = Field(
        default=DEFAULT_DB_CONNECT_TIMEOUT_SECONDS, gt=0
    )
    db_lock_retry_attempts: int = Field(default=DEFAULT_LOCK_RETRY_ATTEMPTS, ge=1)
    db_lock_retry_base_seconds: float = Field(
        default=DEFAULT_LOCK_RETRY_BASE_SECONDS, gt=0
    )
    db_lock_retry_max_seconds: float = Field(
        default=DEFAULT_LOCK_RETRY_MAX_SECONDS, gt=0
    )
    dispatch_batch_limit: int = Field(default=DEFAULT_DISPATCH_BATCH_LIMIT, gt=0)
    dispatch_lease_seconds: float = Field(default=DEFAULT_DISPATCH_LEASE_SECONDS, gt=0)
    poll_interval_seconds: float = Field(default=DEFAULT_POLL_INTERVAL_SECONDS, gt=0)
    rate_limit_max_tracked_ips: int = Field(
        default=DEFAULT_RATE_LIMIT_MAX_TRACKED_IPS, gt=0
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_json: bool = DEFAULT_LOG_JSON

    # Outbound pacing (token bucket) for the 1C leg.
    onec_rate_limit_rps: float = Field(default=DEFAULT_ONEC_RATE_LIMIT_RPS, gt=0)
    onec_rate_limit_burst: int = Field(default=DEFAULT_ONEC_RATE_LIMIT_BURST, ge=1)
    # Outbound pacing (token bucket) for the amoCRM leg.
    amocrm_rate_limit_rps: float = Field(default=DEFAULT_AMOCRM_RATE_LIMIT_RPS, gt=0)
    amocrm_rate_limit_burst: int = Field(default=DEFAULT_AMOCRM_RATE_LIMIT_BURST, ge=1)

    @field_validator(
        "onec_order_path",
        "onec_counterparty_path",
        "amocrm_leads_path",
    )
    @classmethod
    def _endpoint_paths_are_absolute(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("endpoint paths must start with '/'")
        return value

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalize_log_level(cls, value: Any) -> str:
        return str(value).strip().upper()

    @classmethod
    def from_file(cls, path: str = DEFAULT_CONFIG_PATH) -> Settings:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        return cls.model_validate(data)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        """Build settings from ``env`` (or the process environment).

        The four resilience sections start from ``DEFAULT_CONFIG_DATA`` and the
        scalar fields start from their safe defaults, so calling
        ``Settings.from_env({})`` is fully deterministic and never reads the
        developer's ambient environment or ``.env``. With ``env=None`` the
        process environment wins over ``.env`` (matching Docker/pydantic order).
        """
        if env is None:
            source = dict(os.environ)
            dotenv_path = Path(".env")
            if dotenv_path.is_file():
                for key, value in dotenv_values(dotenv_path).items():
                    if value is not None and key not in source:
                        source[key] = value
        else:
            source = dict(env)
        data: dict[str, Any] = {
            section: dict(params) for section, params in DEFAULT_CONFIG_DATA.items()
        }
        for section, params in DEFAULT_CONFIG_DATA.items():
            for key, default in params.items():
                var = f"GATEWAY_{section.upper()}_{key.upper()}"
                if var in source:
                    data[section][key] = _coerce(default, source[var])
        for field, (var, default) in SCALAR_ENV.items():
            data[field] = _coerce(default, source[var]) if var in source else default
        # Compatibility: older deployments used one of these aliases for the
        # worker poll interval. The canonical name still wins when both are set.
        if "GATEWAY_POLL_INTERVAL" not in source:
            for alias in ("POLL_INTERVAL", "GATEWAY_WORKER_INTERVAL_SECONDS"):
                if alias in source:
                    data["poll_interval_seconds"] = _coerce(
                        DEFAULT_POLL_INTERVAL_SECONDS, source[alias]
                    )
                    break
        return cls.model_validate(data)

    def production_warnings(self) -> list[str]:
        """Non-fatal audit findings to log at startup (never at import time)."""
        warnings: list[str] = []
        if self.hmac_secret == DEFAULT_HMAC_SECRET:
            warnings.append(
                "GATEWAY_HMAC_SECRET is the shipped placeholder; rotate it before production"
            )
        if not self.one_c_api_token:
            warnings.append("GATEWAY_ONEC_API_TOKEN is empty: 1C token ingress is disabled")
        if not self.amocrm_access_token:
            warnings.append("GATEWAY_AMOCRM_ACCESS_TOKEN is empty")
        if not self.n8n_dlq_url:
            warnings.append("GATEWAY_N8N_DLQ_URL is empty: failed events stay in SQLite")
        return warnings
