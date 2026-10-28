import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from gateway.config import (
    DEFAULT_CONFIG_DATA,
    DEFAULT_CONFIG_PATH,
    Settings,
)


def _copy_defaults():
    return json.loads(json.dumps(DEFAULT_CONFIG_DATA))


def test_settings_loads_all_four_sections():
    s = Settings.from_file(DEFAULT_CONFIG_PATH)
    assert s.circuit_breaker is not None
    assert s.replay_guard is not None
    assert s.backoff is not None
    assert s.rate_limit is not None


def test_settings_circuit_breaker_params():
    s = Settings.from_file(DEFAULT_CONFIG_PATH)
    assert s.circuit_breaker.failure_threshold > 0
    assert s.circuit_breaker.cooldown_seconds > 0
    assert s.circuit_breaker.half_open_probe_interval > 0


def test_settings_replay_guard_params():
    s = Settings.from_file(DEFAULT_CONFIG_PATH)
    assert s.replay_guard.timestamp_window_seconds > 0
    assert s.replay_guard.nonce_journal_size > 0


def test_settings_backoff_params():
    s = Settings.from_file(DEFAULT_CONFIG_PATH)
    assert s.backoff.base_seconds > 0
    assert s.backoff.multiplier > 1
    assert s.backoff.max_seconds >= s.backoff.base_seconds
    assert 0 <= s.backoff.jitter_ratio <= 1
    assert s.backoff.max_attempts > 0


def test_settings_rate_limit_params():
    s = Settings.from_file(DEFAULT_CONFIG_PATH)
    assert s.rate_limit.algorithm in ("token_bucket", "sliding_window")
    assert s.rate_limit.requests_per_window > 0
    assert s.rate_limit.window_seconds > 0


def test_settings_total_param_count():
    s = Settings.from_file(DEFAULT_CONFIG_PATH)
    total = (
        len(s.circuit_breaker.model_dump())
        + len(s.replay_guard.model_dump())
        + len(s.backoff.model_dump())
        + len(s.rate_limit.model_dump())
    )
    assert total == 13


def test_settings_missing_section_raises(tmp_path):
    data = _copy_defaults()
    del data["backoff"]
    path = tmp_path / "gateway.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValidationError):
        Settings.from_file(str(path))


def test_settings_extra_section_forbidden(tmp_path):
    data = _copy_defaults()
    data["unexpected"] = {"x": 1}
    path = tmp_path / "gateway.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValidationError):
        Settings.from_file(str(path))


def test_settings_from_env():
    env = {
        "GATEWAY_BACKOFF_MAX_ATTEMPTS": "9",
        "GATEWAY_CIRCUIT_BREAKER_FAILURE_THRESHOLD": "2",
        "GATEWAY_RATE_LIMIT_WINDOW_SECONDS": "120",
        "GATEWAY_HMAC_SECRET": "env-secret",
    }
    s = Settings.from_env(env)
    assert s.backoff.max_attempts == 9
    assert s.circuit_breaker.failure_threshold == 2
    assert s.rate_limit.window_seconds == 120
    assert s.hmac_secret == "env-secret"


def test_settings_values_from_config_file():
    raw = json.loads(Path(DEFAULT_CONFIG_PATH).read_text(encoding="utf-8"))
    s = Settings.from_file(DEFAULT_CONFIG_PATH)
    assert s.backoff.max_attempts == raw["backoff"]["max_attempts"]
    assert s.rate_limit.requests_per_window == raw["rate_limit"]["requests_per_window"]


def test_settings_scalars_from_env():
    env = {
        "GATEWAY_DB_PATH": "/tmp/gateway-test.db",
        "GATEWAY_ONEC_BASE_URL": "http://onec:8080/odata",
        "GATEWAY_N8N_DLQ_URL": "http://n8n/webhook/dlq",
        "GATEWAY_HOST": "127.0.0.1",
        "GATEWAY_PORT": "9000",
        "GATEWAY_BUSY_TIMEOUT_MS": "1500",
        "GATEWAY_DB_CONNECT_TIMEOUT_SECONDS": "2.5",
        "GATEWAY_DISPATCH_BATCH_LIMIT": "25",
    }
    s = Settings.from_env(env)
    assert s.db_path == "/tmp/gateway-test.db"
    assert s.one_c_base_url == "http://onec:8080/odata"
    assert s.n8n_dlq_url == "http://n8n/webhook/dlq"
    assert s.host == "127.0.0.1"
    assert s.port == 9000
    assert s.busy_timeout_ms == 1500
    assert s.db_connect_timeout_seconds == 2.5
    assert s.dispatch_batch_limit == 25


def test_settings_scalar_defaults_without_env():
    s = Settings.from_env({})
    assert s.db_path.endswith("gateway.db")
    assert s.busy_timeout_ms == 5000
    assert s.dispatch_batch_limit == 50
