"""Shared fixtures for the gateway test suite.

``tests`` is a regular package (see ``tests/__init__.py``); the fixtures defined
here are discovered automatically by pytest.
"""

from __future__ import annotations

import socket

import pytest

from gateway.config import DEFAULT_CONFIG_PATH, Settings
from gateway.state_store import init_db

TEST_SECRET = "test-secret"


@pytest.fixture(autouse=True)
def strict_offline(monkeypatch):
    """network_mode=strict_offline: any real socket connect is blocked."""

    def _blocked(*args, **kwargs):
        raise RuntimeError("network access blocked (strict_offline)")

    monkeypatch.setattr(socket.socket, "connect", _blocked, raising=True)
    yield


@pytest.fixture(autouse=True)
def _reset_guard_caches():
    from gateway import ingress_guard

    ingress_guard.reset_caches()
    yield
    ingress_guard.reset_caches()


@pytest.fixture
def settings() -> Settings:
    base = Settings.from_file(DEFAULT_CONFIG_PATH)
    return base.model_copy(update={"hmac_secret": TEST_SECRET})


@pytest.fixture
def db_path(tmp_path) -> str:
    return str(tmp_path / "gateway_test.db")


@pytest.fixture
def conn(db_path):
    connection = init_db(db_path)
    yield connection
    connection.close()
