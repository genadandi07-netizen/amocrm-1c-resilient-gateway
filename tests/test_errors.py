from __future__ import annotations

import json

import pytest

from gateway.errors import make_error


def test_make_error_returns_five_fields():
    error = make_error("schema", "bad", "payload.x", "fix it")
    assert set(error.keys()) == {"type", "message", "severity", "location", "how_to_fix"}


def test_error_json_serialization_exact_keys():
    error = make_error("schema", "bad", "payload.x", "fix it")
    assert set(json.loads(json.dumps(error)).keys()) == {
        "type",
        "message",
        "severity",
        "location",
        "how_to_fix",
    }


@pytest.mark.parametrize("severity", ["critical", "warning"])
def test_severity_values_accepted(severity):
    error = make_error("schema", "bad", "payload.x", "fix it", severity=severity)
    assert error["severity"] == severity


def test_invalid_severity_rejected():
    with pytest.raises(ValueError):
        make_error("schema", "bad", "payload.x", "fix it", severity="bogus")


def test_missing_how_to_fix_raises():
    with pytest.raises((TypeError, ValueError)):
        make_error("schema", "bad", "payload.x")
