import hashlib
import hmac
from datetime import datetime, timedelta, timezone

from gateway.config import DEFAULT_CONFIG_PATH, RateLimitConfig, ReplayGuardConfig, Settings
from gateway.ingress_guard import (
    NONCE_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    RateLimiter,
    ReplayGuard,
    authenticate_request,
    check_headers,
    verify_hmac_sha256,
)

TEST_SECRET = "test-secret"
BODY = (
    b'{"idempotency_key":"k1","event_type":"order",'
    b'"occurred_at":"2026-10-12T00:00:00+00:00","nonce":"n1","data":{}}'
)


def _sign(body, secret=TEST_SECRET):
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _headers(body, secret=TEST_SECRET, nonce="n-1", timestamp=None,
             signature=None, content_type="application/json"):
    ts = timestamp if timestamp is not None else datetime.now(timezone.utc).isoformat()
    sig = signature if signature is not None else _sign(body, secret)
    return {
        SIGNATURE_HEADER: sig,
        TIMESTAMP_HEADER: ts,
        NONCE_HEADER: nonce,
        "Content-Type": content_type,
    }


def _replay_cfg(size=100, window=300):
    return ReplayGuardConfig(timestamp_window_seconds=window, nonce_journal_size=size)


def _tb_cfg(limit=5, window=60):
    return RateLimitConfig(
        algorithm="token_bucket",
        requests_per_window=limit,
        window_seconds=window,
    )


def _sw_cfg(limit=3, window=60):
    return RateLimitConfig(
        algorithm="sliding_window",
        requests_per_window=limit,
        window_seconds=window,
    )


def _auth_settings(limit=100):
    base = Settings.from_file(DEFAULT_CONFIG_PATH)
    return base.model_copy(
        update={
            "hmac_secret": TEST_SECRET,
            "rate_limit": RateLimitConfig(
                algorithm="token_bucket", requests_per_window=limit, window_seconds=60
            ),
        }
    )


# --- HMAC-SHA256 ---------------------------------------------------------

def test_hmac_valid_signature_returns_true():
    assert verify_hmac_sha256(BODY, _sign(BODY), TEST_SECRET) is True


def test_hmac_tampered_body_returns_false():
    assert verify_hmac_sha256(BODY + b"x", _sign(BODY), TEST_SECRET) is False


def test_hmac_tampered_signature_returns_false():
    sig = _sign(BODY)
    bad = ("0" if sig[0] != "0" else "1") + sig[1:]
    assert verify_hmac_sha256(BODY, bad, TEST_SECRET) is False


def test_hmac_empty_signature_returns_false():
    assert verify_hmac_sha256(BODY, "", TEST_SECRET) is False
    assert verify_hmac_sha256(BODY, None, TEST_SECRET) is False


def test_hmac_accepts_sha256_prefix():
    assert verify_hmac_sha256(BODY, f"sha256={_sign(BODY)}", TEST_SECRET) is True


def test_hmac_uppercase_normalized():
    assert verify_hmac_sha256(BODY, _sign(BODY).upper(), TEST_SECRET) is True


def test_hmac_uses_constant_time_compare(monkeypatch):
    calls = []
    import gateway.ingress_guard as ig

    real = hmac.compare_digest

    def spy(a, b):
        calls.append((a, b))
        return real(a, b)

    monkeypatch.setattr(ig.hmac, "compare_digest", spy)
    assert verify_hmac_sha256(BODY, _sign(BODY), TEST_SECRET) is True
    assert calls


# --- Header guard --------------------------------------------------------

def test_check_headers_valid_returns_none():
    assert check_headers(_headers(BODY)) is None


def test_check_headers_missing_signature():
    headers = _headers(BODY)
    del headers[SIGNATURE_HEADER]
    err = check_headers(headers)
    assert err is not None and err["location"] == "headers"


def test_check_headers_missing_timestamp():
    headers = _headers(BODY)
    del headers[TIMESTAMP_HEADER]
    err = check_headers(headers)
    assert err is not None and err["location"] == "headers"


def test_check_headers_missing_nonce():
    headers = _headers(BODY)
    del headers[NONCE_HEADER]
    err = check_headers(headers)
    assert err is not None and err["location"] == "headers"


def test_check_headers_wrong_content_type():
    err = check_headers(_headers(BODY, content_type="text/plain"))
    assert err is not None and err["location"] == "headers"


def test_check_headers_duplicate_critical_header():
    class MultiDict(dict):
        def getlist(self, key):
            if str(key).lower() == NONCE_HEADER.lower():
                return ["a", "b"]
            value = self.get(key)
            return [] if value is None else [value]

    err = check_headers(MultiDict(_headers(BODY)))
    assert err is not None and err["location"] == "headers"


# --- ReplayGuard ---------------------------------------------------------

def test_replay_first_nonce_ok():
    guard = ReplayGuard(_replay_cfg())
    now = datetime.now(timezone.utc)
    assert guard.check(now, "n1", now) is None


def test_replay_duplicate_nonce_rejected():
    guard = ReplayGuard(_replay_cfg())
    now = datetime.now(timezone.utc)
    assert guard.check(now, "n1", now) is None
    err = guard.check(now, "n1", now)
    assert err is not None and err["location"] == "replay"


def test_replay_expired_timestamp_rejected():
    guard = ReplayGuard(_replay_cfg(window=10))
    now = datetime.now(timezone.utc)
    err = guard.check(now - timedelta(seconds=60), "n1", now)
    assert err is not None and err["location"] == "replay"


def test_replay_future_timestamp_rejected():
    guard = ReplayGuard(_replay_cfg(window=10))
    now = datetime.now(timezone.utc)
    err = guard.check(now + timedelta(seconds=60), "n1", now)
    assert err is not None and err["location"] == "replay"


def test_replay_journal_rotation():
    guard = ReplayGuard(_replay_cfg(size=3))
    now = datetime.now(timezone.utc)
    for i in range(4):
        assert guard.check(now, f"n{i}", now) is None
    assert guard.check(now, "n0", now) is None


def test_replay_error_shape_fields():
    guard = ReplayGuard(_replay_cfg())
    now = datetime.now(timezone.utc)
    guard.check(now, "n1", now)
    err = guard.check(now, "n1", now)
    assert set(err.keys()) == {"type", "message", "severity", "location", "how_to_fix"}


# --- RateLimiter ---------------------------------------------------------

def test_rate_limit_token_bucket_limit():
    rl = RateLimiter(_tb_cfg(5))
    now = datetime(2026, 10, 12, tzinfo=timezone.utc)
    assert all(rl.allow("a", now) for _ in range(5))
    assert rl.allow("a", now) is False


def test_rate_limit_defers_by_ip():
    rl = RateLimiter(_tb_cfg(2))
    now = datetime(2026, 10, 12, tzinfo=timezone.utc)
    assert rl.allow("a", now) is True
    assert rl.allow("a", now) is True
    assert rl.allow("a", now) is False
    assert rl.allow("b", now) is True


def test_rate_limit_refills_after_window():
    rl = RateLimiter(_tb_cfg(5, 60))
    t0 = datetime(2026, 10, 12, tzinfo=timezone.utc)
    for _ in range(5):
        rl.allow("a", t0)
    assert rl.allow("a", t0) is False
    assert rl.allow("a", t0 + timedelta(seconds=60)) is True


def test_rate_limit_state_separate_instances():
    now = datetime(2026, 10, 12, tzinfo=timezone.utc)
    a = RateLimiter(_tb_cfg(1))
    b = RateLimiter(_tb_cfg(1))
    assert a.allow("x", now) is True
    assert b.allow("x", now) is True


def test_rate_limit_sliding_window_allows():
    rl = RateLimiter(_sw_cfg(3))
    now = datetime(2026, 10, 12, tzinfo=timezone.utc)
    assert all(rl.allow("a", now) for _ in range(3))


def test_rate_limit_sliding_window_blocks_and_recovers():
    rl = RateLimiter(_sw_cfg(3, 60))
    t0 = datetime(2026, 10, 12, tzinfo=timezone.utc)
    for _ in range(3):
        rl.allow("a", t0)
    assert rl.allow("a", t0) is False
    assert rl.allow("a", t0 + timedelta(seconds=61)) is True


# --- authenticate_request ------------------------------------------------

def test_authenticate_success():
    s = _auth_settings()
    now = datetime.now(timezone.utc)
    result = authenticate_request(BODY, _headers(BODY, nonce="auth-ok"), "10.0.0.1", now, s)
    assert result.ok is True and result.error is None and result.status_code is None


def test_authenticate_bad_signature_401():
    s = _auth_settings()
    now = datetime.now(timezone.utc)
    headers = _headers(BODY, nonce="auth-sig", signature="deadbeef")
    result = authenticate_request(BODY, headers, "10.0.0.2", now, s)
    assert result.status_code == 401 and result.error is not None


def test_authenticate_tampered_header_403():
    s = _auth_settings()
    now = datetime.now(timezone.utc)
    headers = _headers(BODY, nonce="auth-h")
    del headers[NONCE_HEADER]
    result = authenticate_request(BODY, headers, "10.0.0.3", now, s)
    assert result.status_code == 403


def test_authenticate_replay_403():
    s = _auth_settings()
    now = datetime.now(timezone.utc)
    headers = _headers(BODY, nonce="auth-replay")
    first = authenticate_request(BODY, headers, "10.0.0.4", now, s)
    second = authenticate_request(BODY, headers, "10.0.0.4", now, s)
    assert first.ok is True
    assert second.status_code == 403 and second.error["location"] == "replay"


def test_authenticate_rate_limited_429():
    s = _auth_settings(limit=1)
    now = datetime.now(timezone.utc)
    assert authenticate_request(BODY, _headers(BODY, nonce="rl-1"), "10.0.0.5", now, s).ok is True
    second = authenticate_request(BODY, _headers(BODY, nonce="rl-2"), "10.0.0.5", now, s)
    assert second.status_code == 429
