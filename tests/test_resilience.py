"""Tests for the resilience primitives: token bucket pacing, status classes, Retry-After.

Everything here is deterministic and offline: the token bucket is driven by a
virtual clock whose ``sleep_fn`` advances time exactly like a real sleep, so
the refill/deficit arithmetic can be asserted without waiting.
"""

from __future__ import annotations

import email.utils
import threading
import time

import pytest

from gateway.resilience import (
    TokenBucketRateLimiter,
    classify_status,
    parse_retry_after,
    retry_after_floor,
)


class VirtualClock:
    """Deterministic monotonic clock; sleeping advances it like a real sleep."""

    def __init__(self, start: float = 1000.0):
        self.t = start
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.t += seconds

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _limiter(rate_limit_rps=2.0, burst=5, clock=None):
    clock = clock if clock is not None else VirtualClock()
    limiter = TokenBucketRateLimiter(
        rate_limit_rps, burst, clock=clock, sleep_fn=clock.sleep
    )
    return limiter, clock


# --- TokenBucketRateLimiter: validation -------------------------------------

def test_token_bucket_rejects_non_positive_rate():
    with pytest.raises(ValueError):
        TokenBucketRateLimiter(0)
    with pytest.raises(ValueError):
        TokenBucketRateLimiter(-1.0)


def test_token_bucket_rejects_burst_below_one():
    with pytest.raises(ValueError):
        TokenBucketRateLimiter(2.0, 0)
    with pytest.raises(ValueError):
        TokenBucketRateLimiter(2.0, -3)


# --- TokenBucketRateLimiter: refill math ------------------------------------

def test_token_bucket_starts_full():
    limiter, _ = _limiter(rate_limit_rps=2.0, burst=5)
    assert limiter.tokens == 5.0
    assert limiter.rate_limit_rps == 2.0
    assert limiter.burst == 5.0


def test_token_bucket_refills_at_configured_rate():
    limiter, clock = _limiter(rate_limit_rps=2.0, burst=5)
    limiter.acquire(1.0)
    assert limiter.tokens == 4.0
    clock.advance(0.5)  # 0.5 s * 2 rps = 1 token
    limiter.acquire(0.0)  # a zero-cost acquire still refreshes the bucket
    assert limiter.tokens == pytest.approx(5.0)


def test_token_bucket_is_capped_at_burst():
    limiter, clock = _limiter(rate_limit_rps=2.0, burst=5)
    clock.advance(3600.0)  # idle for a very long time
    limiter.acquire(0.0)
    assert limiter.tokens == 5.0


def test_token_bucket_refill_never_goes_negative_after_idle():
    limiter, clock = _limiter(rate_limit_rps=0.5, burst=2)
    clock.advance(100.0)
    limiter.acquire(1.0)
    assert limiter.tokens == pytest.approx(1.0)


# --- TokenBucketRateLimiter: deficit calculations --------------------------

def test_acquire_consumes_one_token_without_sleeping():
    limiter, clock = _limiter(rate_limit_rps=2.0, burst=5)
    waited = limiter.acquire()
    assert waited == 0.0
    assert clock.slept == []
    assert limiter.tokens == 4.0


def test_acquire_sleeps_for_the_exact_deficit():
    # burst=1 -> the second acquire has a deficit of 1 token = 1/2 s at 2 rps.
    limiter, clock = _limiter(rate_limit_rps=2.0, burst=1)
    assert limiter.acquire() == 0.0
    waited = limiter.acquire()
    assert clock.slept == [0.5]
    assert waited == pytest.approx(0.5)
    assert limiter.tokens == pytest.approx(0.0)


def test_acquire_returns_the_total_time_waited():
    limiter, _ = _limiter(rate_limit_rps=1.0, burst=1)
    limiter.acquire()
    waited = limiter.acquire()
    assert waited == pytest.approx(1.0)


def test_acquire_of_multiple_tokens_waits_for_the_whole_deficit():
    limiter, clock = _limiter(rate_limit_rps=2.0, burst=5)
    limiter.acquire(5.0)  # drains the bucket
    waited = limiter.acquire(2.0)  # deficit 2 tokens = 1 s at 2 rps
    assert clock.slept == [1.0]
    assert waited == pytest.approx(1.0)
    assert limiter.tokens == pytest.approx(0.0)


def test_acquire_never_sleeps_when_the_bucket_is_full():
    limiter, clock = _limiter(rate_limit_rps=2.0, burst=5)
    for _ in range(5):
        limiter.acquire()
    assert clock.slept == []
    assert limiter.tokens == 0.0


def test_acquire_waits_only_for_the_missing_tokens():
    limiter, clock = _limiter(rate_limit_rps=4.0, burst=2)
    limiter.acquire(1.0)  # 1 token left
    limiter.acquire(1.0)  # 0 tokens left
    waited = limiter.acquire(1.0)  # deficit 1 token = 0.25 s at 4 rps
    assert clock.slept == [0.25]
    assert waited == pytest.approx(0.25)


# --- TokenBucketRateLimiter: thread safety ----------------------------------

def test_token_bucket_is_thread_safe():
    limiter = TokenBucketRateLimiter(rate_limit_rps=1000.0, burst=5)
    errors: list[Exception] = []

    def worker():
        try:
            for _ in range(4):
                limiter.acquire()
        except Exception as exc:  # pragma: no cover - defensive
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert errors == []
    assert all(not thread.is_alive() for thread in threads)
    assert limiter.tokens >= 0.0


def test_token_bucket_default_clock_and_sleep_are_usable():
    limiter = TokenBucketRateLimiter(rate_limit_rps=1000.0, burst=1)
    started = time.monotonic()
    limiter.acquire()
    limiter.acquire()
    assert time.monotonic() - started < 5.0


# --- classify_status --------------------------------------------------------

@pytest.mark.parametrize("status_code", [200, 201, 204, 299])
def test_classify_status_ok(status_code):
    assert classify_status(status_code) == "ok"


def test_classify_status_retry_for_429():
    assert classify_status(429) == "retry"


@pytest.mark.parametrize("status_code", [401, 403])
def test_classify_status_auth(status_code):
    assert classify_status(status_code) == "auth"


@pytest.mark.parametrize("status_code", [500, 502, 503, 599])
def test_classify_status_fail(status_code):
    assert classify_status(status_code) == "fail"


@pytest.mark.parametrize("status_code", [400, 404, 409, 302, 418])
def test_classify_status_error(status_code):
    assert classify_status(status_code) == "error"


def test_classify_status_covers_every_class():
    assert {
        classify_status(200),
        classify_status(429),
        classify_status(401),
        classify_status(500),
        classify_status(404),
    } == {"ok", "retry", "auth", "fail", "error"}


# --- parse_retry_after ------------------------------------------------------

def test_parse_retry_after_seconds():
    assert parse_retry_after({"Retry-After": "120"}) == 120.0


def test_parse_retry_after_is_case_insensitive():
    assert parse_retry_after({"retry-after": "5"}) == 5.0
    assert parse_retry_after({"RETRY-AFTER": "7"}) == 7.0


def test_parse_retry_after_trims_whitespace():
    assert parse_retry_after({"Retry-After": "  30  "}) == 30.0


def test_parse_retry_after_fractional_seconds():
    assert parse_retry_after({"Retry-After": "2.5"}) == 2.5


def test_parse_retry_after_negative_is_clamped_to_zero():
    assert parse_retry_after({"Retry-After": "-5"}) == 0.0


def test_parse_retry_after_missing_header_is_none():
    assert parse_retry_after({}) is None
    assert parse_retry_after({"Content-Type": "application/json"}) is None


def test_parse_retry_after_empty_header_is_none():
    assert parse_retry_after({"Retry-After": ""}) is None


def test_parse_retry_after_invalid_value_is_none():
    assert parse_retry_after({"Retry-After": "soon"}) is None
    assert parse_retry_after({"Retry-After": "not-a-date"}) is None


def test_parse_retry_after_http_date():
    future = email.utils.formatdate(time.time() + 60, usegmt=True)
    value = parse_retry_after({"Retry-After": future})
    assert value is not None
    assert 55.0 <= value <= 61.0


def test_parse_retry_after_http_date_in_the_past_is_zero():
    past = email.utils.formatdate(time.time() - 120, usegmt=True)
    assert parse_retry_after({"Retry-After": past}) == 0.0


def test_parse_retry_after_accepts_other_headers_untouched():
    headers = {"Content-Type": "application/json", "Retry-After": "9"}
    assert parse_retry_after(headers) == 9.0


# --- retry_after_floor ------------------------------------------------------

def test_retry_after_floor_without_header_keeps_delay():
    assert retry_after_floor(5.0, None) == 5.0


def test_retry_after_floor_raises_the_delay():
    assert retry_after_floor(1.0, 30.0) == 30.0


def test_retry_after_floor_keeps_a_larger_jitter_delay():
    assert retry_after_floor(30.0, 5.0) == 30.0


def test_retry_after_floor_is_a_floor_not_a_ceiling():
    # The header may exceed the local backoff cap: it must win.
    assert retry_after_floor(0.05, 120.0) == 120.0


def test_retry_after_floor_with_zero_delay():
    assert retry_after_floor(0.0, 10.0) == 10.0
    assert retry_after_floor(0.0, None) == 0.0
