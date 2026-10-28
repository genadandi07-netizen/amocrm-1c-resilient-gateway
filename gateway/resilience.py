"""Resilience primitives for the egress path: pacing, status classes, Retry-After.

Everything in this module is strictly synchronous and thread-safe:

* :class:`TokenBucketRateLimiter` is a **pacer**: it *sleeps* until the caller
  is allowed to issue the next request (used to smooth outbound traffic to
  1C / amoCRM). It is deliberately different from
  :class:`gateway.ingress_guard.RateLimiter`, which is a **budget**: it
  *rejects* a call instead of waiting.
* :func:`classify_status` maps an HTTP status onto a coarse class used to
  decide whether a failure counts against the circuit breaker.
* :func:`parse_retry_after` / :func:`retry_after_floor` implement the
  RFC 9110 ``Retry-After`` semantics: the header is a **floor**, never a
  ceiling, so the effective delay is ``max(jitter_delay, retry_after)``.

No asyncio, no event loop: the egress execution path stays synchronous and
uses ``threading.Lock`` plus an injectable ``sleep_fn`` so tests can drive
the clock deterministically.
"""

from __future__ import annotations

import email.utils
import logging
import threading
import time
from collections.abc import Callable, Mapping

LOG = logging.getLogger("gateway.resilience")

__all__ = [
    "TokenBucketRateLimiter",
    "classify_status",
    "parse_retry_after",
    "retry_after_floor",
]


class TokenBucketRateLimiter:
    """Blocking token bucket used to pace outbound requests.

    The bucket starts full (``burst`` tokens) and refills at
    ``rate_limit_rps`` tokens per second, capped at ``burst``. :meth:`acquire`
    waits (via the injected ``sleep_fn``) until enough tokens are available
    and returns the total number of seconds it waited.
    """

    def __init__(
        self,
        rate_limit_rps: float = 2.0,
        burst: int = 5,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], None] | None = None,
    ):
        if rate_limit_rps <= 0:
            raise ValueError("rate_limit_rps must be > 0")
        if burst < 1:
            raise ValueError("burst must be >= 1")
        self.rate_limit_rps = float(rate_limit_rps)
        self.burst = float(burst)
        self._tokens = float(burst)
        self._clock = clock
        self._sleep = sleep_fn or time.sleep
        self._updated_at = self._clock()
        self._lock = threading.Lock()

    @property
    def tokens(self) -> float:
        with self._lock:
            return self._tokens

    def acquire(self, tokens: float = 1.0) -> float:
        """Consume ``tokens`` from the bucket, waiting if necessary.

        Returns the total time spent waiting (seconds).
        """
        total_waited = 0.0
        while True:
            sleep_needed = 0.0
            with self._lock:
                now = self._clock()
                elapsed = max(0.0, now - self._updated_at)
                self._tokens = min(self.burst, self._tokens + elapsed * self.rate_limit_rps)
                self._updated_at = now

                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return total_waited

                deficit = tokens - self._tokens
                sleep_needed = deficit / self.rate_limit_rps

            self._sleep(sleep_needed)
            total_waited += sleep_needed


def classify_status(status_code: int) -> str:
    """Classify an HTTP status: ``ok`` / ``retry`` / ``auth`` / ``fail`` / ``error``.

    * ``ok``    - 2xx success.
    * ``retry`` - 429 rate limit (transient, must not trip the breaker).
    * ``auth``  - 401/403 authentication/authorization (must not trip the breaker).
    * ``fail``  - 5xx server error (a genuine dependency failure).
    * ``error`` - anything else (4xx client errors).
    """
    if 200 <= status_code < 300:
        return "ok"
    if status_code == 429:
        return "retry"
    if status_code in (401, 403):
        return "auth"
    if 500 <= status_code < 600:
        return "fail"
    return "error"


def parse_retry_after(headers: Mapping[str, str]) -> float | None:
    """Parse a ``Retry-After`` header into a non-negative delay in seconds.

    Supports both RFC 9110 forms: ``delay-seconds`` and ``HTTP-date``.
    Returns ``None`` when the header is absent or cannot be parsed.
    """
    raw = None
    for k, v in headers.items():
        if k.lower() == "retry-after":
            raw = v.strip()
            break
    if not raw:
        return None

    try:
        val = float(raw)
        return max(0.0, val)
    except ValueError:
        # Not a delay-seconds value; fall through to the HTTP-date form.
        pass

    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt is None:
            return None
        now_epoch = time.time()
        delta = dt.timestamp() - now_epoch
        return max(0.0, delta)
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        LOG.debug("cannot parse Retry-After=%r: %s", raw, exc)
        return None


def retry_after_floor(delay: float, retry_after: float | None) -> float:
    """Apply the RFC 9110 floor: the effective delay is ``max(delay, retry_after)``.

    ``Retry-After`` is never treated as a ceiling - a server asking for a long
    pause must be honoured even when the local jittered backoff is shorter.
    """
    if retry_after is None:
        return delay
    return max(delay, retry_after)
