"""amoCRM egress client: lead/status updates with a client-side rate limiter.

The HTTP transport is injected, so tests run fully offline (``strict_offline``)
against a fake transport, exactly like :class:`gateway.egress_dispatcher.OneCClient`.
A :class:`~gateway.ingress_guard.RateLimiter` caps outbound calls (default
100 rps = token bucket with 100 tokens / 1 s window).

Token ownership (Stage 3)
-------------------------
:class:`AmoClient` is the **single owner** of the dynamic
``Authorization: Bearer <token>`` header: the token is injected per request
from the current in-memory token, never pinned on the transport/client.

When amoCRM answers ``401`` the client rotates the token **single-flight**:
concurrent workers that hit the same 401 share exactly one refresh (the
others reuse the freshly rotated token), and a single delivery request is
retried **at most once** after the rotation - a second 401 is propagated to
the caller instead of looping.

The primary path is synchronous: ``threading.Lock`` provides the single-flight
guarantee for the egress worker. Async callers use
:meth:`AmoClient.async_refresh_token`, which serializes coroutines with an
``asyncio.Lock`` and delegates the critical section to the same thread-safe
implementation via ``asyncio.to_thread`` (both paths can coexist).
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from .config import DEFAULT_AMOCRM_LEADS_PATH, DEFAULT_AMOCRM_REQUESTS_PER_SECOND, RateLimitConfig
from .contracts import OneCOrderStatusSchema
from .errors import GatewayError
from .ingress_guard import RateLimiter
from .resilience import TokenBucketRateLimiter, parse_retry_after
from .transport import transport_accepts_headers

LOG = logging.getLogger("gateway.amocrm")


class AmoError(GatewayError):
    """Raised when the amoCRM transport reports a non-2xx response."""

    error_type = "dependency"
    location = "egress.amocrm"
    how_to_fix = "Check amoCRM API availability, token permissions and request rate."

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        retry_after: float | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        # RFC 9110 ``Retry-After`` hint (seconds) parsed from the response
        # headers; ``None`` when the server did not send one.
        self.retry_after = retry_after


class AmoRateLimitError(AmoError):
    """Raised when the local client-side rate limiter rejects a call."""

    error_type = "security"
    location = "egress.amocrm.rate_limit"
    how_to_fix = (
        "Lower the delivery rate or raise GATEWAY_AMOCRM_REQUESTS_PER_SECOND."
    )
    severity = "warning"


def default_rate_limit_config(
    requests_per_second: int = DEFAULT_AMOCRM_REQUESTS_PER_SECOND,
) -> RateLimitConfig:
    """Token-bucket config bounding amoCRM to ``requests_per_second`` rps."""
    return RateLimitConfig(
        algorithm="token_bucket",
        requests_per_window=max(1, int(requests_per_second)),
        window_seconds=1,
    )


def comment_with_idempotency_key(
    base_comment: str | None, idempotency_key: str
) -> str:
    """Embed the dedup key in the natural comment field (used by the 1C leg).

    Retries must be de-duplicated by 1C itself, so the transport-level
    ``X-Idempotency-Key`` header is mirrored into a human-visible comment token
    ``[idempotency-key:...]`` that 1C can persist alongside the document.
    """
    marker = f"[idempotency-key:{idempotency_key}]"
    if base_comment is None or str(base_comment).strip() == "":
        return marker
    return f"{marker} {base_comment}"


def map_status_to_amo(status: str, payment_status: str | None = None) -> str:
    """Map a 1C order status onto an amoCRM lead status pipeline id.

    Distinct status/payment combinations get distinct pipeline ids so retries
    are visible to operators; unknown values pass through unchanged.
    """
    normalized = str(status).strip().lower()
    mapping = {
        "new": "NEW",
        "created": "NEW",
        "in_progress": "IN_PROGRESS",
        "processing": "IN_PROGRESS",
        "paid": "PAID",
        "ready": "READY",
        "shipped": "SHIPPED",
        "cancelled": "CANCELLED",
        "canceled": "CANCELLED",
        "closed": "CLOSED",
    }
    mapped = mapping.get(normalized, str(status))
    if payment_status is not None and str(payment_status).strip().lower() in (
        "paid",
        "completed",
    ):
        return "PAID"
    return mapped


class AmoClient:
    """Injected-transport amoCRM client with a per-client rate limiter.

    The client owns the bearer token: ``access_token`` seeds it and the
    optional ``token_refresher`` callback provides a fresh one after a 401.
    """

    def __init__(
        self,
        base_url: str,
        transport: Callable[..., dict],
        access_token: str = "",
        headers: dict | None = None,
        rate_limiter: RateLimiter | None = None,
        requests_per_second: int = DEFAULT_AMOCRM_REQUESTS_PER_SECOND,
        clock: Callable[[], datetime] | None = None,
        pacer: TokenBucketRateLimiter | None = None,
        *,
        token_refresher: Callable[[], str | None] | None = None,
        leads_path: str = DEFAULT_AMOCRM_LEADS_PATH,
    ):
        self.base_url = str(base_url).rstrip("/")
        self.transport = transport
        self.leads_path = leads_path.rstrip("/")
        self.requests_per_second = int(requests_per_second)
        # Single-flight state. ``_lock`` serializes the refresh critical
        # section; ``_token_version`` counts the applied rotations so that
        # concurrent workers can tell "somebody already rotated" from
        # "the token is still the one I failed with". ``asyncio.Lock`` is the
        # outer gate for async callers (FastAPI lifespan/async workers); it is
        # created lazily so the client can also be constructed before a loop
        # exists, which the synchronous egress worker relies on.
        self._lock = threading.Lock()
        self._async_lock: asyncio.Lock | None = None
        self._token: str = str(access_token or "")
        self._token_version: int = 0
        self._token_refresher: Callable[[], str | None] = (
            token_refresher if token_refresher is not None else (lambda: self._token)
        )
        self.headers = headers or {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        # Non-blocking budget: it *rejects* a call when the window is full.
        self.rate_limiter = (
            rate_limiter
            if rate_limiter is not None
            else RateLimiter(default_rate_limit_config(self.requests_per_second))
        )
        # Optional blocking pacer: it *sleeps* to smooth the outbound rate.
        # Both coexist: the budget still rejects, the pacer still waits.
        self.pacer = pacer
        self._clock = clock if clock is not None else (lambda: datetime.now(timezone.utc))
        self._transport_accepts_headers = transport_accepts_headers(transport)

    def _get_async_lock(self) -> asyncio.Lock:
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    # -- token state --------------------------------------------------------
    @property
    def token(self) -> str:
        """The bearer token currently used for outbound requests.

        Read without taking ``_lock``: the attribute holds an immutable
        ``str`` snapshot (an atomic read in CPython) and the token refresher
        runs *while* ``_lock`` is held, so locking here would deadlock a
        refresher that reads the client back.
        """
        return self._token

    @property
    def token_version(self) -> int:
        """Number of token rotations applied so far (0 = initial token)."""
        return self._token_version

    @property
    def access_token(self) -> str:
        """Backwards-compatible alias of :attr:`token`."""
        return self._token

    @access_token.setter
    def access_token(self, value: str) -> None:
        with self._lock:
            self._token = str(value or "")
            # A manual rotation is a new epoch: in-flight 401 handlers must
            # not treat it as "already refreshed by somebody else".
            self._token_version += 1

    def refresh_token(self, seen_version: int | None = None) -> str | None:
        """Rotate the bearer token, at most once per rotation epoch.

        ``seen_version`` is the :attr:`token_version` observed *before* the
        failed request was issued. When the current version differs from it,
        another worker already rotated the token: the fresh token is returned
        as-is and the refresher is **not** called again (single flight).

        Without ``seen_version`` the refresher is always invoked and the
        version is always incremented.
        """
        with self._lock:
            if seen_version is not None and self._token_version != seen_version:
                # Somebody else rotated the token: reuse it, do not refresh.
                return self._token
            new_token = self._token_refresher()
            if new_token:
                self._token = new_token
            self._token_version += 1
            return self._token

    async def async_refresh_token(self, seen_version: int | None = None) -> str | None:
        """Async single-flight rotation used by FastAPI/async callers.

        ``asyncio.Lock`` serializes coroutines, while the authoritative
        ``threading.Lock`` inside :meth:`refresh_token` (executed in a worker
        thread so the event loop is never blocked) still protects against a
        concurrent synchronous worker. Both paths compare ``seen_version``, so
        exactly one OAuth refresh happens per rotation epoch regardless of how
        many coroutines observed the same 401.
        """
        async with self._get_async_lock():
            return await asyncio.to_thread(self.refresh_token, seen_version)

    # -- transport plumbing -------------------------------------------------
    def _build_headers(self, extra: dict | None = None) -> dict:
        """Build the outbound headers and inject the *current* bearer token.

        ``Authorization`` is owned by the client: it is always (re)built from
        the live token, so a rotated token is used by the next request and a
        caller can never pin a stale one.
        """
        request_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        request_headers.update(extra or {})
        token = self._token
        if token:
            request_headers["Authorization"] = f"Bearer {token}"
        else:
            request_headers.pop("Authorization", None)
        return request_headers

    @staticmethod
    def _status_of(response: Any) -> int:
        """HTTP status code of a transport response (dict or response object)."""
        if isinstance(response, dict):
            status = response.get("status_code", response.get("status", 200))
        else:
            status = getattr(response, "status_code", 200)
        try:
            return int(status)
        except (TypeError, ValueError):
            return 200

    def _request(self, method: str, path: str, body: dict, headers: dict | None = None):
        """Rate-limited, injected call with single-flight 401 recovery.

        Supports three transport styles so both production (``httpx.Client``-like
        with a ``headers`` kwarg) and offline test fixtures work:
          * real HTTP client object (``request``/``post``/``patch`` methods),
          * ``transport(url, method, body, headers=...)``,
          * ``transport(url, method, body)`` (legacy positional fixture).

        On ``401`` the token is rotated (single flight) and the request is
        replayed **exactly once** with the new token. A second 401 (or any
        other failure) is propagated to the caller - the client never loops.
        """
        if not self.rate_limiter.allow("amocrm", self._clock()):
            raise AmoRateLimitError(
                "Local amoCRM rate limit exceeded "
                f"({self.requests_per_second} rps)"
            )
        # Pace the call (sleep) before it leaves the process.
        if self.pacer is not None:
            self.pacer.acquire()
        url = f"{self.base_url}{path}"
        # Snapshot the token epoch *before* the call: a 401 may only rotate
        # the token if nobody else rotated it in the meantime.
        seen_version = self._token_version
        request_headers = self._build_headers(headers)
        try:
            response = self._invoke_transport(url, method, body, request_headers)
            if self._status_of(response) == 401:
                # Single-flight rotation + exactly one retry with the new token.
                try:
                    self.refresh_token(seen_version)
                except Exception as exc:  # noqa: BLE001 - refresher is user code
                    # The token could not be refreshed: propagate the original
                    # 401 (an auth failure) instead of retrying blindly.
                    LOG.error(
                        "amoCRM token refresh failed; returning the original "
                        "401 to the caller: %s",
                        exc,
                        exc_info=True,
                    )
                    return response
                request_headers = self._build_headers(headers)
                response = self._invoke_transport(url, method, body, request_headers)
            return response
        except AmoError:
            raise
        except Exception as exc:  # noqa: BLE001 - any transport failure is domain
            LOG.warning("amoCRM transport failure: %s", exc, exc_info=True)
            raise AmoError(f"amoCRM transport failure: {exc}") from exc

    def _invoke_transport(self, url, method, body, headers):
        transport = self.transport
        # Style 1: a real HTTP client exposing request()/patch()/post().
        client_request = getattr(transport, "request", None)
        if callable(client_request):
            response = transport.request(method, url, json=body, headers=headers)
            status = getattr(response, "status_code", 200)
            payload = getattr(response, "json", None)
            try:
                parsed = payload() if callable(payload) else {}
            except (TypeError, ValueError) as exc:
                # A malformed/empty JSON body is not an auth failure; keep the
                # status code authoritative and record why the body was lost.
                LOG.warning(
                    "amoCRM response body is not valid JSON (status=%s): %s",
                    status,
                    exc,
                )
                parsed = {}
            if not isinstance(parsed, dict):
                parsed = {"body": parsed}
            return {"status_code": status, "body": parsed}
        # Style 2/3: plain callable transport. The signature was inspected at
        # construction time so an internal TypeError is never mistaken for a
        # "legacy transport without headers" and retried.
        if self._transport_accepts_headers:
            return transport(url, method, body, headers=headers)
        return transport(url, method, body)

    @staticmethod
    def _check(response: Any) -> dict:
        if isinstance(response, dict):
            status = response.get("status_code", response.get("status", 200))
            body = response.get("body", response.get("json", {}))
            raw_headers = response.get("headers", {}) or {}
        else:
            status = getattr(response, "status_code", 200)
            body = getattr(response, "body", {})
            raw_headers = getattr(response, "headers", {}) or {}
        if isinstance(status, int) and status >= 400:
            retry_after = (
                parse_retry_after(raw_headers)
                if hasattr(raw_headers, "items")
                else None
            )
            raise AmoError(
                f"amoCRM returned HTTP {status}",
                status_code=status,
                retry_after=retry_after,
            )
        return body if isinstance(body, dict) else {"body": body}

    # -- operations ---------------------------------------------------------
    def update_lead_status(
        self,
        lead_id: str | int,
        status_id: str,
        *,
        comment: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        """PATCH a lead's status; returns the parsed response body."""
        endpoint = f"{self.leads_path}/{lead_id}"
        body: dict[str, Any] = {"status_id": status_id}
        if comment is not None:
            body["comment"] = comment
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if idempotency_key:
            headers["X-Idempotency-Key"] = str(idempotency_key)
        response = self._request("PATCH", endpoint, body, headers=headers)
        return self._check(response)

    def update_order_status(
        self,
        status: OneCOrderStatusSchema,
        idempotency_key: str | None = None,
    ) -> dict:
        """Apply a validated 1C order-status contract to the matching lead."""
        lead_id: str | int = status.order_id
        comment = status.comment
        if status.payment_status:
            note = f"payment:{status.payment_status}"
            comment = f"{comment} {note}".strip() if comment else note
        return self.update_lead_status(
            lead_id,
            map_status_to_amo(status.status, status.payment_status),
            comment=comment,
            idempotency_key=idempotency_key,
        )


__all__ = [
    "AmoClient",
    "AmoError",
    "AmoRateLimitError",
    "comment_with_idempotency_key",
    "default_rate_limit_config",
    "map_status_to_amo",
]
