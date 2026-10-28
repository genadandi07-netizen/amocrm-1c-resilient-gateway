"""Unified error shape used by every module (SPEC.md «Единый формат ошибок»).

Exactly five keys: type, message, severity, location, how_to_fix.
"""

from __future__ import annotations

from typing_extensions import TypedDict

SEVERITY_CRITICAL = "critical"
SEVERITY_WARNING = "warning"
ALLOWED_SEVERITIES = (SEVERITY_CRITICAL, SEVERITY_WARNING)

ERROR_KEYS = ("type", "message", "severity", "location", "how_to_fix")


class ErrorShape(TypedDict, total=True):
    type: str
    message: str
    severity: str
    location: str
    how_to_fix: str


class GatewayError(Exception):
    """Base class for every typed, operator-facing gateway failure.

    Domain exceptions carry the same five-field shape as ingress validation
    errors, so API handlers and the egress worker can log/report them without
    guessing. ``str(exc)`` stays the human-readable message for compatibility
    with the standard library and with existing call sites.
    """

    error_type: str = "dependency"
    location: str = "gateway"
    how_to_fix: str = "Inspect the gateway logs and retry the operation."
    severity: str = SEVERITY_CRITICAL

    def __init__(
        self,
        message: str,
        *,
        location: str | None = None,
        how_to_fix: str | None = None,
        severity: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = str(message)
        self._location = location or self.location
        self._how_to_fix = how_to_fix or self.how_to_fix
        self._severity = severity or self.severity

    def to_error_shape(self) -> ErrorShape:
        return make_error(
            self.error_type,
            self.message,
            self._location,
            self._how_to_fix,
            self._severity,
        )


class ConfigurationError(GatewayError):
    """Invalid or unsafe runtime configuration detected at startup."""

    error_type = "configuration"
    location = "config"
    how_to_fix = "Fix the GATEWAY_* variables (see .env.example) and restart."


class StorageError(GatewayError):
    """SQLite could not be opened, configured, or used after retries."""

    error_type = "storage"
    location = "state_store"
    how_to_fix = "Check the SQLite path/permissions and the WAL filesystem support."


class TransportError(GatewayError):
    """A network/transport failure that is not an HTTP status error."""

    error_type = "dependency"
    location = "egress"
    how_to_fix = "Check connectivity to the dependency, then retry the delivery."


class AuthenticationError(GatewayError):
    """Upstream rejected the gateway credentials (401/403)."""

    error_type = "security"
    location = "egress.auth"
    how_to_fix = "Rotate the access token/credentials and verify the account permissions."
    severity = SEVERITY_WARNING


class RateLimitExceeded(GatewayError):
    """A local or remote rate limit rejected the call (429)."""

    error_type = "security"
    location = "egress.rate_limit"
    how_to_fix = "Lower the outbound rate or wait for the Retry-After window."
    severity = SEVERITY_WARNING


class TokenRotationError(GatewayError):
    """The OAuth token refresher failed or returned an empty token."""

    error_type = "security"
    location = "egress.amocrm.token"
    how_to_fix = "Verify the amoCRM OAuth client credentials and refresh token."
    severity = SEVERITY_WARNING


def make_error(
    type: str,
    message: str,
    location: str,
    how_to_fix: str,
    severity: str = SEVERITY_CRITICAL,
) -> ErrorShape:
    """Build a validated :class:`ErrorShape`.

    Raises ``ValueError`` when an invariant is violated (unknown severity or an
    empty ``how_to_fix``). Missing mandatory fields raise ``TypeError`` from the
    signature itself.
    """
    if severity not in ALLOWED_SEVERITIES:
        raise ValueError(
            f"severity must be one of {ALLOWED_SEVERITIES}, got {severity!r}"
        )
    if not how_to_fix or not str(how_to_fix).strip():
        raise ValueError("how_to_fix is mandatory and must be non-empty")
    if not type or not str(type).strip():
        raise ValueError("type is mandatory and must be non-empty")
    if not location or not str(location).strip():
        raise ValueError("location is mandatory and must be non-empty")
    return ErrorShape(
        type=str(type),
        message=str(message),
        severity=str(severity),
        location=str(location),
        how_to_fix=str(how_to_fix),
    )
