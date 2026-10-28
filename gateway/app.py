"""FastAPI application exposing the amoCRM/1C webhooks and GET /health."""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .amocrm_client import AmoClient
from .config import Settings
from .egress_dispatcher import OneCClient
from .errors import GatewayError, make_error
from .observability import (
    REQUEST_ID_HEADER,
    configure_logging,
    request_id_var,
    set_request_id,
)
from .pipeline import process_webhook
from .state_store import init_db

LOG = logging.getLogger("gateway.app")


def _no_transport(*args, **kwargs):
    raise RuntimeError("No egress transport configured (strict_offline by default)")


def _no_dlq(*args, **kwargs):
    raise RuntimeError("No DLQ transport configured (strict_offline by default)")


def create_app(
    settings: Settings | None = None,
    conn: sqlite3.Connection | None = None,
    client: OneCClient | None = None,
    dlq_transport=None,
    sleep_fn=None,
    db_path: str | None = None,
    amocrm_client: AmoClient | None = None,
    amocrm_transport=None,
) -> FastAPI:
    # Environment-driven config: GATEWAY_DB_PATH selects the SQLite file and
    # GATEWAY_<SECTION>_<PARAM> overrides any tunable default.
    settings = settings if settings is not None else Settings.from_env()
    configure_logging(settings.log_level, json_logs=settings.log_json, force=False)
    for warning in settings.production_warnings():
        LOG.warning("configuration: %s", warning)
    resolved_db_path = db_path if db_path is not None else settings.db_path
    if client is None:
        client = OneCClient(
            settings.one_c_base_url,
            _no_transport,
            order_path=settings.onec_order_path,
            counterparty_path=settings.onec_counterparty_path,
        )
    if amocrm_client is None:
        transport = amocrm_transport if amocrm_transport is not None else _no_transport
        amocrm_client = AmoClient(
            settings.amocrm_base_url,
            transport,
            access_token=settings.amocrm_access_token,
            requests_per_second=settings.amocrm_requests_per_second,
            leads_path=settings.amocrm_leads_path,
        )
    if dlq_transport is None:
        dlq_transport = _no_dlq
    if sleep_fn is None:
        sleep_fn = time.sleep

    app = FastAPI(
        title="amocrm-1c-resilient-gateway",
        version="1.0.0",
        description=(
            "Resilient AmoCRM <-> 1C integration gateway "
            "(Ingress -> Validate -> Store -> Egress)."
        ),
    )
    app.state.settings = settings
    app.state.conn = conn
    app.state.db_path = resolved_db_path
    app.state.client = client
    app.state.amocrm_client = amocrm_client
    app.state.dlq_transport = dlq_transport
    app.state.sleep_fn = sleep_fn

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        """Bind a request id and map domain/storage failures to typed errors."""
        token = set_request_id(request.headers.get(REQUEST_ID_HEADER))
        request_id = request_id_var.get()
        started = time.perf_counter()
        response = None
        try:
            response = await call_next(request)
        except GatewayError as exc:
            LOG.error("request failed: %s", exc)
            response = JSONResponse(status_code=503, content=exc.to_error_shape())
        except sqlite3.Error as exc:
            LOG.exception("SQLite failure while handling the request")
            response = JSONResponse(
                status_code=503,
                content=make_error(
                    "storage",
                    f"SQLite failure: {exc}",
                    "state_store",
                    "Check the SQLite database/WAL files and retry.",
                    "critical",
                ),
            )
        except Exception as exc:  # noqa: BLE001 - last-resort HTTP boundary
            LOG.exception("unhandled request failure")
            response = JSONResponse(
                status_code=500,
                content=make_error(
                    "internal",
                    f"{type(exc).__name__}: {exc}",
                    "app",
                    "Inspect the structured logs for the failing request_id.",
                    "critical",
                ),
            )
        finally:
            duration_ms = (time.perf_counter() - started) * 1000.0
            status_code = response.status_code if response is not None else 500
            LOG.info(
                "http_request method=%s path=%s status=%s duration_ms=%.2f",
                request.method,
                request.url.path,
                status_code,
                duration_ms,
                extra={
                    "status_code": status_code,
                    "duration_ms": round(duration_ms, 2),
                },
            )
            if response is not None:
                response.headers[REQUEST_ID_HEADER] = request_id
            request_id_var.reset(token)
        return response

    def _connection() -> sqlite3.Connection:
        """Open the SQLite store lazily so importing the module never touches disk."""
        if app.state.conn is None:
            app.state.conn = init_db(
                app.state.db_path,
                busy_timeout_ms=settings.busy_timeout_ms,
                timeout=settings.db_connect_timeout_seconds,
            )
        return app.state.conn

    @app.get("/health")
    async def health_check() -> dict[str, str]:
        return {"status": "ok", "service": "amocrm-1c-gateway"}

    @app.post(
        "/webhook/amocrm",
        responses={
            200: {"description": "Event accepted (or duplicate)"},
            401: {"description": "Invalid HMAC-SHA256 signature"},
            403: {"description": "Replay detected or header tampering"},
            422: {"description": "Payload validation error"},
            429: {"description": "Rate limit exceeded"},
        },
    )
    async def amocrm_webhook(request: Request) -> JSONResponse:
        raw_body = await request.body()
        headers = request.headers
        source_ip = request.client.host if request.client else "unknown"
        status_code, body = process_webhook(
            raw_body,
            headers,
            source_ip,
            _connection(),
            settings,
            now=datetime.now(timezone.utc),
        )
        return JSONResponse(status_code=status_code, content=body)

    @app.post(
        "/webhook/onec",
        responses={
            200: {"description": "1C status accepted (or duplicate)"},
            401: {"description": "Invalid 1C API token / HMAC signature"},
            403: {"description": "Replay detected or header tampering"},
            422: {"description": "Payload validation error"},
            429: {"description": "Rate limit exceeded"},
        },
    )
    async def onec_webhook(request: Request) -> JSONResponse:
        raw_body = await request.body()
        headers = request.headers
        source_ip = request.client.host if request.client else "unknown"
        # Hybrid auth: an explicit X-Signature implies HMAC, otherwise the 1C
        # API token (X-Api-Token / Bearer) path is used. The header value is
        # only a selector; the signature itself is still verified.
        has_signature = any(k.lower() == "x-signature" for k in headers)
        status_code, body = process_webhook(
            raw_body,
            headers,
            source_ip,
            _connection(),
            settings,
            now=datetime.now(timezone.utc),
            require_signature=has_signature,
        )
        return JSONResponse(status_code=status_code, content=body)

    return app


app = create_app()
