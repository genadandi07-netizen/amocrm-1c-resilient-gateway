"""Transport invocation helpers shared by the 1C and amoCRM clients.

The gateway supports two injected transport shapes:

* a callable ``transport(url, method, body, headers=...)`` (production and
  most offline fixtures);
* a legacy callable ``transport(url, method, body)`` without a ``headers``
  keyword.

The decision is made by inspecting the signature **before** the call. Retrying
after an arbitrary ``TypeError`` would be unsafe: a bug inside the transport
would be silently turned into a second outbound HTTP request.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

__all__ = ["transport_accepts_headers"]


def transport_accepts_headers(transport: Callable[..., Any]) -> bool:
    """Return whether ``transport`` supports a ``headers=`` keyword."""
    try:
        signature = inspect.signature(transport)
    except (TypeError, ValueError):  # builtins / C callables
        return True
    if any(
        param.kind is inspect.Parameter.VAR_KEYWORD
        for param in signature.parameters.values()
    ):
        return True
    return "headers" in signature.parameters
