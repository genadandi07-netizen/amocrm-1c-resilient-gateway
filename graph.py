"""Pipeline graph for the amocrm-1c-resilient-gateway.

The gateway is a single-agent, acyclic pipeline. This module exposes a
light-weight, dependency-free description of the graph so that orchestrators
and the Documentation Weaver can introspect nodes and edges without importing
the network-facing application.

It is the single source of truth for the topology: four pipeline components
(:mod:`gateway.ingress_guard`, :mod:`gateway.contract_validator`,
:mod:`gateway.state_store`, :mod:`gateway.egress_dispatcher`) plus the n8n
Dead Letter Queue fallback, and the event state machine enforced by
:mod:`gateway.state_store`.
"""

from __future__ import annotations

import json
from typing import TypedDict

__all__ = ["EDGES", "NODES", "STATE_MACHINE", "Edge", "Node", "build_graph"]


class Node(TypedDict):
    """A pipeline component or external sink."""

    id: str
    module: str
    role: str


class Edge(TypedDict):
    """A directed transition between two pipeline nodes."""

    source: str
    target: str
    contract: str


NODES: list[Node] = [
    {
        "id": "ingress_guard",
        "module": "gateway.ingress_guard",
        "role": "Шлюз входа и безопасность",
    },
    {
        "id": "contract_validator",
        "module": "gateway.contract_validator",
        "role": "Валидатор контрактов",
    },
    {
        "id": "state_store",
        "module": "gateway.state_store",
        "role": "Хранилище состояния и идемпотентности",
    },
    {
        "id": "egress_dispatcher",
        "module": "gateway.egress_dispatcher",
        "role": "Диспетчер исходящих вызовов",
    },
    {
        "id": "n8n_dlq",
        "module": "external:n8n",
        "role": "Dead Letter Queue (fallback для FAILED-событий)",
    },
]

EDGES: list[Edge] = [
    {
        "source": "ingress_guard",
        "target": "contract_validator",
        "contract": "WebhookPayload (signed, fresh)",
    },
    {
        "source": "contract_validator",
        "target": "state_store",
        "contract": "validated domain model + idempotency_key",
    },
    {
        "source": "state_store",
        "target": "egress_dispatcher",
        "contract": "VALIDATED event (BEGIN IMMEDIATE)",
    },
    {
        "source": "egress_dispatcher",
        "target": "state_store",
        "contract": "DISPATCHED -> DELIVERED / FAILED",
    },
    {
        "source": "egress_dispatcher",
        "target": "n8n_dlq",
        "contract": "FAILED event (attempts exhausted)",
    },
]

STATE_MACHINE: dict[str, list[str]] = {
    "RECEIVED": ["VALIDATED"],
    "VALIDATED": ["DISPATCHED"],
    "DISPATCHED": ["DELIVERED", "FAILED"],
    "DELIVERED": [],
    "FAILED": ["VALIDATED"],
}


def build_graph() -> dict[str, object]:
    """Return the pipeline graph description."""
    return {
        "entrypoint": "ingress_guard",
        "nodes": NODES,
        "edges": EDGES,
        "state_machine": STATE_MACHINE,
        "acyclic": True,
    }


if __name__ == "__main__":
    print(json.dumps(build_graph(), ensure_ascii=False, indent=2))
