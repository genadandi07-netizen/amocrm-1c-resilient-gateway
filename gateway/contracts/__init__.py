"""Pydantic v2 contracts (extra=forbid) for the gateway."""

from .amo_lead_create import AmoLeadCreateSchema
from .onec_counterparty_create import OneCCounterpartySchema
from .onec_order_create import OneCOrderCreateSchema, OneCOrderItem
from .onec_order_status import OneCOrderStatusSchema
from .webhook_payload import WebhookPayload

__all__ = [
    "AmoLeadCreateSchema",
    "OneCCounterpartySchema",
    "OneCOrderCreateSchema",
    "OneCOrderItem",
    "OneCOrderStatusSchema",
    "WebhookPayload",
]
