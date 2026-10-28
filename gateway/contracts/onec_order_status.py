"""1C -> amoCRM ingress contract: order/payment status update from 1C."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class OneCOrderStatusSchema(BaseModel):
    """Status notification emitted by 1C for a customer order.

    ``order_id`` is the natural key used to route the update to the matching
    amoCRM lead; ``payment_status`` and ``comment`` are optional because 1C
    may omit them depending on the document state.
    """

    model_config = ConfigDict(extra="forbid")

    order_id: str = Field(min_length=1)
    status: str = Field(min_length=1)
    payment_status: str | None = None
    updated_at: datetime
    comment: str | None = None
