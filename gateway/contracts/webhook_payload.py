"""Incoming AmoCRM webhook contract."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class WebhookPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotency_key: str = Field(min_length=1)
    event_type: str = Field(min_length=1)
    occurred_at: datetime
    nonce: str = Field(min_length=1)
    data: dict[str, Any]
