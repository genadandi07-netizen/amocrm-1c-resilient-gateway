"""AmoCRM lead contract."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class AmoLeadCreateSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amo_lead_id: int
    name: str = Field(min_length=1)
    budget: float = Field(ge=0)
    status_id: int
    created_at: datetime
    tags: list[str] = Field(default_factory=list)
