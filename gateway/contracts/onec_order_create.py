"""1C OData order contract (Document_ЗаказПокупателя)."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class OneCOrderItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sku: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    price: float = Field(ge=0)


class OneCOrderCreateSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    number: str = Field(min_length=1)
    date: datetime
    counterparty_ref: str = Field(min_length=1)
    items: list[OneCOrderItem] = Field(min_length=1)
    total: float = Field(ge=0)
