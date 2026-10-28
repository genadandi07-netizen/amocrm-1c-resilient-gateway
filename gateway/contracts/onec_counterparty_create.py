"""1C OData counterparty contract (Catalog_Контрагенты)."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class OneCCounterpartySchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    inn: str = Field(pattern=r"^([0-9]{10}|[0-9]{12})$")
    name: str = Field(min_length=1)
    legal_name: str | None = None
    phone: str | None = None
    email: str | None = None
    address: str | None = None
