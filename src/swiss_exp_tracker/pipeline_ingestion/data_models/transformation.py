from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    RevolutTransaction,
)


class RevolutExchangePending(BaseModel):
    raw_id: int
    transaction: RevolutTransaction


class RevolutExchangeEvent(BaseModel):
    event_date: datetime
    from_currency: str
    to_currency: str
    from_amount: float
    to_amount: float


class RevolutRatePoint(BaseModel):
    event_date: datetime
    rate_to_chf: float


class DebitEBankingContext(BaseModel):
    """Track the current ZKB eBanking parent/detail parsing context."""

    parent_date: datetime | None = None
    parent_reference: str | None = None
    detail_counter: int = 0
