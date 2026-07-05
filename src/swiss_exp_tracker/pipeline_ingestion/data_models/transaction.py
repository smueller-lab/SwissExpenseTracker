from __future__ import annotations

import uuid

from datetime import datetime
from enum import Enum

from pydantic import BaseModel
from pydantic import Field

from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType


class TransactionType(Enum):
    EXPENSE = "EXPENSE"
    INCOME = "INCOME"


class Currency(Enum):
    CHF = "CHF"
    EUR = "EUR"
    USD = "USD"
    AED = "AED"
    GBP = "GBP"
    DKK = "DKK"
    MAD = "MAD"
    TRY = "TRY"
    PLN = "PLN"


class EnrichmentStatus(Enum):
    PENDING = "pending"
    DONE = "done"
    FAILED = "failed"
    SKIPPED = "skipped"


class UnifiedTransaction(BaseModel):
    id: uuid.UUID = Field(description="Unique identifier for the transaction")
    source: SourceType = Field(
        description="Source of the transaction, e.g., 'ZKB_DEBIT', 'VISECA', etc."
    )
    reference_id: str = Field(
        description="Reference number from the original transaction, if available"
    )
    date: datetime | None = Field(description="Date of the transaction")
    amount: float = Field(description="Amount of the transaction in CHF")
    currency: Currency = Field(description="Original currency of the transaction")
    transaction_type: TransactionType = Field(description="Expense or Income")
    booking_text: str | None = Field(description="Booking text of the transaction")
    source_file: str = Field(description="Source file")
