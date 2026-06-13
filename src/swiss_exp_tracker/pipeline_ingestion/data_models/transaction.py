from __future__ import annotations

import uuid

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator

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


class ZKBTransaction(BaseModel):
    Date: datetime | None
    BookingText: str = Field(..., alias="Booking text")
    Curr: Currency | None
    AmountDetails: float | None = Field(..., alias="Amount details")
    ZKBReference: str | None = Field(..., alias="ZKB reference")
    ReferenceNumber: str | None = Field(..., alias="Reference number")
    AmountDebit: float | None = Field(..., alias="Debit CHF")
    AmountCredit: float | None = Field(..., alias="Credit CHF")
    ValueDate: datetime | None = Field(..., alias="Value date")
    Balance: float | None = Field(..., alias="Balance CHF")
    PaymentPurpose: str | None = Field(..., alias="Payment purpose")
    Details: str | None = Field(..., alias="Details")

    @field_validator("Date", "ValueDate", mode="before")
    @classmethod
    def _parse_zkb_datetime(cls, value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.strptime(value, "%d.%m.%Y")
            except ValueError:
                return datetime.fromisoformat(value)
        return datetime.fromisoformat(str(value))

    @field_validator(
        "AmountDetails",
        "AmountDebit",
        "AmountCredit",
        "Balance",
        mode="before",
    )
    @classmethod
    def _parse_optional_float(cls, value: Any) -> float | None:
        if value in (None, ""):
            return None
        return float(value)

    @field_validator(
        "Curr",
        "ZKBReference",
        "ReferenceNumber",
        "PaymentPurpose",
        "Details",
        mode="before",
    )
    @classmethod
    def _parse_optional_str(cls, value: Any) -> str | None:
        if value in (None, ""):
            return None
        return str(value)


class VisecaTransaction(BaseModel):
    TransactionId: str
    CardId: str | None
    Date: datetime
    ValutaDate: datetime
    Amount: float = Field(description="Amount in domestic currency (CHF)")
    currency: Currency = Field(
        alias="Currency", description="Currency of the transaction"
    )
    OriginalAmount: float = Field(description="Original amount in transaction currency")
    original_currency: Currency = Field(
        alias="OriginalCurrency", description="Original currency of the transaction"
    )
    MerchantName: str | None
    MerchantPlace: str | None
    MerchantCountry: str | None
    StateType: str
    Details: str
    Type: str
    ExchangeRate: float = Field(..., alias="Exchange Rate")


class RevolutTransaction(BaseModel):
    Type: str
    Product: str
    StartedDate: datetime = Field(..., alias="Started Date")
    CompletedDate: datetime | None = Field(..., alias="Completed Date")
    Description: str
    Amount: float
    currency: str = Field(..., alias="Currency")
    State: str
    Balance: float | None

    @field_validator("CompletedDate", mode="before")
    @classmethod
    def _parse_optional_completed_date(cls, value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        return datetime.fromisoformat(str(value))

    @field_validator("Balance", mode="before")
    @classmethod
    def _parse_optional_balance(cls, value: Any) -> float | None:
        if value in (None, ""):
            return None
        return float(value)
