from __future__ import annotations

import uuid

from abc import ABC
from abc import abstractmethod
from typing import TypeVar

from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import Currency
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    RevolutTransaction,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import TransactionType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    UnifiedTransaction,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    VisecaTransaction,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import ZKBTransaction


S = TypeVar("S")


class BaseAdapter[S](ABC):
    source: SourceType

    @abstractmethod
    def to_unified(self, row: S, source_file: str) -> UnifiedTransaction: ...


class ZKBDebitAdapter(BaseAdapter[ZKBTransaction]):
    source = SourceType.ZKB_DEBIT

    def to_unified(self, row: ZKBTransaction, source_file: str) -> UnifiedTransaction:
        if row.AmountDebit is not None:
            amount = abs(row.AmountDebit)
            transaction_type = TransactionType.EXPENSE
            currency = Currency.CHF
        elif row.AmountCredit is not None:
            amount = abs(row.AmountCredit)
            transaction_type = TransactionType.INCOME
            currency = Currency.CHF
        elif row.AmountDetails is not None and row.Curr is not None:
            amount = abs(row.AmountDetails)
            transaction_type = TransactionType.EXPENSE
            currency = row.Curr
        else:
            amount = 0.0
            transaction_type = TransactionType.EXPENSE
            currency = Currency.CHF

        return UnifiedTransaction(
            id=uuid.uuid4(),
            source=self.source,
            reference_id=row.ZKBReference
            or row.ReferenceNumber
            or f"NOID-{uuid.uuid4()}",
            date=row.Date or row.ValueDate,
            amount=amount,
            currency=currency,
            transaction_type=transaction_type,
            booking_text=row.BookingText,
            source_file=source_file,
        )


class VisecaAdapter(BaseAdapter[VisecaTransaction]):
    source = SourceType.VISECA

    def to_unified(
        self, row: VisecaTransaction, source_file: str
    ) -> UnifiedTransaction:
        transaction_type = (
            TransactionType.EXPENSE if row.Amount > 0 else TransactionType.INCOME
        )

        return UnifiedTransaction(
            id=uuid.uuid4(),
            source=self.source,
            reference_id=row.TransactionId or f"NOID-{uuid.uuid4()}",
            date=row.Date,
            amount=abs(row.Amount),
            currency=Currency.CHF,
            transaction_type=transaction_type,
            booking_text=row.MerchantName,
            source_file=source_file,
        )


class RevolutAdapter(BaseAdapter[RevolutTransaction]):
    source = SourceType.REVOLUT

    def to_unified(
        self, row: RevolutTransaction, source_file: str
    ) -> UnifiedTransaction:
        transaction_type = (
            TransactionType.EXPENSE if row.Amount < 0 else TransactionType.INCOME
        )

        return UnifiedTransaction(
            id=uuid.uuid4(),
            source=self.source,
            reference_id=f"NOID-{uuid.uuid4()}",
            date=row.CompletedDate or row.StartedDate,
            amount=abs(row.Amount),
            currency=Currency(row.currency),
            transaction_type=transaction_type,
            booking_text=row.Description,
            source_file=source_file,
        )
