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
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    SwissquoteTransaction,
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
        elif row.AmountCredit is not None:
            amount = abs(row.AmountCredit)
            transaction_type = TransactionType.INCOME
        else:
            amount = 0.0
            transaction_type = TransactionType.EXPENSE

        return UnifiedTransaction(
            id=uuid.uuid4(),
            source=self.source,
            zkb_reference=row.ZKBReference or row.ReferenceNumber or "",
            date=row.Date,
            amount=amount,
            currency=Currency.CHF,
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
            zkb_reference=row.TransactionId,
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
            zkb_reference="",
            date=row.CompletedDate or row.StartedDate,
            amount=abs(row.Amount),
            currency=Currency(row.currency),
            transaction_type=transaction_type,
            booking_text=row.Description,
            source_file=source_file,
        )


class SwissquoteAdapter(BaseAdapter[SwissquoteTransaction]):
    source = SourceType.SWISSQUOTE

    def to_unified(
        self, row: SwissquoteTransaction, source_file: str
    ) -> UnifiedTransaction:
        raise NotImplementedError("SwissquoteAdapter.to_unified is not yet implemented")
