from __future__ import annotations

from enum import Enum

from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator

from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import Currency


class AmountMode(Enum):
    """How the source row encodes amounts: separate debit/credit columns or one signed column."""

    debit_credit = "debit_credit"
    signed = "signed"


class ExpenseSign(Enum):
    """Which arithmetic sign on a signed amount column marks an expense."""

    positive = "positive"
    negative = "negative"


class CurrencyMode(Enum):
    """Whether the currency is a fixed value or read from a row column."""

    fixed = "fixed"
    column = "column"


class AmountSpec(BaseModel):
    """Declarative description of how to extract and classify an amount from a source row."""

    mode: AmountMode
    debit_column: str | None = None
    credit_column: str | None = None
    fallback_amount_column: str | None = None
    amount_column: str | None = None
    expense_sign: ExpenseSign | None = None

    @model_validator(mode="after")
    def _check_mode_consistency(self) -> AmountSpec:
        """Raise ValueError when required fields for the chosen mode are absent."""
        if self.mode == AmountMode.debit_credit and (
            not self.debit_column or not self.credit_column
        ):
            raise ValueError(
                "debit_credit mode requires both debit_column and credit_column"
            )
        if self.mode == AmountMode.signed and (
            not self.amount_column or self.expense_sign is None
        ):
            raise ValueError("signed mode requires both amount_column and expense_sign")
        return self


class CurrencySpec(BaseModel):
    """Declarative description of how to determine the currency of a source row."""

    mode: CurrencyMode
    column: str | None = None
    default: Currency = Currency.CHF

    @model_validator(mode="after")
    def _check_mode_consistency(self) -> CurrencySpec:
        """Raise ValueError when column mode is requested but no column name is given."""
        if self.mode == CurrencyMode.column and not self.column:
            raise ValueError("column currency mode requires a column name")
        return self


class SourceProfile(BaseModel):
    """Declarative mapping for one transaction source — schema only, no parse logic.

    All column references (date_columns, amount.*, currency.column, etc.) use the
    canonical column name (right-hand side of column_aliases). ``"iso"`` in
    date_formats is a sentinel for datetime.fromisoformat; all others are strptime
    format strings.
    """

    source: SourceType
    detect_required_columns: list[str]
    header_signature: str | None = None
    column_aliases: dict[str, str] = Field(default_factory=dict)
    date_columns: list[str]
    date_formats: list[str]
    reference_columns: list[str] = Field(default_factory=list)
    booking_text_columns: list[str]
    amount: AmountSpec
    currency: CurrencySpec
    balance_column: str | None = None
