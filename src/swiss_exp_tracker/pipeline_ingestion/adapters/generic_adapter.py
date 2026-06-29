from __future__ import annotations

import uuid

from datetime import datetime
from typing import Any

from swiss_exp_tracker.pipeline_ingestion.adapters.coercion import parse_optional_text
from swiss_exp_tracker.pipeline_ingestion.adapters.coercion import parse_swiss_float
from swiss_exp_tracker.pipeline_ingestion.data_models.source_profile import AmountMode
from swiss_exp_tracker.pipeline_ingestion.data_models.source_profile import AmountSpec
from swiss_exp_tracker.pipeline_ingestion.data_models.source_profile import CurrencyMode
from swiss_exp_tracker.pipeline_ingestion.data_models.source_profile import CurrencySpec
from swiss_exp_tracker.pipeline_ingestion.data_models.source_profile import ExpenseSign
from swiss_exp_tracker.pipeline_ingestion.data_models.source_profile import (
    SourceProfile,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import Currency
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import TransactionType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    UnifiedTransaction,
)


def _coalesce_date(row: dict[str, Any], profile: SourceProfile) -> datetime | None:
    """Return the first parseable datetime from profile.date_columns in row, or None.

    Tries every format in profile.date_formats for each non-empty column value in order.
    """
    for col in profile.date_columns:
        raw = row.get(col)
        if raw in (None, ""):
            continue
        value = str(raw).strip()
        for fmt in profile.date_formats:
            try:
                if fmt == "iso":
                    return datetime.fromisoformat(value)
                return datetime.strptime(value, fmt)
            except (ValueError, TypeError):
                continue
    return None


def _coalesce_str(row: dict[str, Any], columns: list[str]) -> str | None:
    """Return the first non-empty text value from columns in row, or None."""
    for col in columns:
        text = parse_optional_text(row.get(col))
        if text is not None:
            return text
    return None


def _resolve_amount(
    row: dict[str, Any], spec: AmountSpec
) -> tuple[float, TransactionType]:
    """Return (abs_amount, transaction_type) according to the amount spec mode and row values."""
    if spec.mode == AmountMode.debit_credit:
        debit = parse_swiss_float(
            row.get(spec.debit_column) if spec.debit_column else None
        )
        if debit is not None:
            return abs(debit), TransactionType.EXPENSE

        credit = parse_swiss_float(
            row.get(spec.credit_column) if spec.credit_column else None
        )
        if credit is not None:
            return abs(credit), TransactionType.INCOME

        if spec.fallback_amount_column:
            fallback = parse_swiss_float(row.get(spec.fallback_amount_column))
            if fallback is not None:
                return abs(fallback), TransactionType.EXPENSE

        return 0.0, TransactionType.EXPENSE

    # signed mode
    raw_opt = parse_swiss_float(
        row.get(spec.amount_column) if spec.amount_column else None
    )
    value = raw_opt if raw_opt is not None else 0.0

    if spec.expense_sign == ExpenseSign.positive:
        tx_type = TransactionType.EXPENSE if value > 0 else TransactionType.INCOME
    else:
        tx_type = TransactionType.EXPENSE if value < 0 else TransactionType.INCOME

    return abs(value), tx_type


def _resolve_currency(row: dict[str, Any], spec: CurrencySpec) -> Currency:
    """Return the Currency for this row using the currency spec; falls back to spec.default."""
    if spec.mode == CurrencyMode.fixed:
        return spec.default

    col_val = row.get(spec.column) if spec.column else None
    if col_val not in (None, ""):
        try:
            return Currency(str(col_val).strip())
        except ValueError:
            pass
    return spec.default


def to_unified(
    row: dict[str, Any], profile: SourceProfile, source_file: str
) -> UnifiedTransaction:
    """Convert a raw canonical-keyed row dict and source profile to a UnifiedTransaction."""
    date = _coalesce_date(row, profile)
    ref_raw = _coalesce_str(row, profile.reference_columns)
    reference_id = ref_raw if ref_raw is not None else f"NOID-{uuid.uuid4()}"
    booking_text = _coalesce_str(row, profile.booking_text_columns)
    amount, transaction_type = _resolve_amount(row, profile.amount)
    currency = _resolve_currency(row, profile.currency)

    return UnifiedTransaction(
        id=uuid.uuid4(),
        source=profile.source,
        reference_id=reference_id,
        date=date,
        amount=amount,
        currency=currency,
        transaction_type=transaction_type,
        booking_text=booking_text,
        source_file=source_file,
    )
