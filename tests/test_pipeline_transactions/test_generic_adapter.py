from __future__ import annotations

import csv
import random

from pathlib import Path
from typing import Any

import pytest

from swiss_exp_tracker.pipeline_ingestion.adapters.generic_adapter import to_unified
from swiss_exp_tracker.pipeline_ingestion.data_models.data_sources import get_profile
from swiss_exp_tracker.pipeline_ingestion.data_models.profile_loader import (
    load_profiles,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.source_profile import (
    SourceProfile,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import Currency
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import TransactionType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    UnifiedTransaction,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_01_landing import (
    _read_rows,
)

TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "test_data"
RANDOM_SEED = 42
SAMPLE_SIZE = 100

_SOURCES_WITH_PREAMBLE = {SourceType.UBS_DEBIT, SourceType.UBS_CREDIT}

_SOURCE_SAMPLE_FILES: dict[SourceType, str] = {
    SourceType.ZKB_DEBIT: "zkb_smoke_test.csv",
    SourceType.VISECA: "viseca_smoke_test.csv",
    SourceType.REVOLUT: "revolut_smoke_test.csv",
    SourceType.UBS_DEBIT: "ubs_debit_test.csv",
    SourceType.UBS_CREDIT: "ubs_credit_test.csv",
}


def _make_debit_credit_profile(**overrides: object) -> SourceProfile:
    """Return a minimal ZKB-style debit_credit SourceProfile."""
    base: dict[str, object] = {
        "source": "ZKB_DEBIT",
        "detect_required_columns": ["Debit CHF"],
        "date_columns": ["Date", "Value date"],
        "date_formats": ["%d.%m.%Y", "iso"],
        "reference_columns": ["ZKB reference"],
        "booking_text_columns": ["Booking text"],
        "amount": {
            "mode": "debit_credit",
            "debit_column": "Debit CHF",
            "credit_column": "Credit CHF",
            "fallback_amount_column": "Amount details",
        },
        "currency": {"mode": "column", "column": "Curr", "default": "CHF"},
    }
    base.update(overrides)
    return SourceProfile.model_validate(base)


def _make_signed_profile(
    expense_sign: str = "negative", source: str = "REVOLUT"
) -> SourceProfile:
    """Return a minimal signed-mode SourceProfile."""
    return SourceProfile.model_validate(
        {
            "source": source,
            "detect_required_columns": ["Amount"],
            "date_columns": ["Completed Date", "Started Date"],
            "date_formats": ["iso"],
            "reference_columns": [],
            "booking_text_columns": ["Description"],
            "amount": {
                "mode": "signed",
                "amount_column": "Amount",
                "expense_sign": expense_sign,
            },
            "currency": {"mode": "column", "column": "Currency", "default": "CHF"},
        }
    )


def _make_debit_credit_row(**overrides: object) -> dict[str, object]:
    """Return a valid ZKB-style row dict for debit_credit profile tests."""
    base: dict[str, object] = {
        "Date": "12.01.2025",
        "Booking text": "Test Payment",
        "Curr": "",
        "Amount details": "",
        "ZKB reference": "L12345",
        "Debit CHF": "10.00",
        "Credit CHF": "",
        "Value date": "12.01.2025",
    }
    base.update(overrides)
    return base


def _make_signed_row(**overrides: object) -> dict[str, object]:
    """Return a valid Revolut-style row dict for signed-mode profile tests."""
    base: dict[str, object] = {
        "Type": "Payment",
        "Started Date": "2025-01-16 09:00:00",
        "Completed Date": "2025-01-16 09:01:00",
        "Description": "Test Payment",
        "Amount": "-28.0",
        "Currency": "CHF",
        "State": "COMPLETED",
    }
    base.update(overrides)
    return base


def _read_source_rows(
    source_type: SourceType, sample_file: str
) -> list[dict[str, Any]]:
    """Read rows from sample file; use profile-driven reading for UBS sources with preamble."""
    path = TEST_DATA_DIR / sample_file
    if source_type in _SOURCES_WITH_PREAMBLE:
        profile = get_profile(source_type)
        return _read_rows(path, profile)
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _sample(rows: list[dict[str, Any]], size: int, seed: int) -> list[dict[str, Any]]:
    """Return up to size rows sampled with a fixed seed."""
    if len(rows) <= size:
        return rows
    rng = random.Random(seed)
    return rng.sample(rows, size)


# ---------------------------------------------------------------------------
# debit_credit mode tests
# ---------------------------------------------------------------------------


def test_debit_credit_negative_debit_is_expense() -> None:
    """A negative Debit CHF value → EXPENSE with abs(amount)."""
    profile = _make_debit_credit_profile()
    row = _make_debit_credit_row(**{"Debit CHF": "-45.20"})
    unified = to_unified(row, profile, "test.csv")
    assert unified.transaction_type is TransactionType.EXPENSE
    assert unified.amount == pytest.approx(45.20)
    assert unified.amount >= 0


def test_debit_credit_positive_debit_is_expense() -> None:
    """A positive Debit CHF value → also EXPENSE (sign is ignored, abs taken)."""
    profile = _make_debit_credit_profile()
    row = _make_debit_credit_row(**{"Debit CHF": "45.20"})
    unified = to_unified(row, profile, "test.csv")
    assert unified.transaction_type is TransactionType.EXPENSE
    assert unified.amount == pytest.approx(45.20)


def test_debit_credit_credit_column_is_income() -> None:
    """Credit CHF present with empty Debit → INCOME."""
    profile = _make_debit_credit_profile()
    row = _make_debit_credit_row(**{"Debit CHF": "", "Credit CHF": "3500.00"})
    unified = to_unified(row, profile, "test.csv")
    assert unified.transaction_type is TransactionType.INCOME
    assert unified.amount == pytest.approx(3500.00)


def test_debit_credit_fallback_amount_column_is_expense() -> None:
    """Debit and credit both empty, fallback_amount_column present → EXPENSE."""
    profile = _make_debit_credit_profile()
    row = _make_debit_credit_row(
        **{"Debit CHF": "", "Credit CHF": "", "Amount details": "120.00", "Curr": "EUR"}
    )
    unified = to_unified(row, profile, "test.csv")
    assert unified.transaction_type is TransactionType.EXPENSE
    assert unified.amount == pytest.approx(120.00)


def test_debit_credit_all_empty_produces_zero_expense() -> None:
    """All amount fields empty → zero-amount EXPENSE."""
    profile = _make_debit_credit_profile()
    row = _make_debit_credit_row(
        **{"Debit CHF": "", "Credit CHF": "", "Amount details": ""}
    )
    unified = to_unified(row, profile, "test.csv")
    assert unified.transaction_type is TransactionType.EXPENSE
    assert unified.amount == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# signed mode tests
# ---------------------------------------------------------------------------


def test_signed_expense_sign_negative_negative_amount_is_expense() -> None:
    """expense_sign=negative: negative Amount → EXPENSE with abs(amount)."""
    profile = _make_signed_profile(expense_sign="negative")
    row = _make_signed_row(Amount="-28.0")
    unified = to_unified(row, profile, "test.csv")
    assert unified.transaction_type is TransactionType.EXPENSE
    assert unified.amount == pytest.approx(28.0)
    assert unified.amount >= 0


def test_signed_expense_sign_negative_positive_amount_is_income() -> None:
    """expense_sign=negative: positive Amount → INCOME."""
    profile = _make_signed_profile(expense_sign="negative")
    row = _make_signed_row(Amount="50.0")
    unified = to_unified(row, profile, "test.csv")
    assert unified.transaction_type is TransactionType.INCOME
    assert unified.amount == pytest.approx(50.0)


def test_signed_expense_sign_positive_positive_amount_is_expense() -> None:
    """expense_sign=positive (Viseca-style): positive Amount → EXPENSE."""
    profile = _make_signed_profile(expense_sign="positive", source="VISECA")
    row = _make_signed_row(Amount="55.0")
    unified = to_unified(row, profile, "test.csv")
    assert unified.transaction_type is TransactionType.EXPENSE
    assert unified.amount == pytest.approx(55.0)


def test_signed_expense_sign_positive_negative_amount_is_income() -> None:
    """expense_sign=positive: negative Amount → INCOME."""
    profile = _make_signed_profile(expense_sign="positive", source="VISECA")
    row = _make_signed_row(Amount="-50.0")
    unified = to_unified(row, profile, "test.csv")
    assert unified.transaction_type is TransactionType.INCOME
    assert unified.amount == pytest.approx(50.0)


def test_signed_zero_amount_not_expense_for_negative_expense_sign() -> None:
    """expense_sign=negative: zero Amount is not < 0 → INCOME."""
    profile = _make_signed_profile(expense_sign="negative")
    row = _make_signed_row(Amount="0.0")
    unified = to_unified(row, profile, "test.csv")
    assert unified.transaction_type is TransactionType.INCOME
    assert unified.amount == pytest.approx(0.0)


def test_signed_amount_is_always_non_negative() -> None:
    """to_unified always returns amount >= 0 for signed mode."""
    profile = _make_signed_profile(expense_sign="negative")
    for amount_str in ["-100.0", "100.0", "0.0"]:
        row = _make_signed_row(Amount=amount_str)
        unified = to_unified(row, profile, "test.csv")
        assert unified.amount >= 0


# ---------------------------------------------------------------------------
# date coalescing tests
# ---------------------------------------------------------------------------


def test_date_first_present_column_wins() -> None:
    """First non-empty date column is used; second column is ignored."""
    profile = _make_debit_credit_profile()
    row = _make_debit_credit_row(Date="12.01.2025", **{"Value date": "15.01.2025"})
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert unified.date.day == 12


def test_date_falls_back_to_second_column_when_first_empty() -> None:
    """Empty primary date → falls back to second date column."""
    profile = _make_debit_credit_profile()
    row = _make_debit_credit_row(Date="", **{"Value date": "15.01.2025"})
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert unified.date.day == 15


def test_date_format_strptime_ddmmyyyy() -> None:
    """Date in dd.mm.YYYY format parses correctly."""
    profile = _make_debit_credit_profile()
    row = _make_debit_credit_row(Date="17.05.2026")
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert unified.date.year == 2026
    assert unified.date.month == 5
    assert unified.date.day == 17


def test_date_format_iso_date_string() -> None:
    """Date in ISO date format (YYYY-MM-DD) parses correctly."""
    profile = _make_debit_credit_profile()
    row = _make_debit_credit_row(Date="2026-05-17")
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert unified.date.year == 2026
    assert unified.date.month == 5
    assert unified.date.day == 17


def test_date_format_iso_datetime_string() -> None:
    """Date in ISO datetime format (with space separator) parses correctly."""
    profile = _make_signed_profile()
    row = _make_signed_row(**{"Completed Date": "2023-01-09 13:44:17"})
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert unified.date.year == 2023
    assert unified.date.month == 1


def test_date_missing_in_all_columns_produces_none() -> None:
    """Both date columns empty → unified.date is None."""
    profile = _make_debit_credit_profile()
    row = _make_debit_credit_row(Date="", **{"Value date": ""})
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is None


# ---------------------------------------------------------------------------
# reference fallback tests
# ---------------------------------------------------------------------------


def test_reference_uses_first_non_empty_column() -> None:
    """First non-empty reference column value is used as reference_id."""
    profile = _make_debit_credit_profile()
    row = _make_debit_credit_row(**{"ZKB reference": "L99001"})
    unified = to_unified(row, profile, "test.csv")
    assert unified.reference_id == "L99001"


def test_reference_falls_back_to_noid_when_no_reference_columns() -> None:
    """Profile with empty reference_columns list → reference_id starts with NOID-."""
    profile = _make_signed_profile()  # reference_columns=[]
    row = _make_signed_row()
    unified = to_unified(row, profile, "test.csv")
    assert unified.reference_id.startswith("NOID-")


def test_reference_falls_back_to_noid_when_all_columns_empty() -> None:
    """All reference columns empty → reference_id starts with NOID-."""
    profile = _make_debit_credit_profile()
    row = _make_debit_credit_row(**{"ZKB reference": ""})
    unified = to_unified(row, profile, "test.csv")
    assert unified.reference_id.startswith("NOID-")


# ---------------------------------------------------------------------------
# currency tests
# ---------------------------------------------------------------------------


def test_currency_fixed_mode_returns_default_regardless_of_row() -> None:
    """Fixed currency mode always returns the default currency."""
    profile = _make_debit_credit_profile(currency={"mode": "fixed", "default": "CHF"})
    row = _make_debit_credit_row()
    unified = to_unified(row, profile, "test.csv")
    assert unified.currency is Currency.CHF


def test_currency_column_mode_reads_from_row() -> None:
    """Column currency mode reads the currency from the designated row column."""
    profile = _make_signed_profile()
    row = _make_signed_row(Currency="EUR")
    unified = to_unified(row, profile, "test.csv")
    assert unified.currency is Currency.EUR


def test_currency_column_mode_empty_falls_back_to_default() -> None:
    """Column currency mode with an empty column value falls back to the default CHF."""
    profile = _make_signed_profile()
    row = _make_signed_row(Currency="")
    unified = to_unified(row, profile, "test.csv")
    assert unified.currency is Currency.CHF


def test_currency_column_mode_invalid_value_falls_back_to_default() -> None:
    """Column currency mode with an unrecognised value falls back to the default CHF."""
    profile = _make_signed_profile()
    row = _make_signed_row(Currency="XYZ")
    unified = to_unified(row, profile, "test.csv")
    assert unified.currency is Currency.CHF


# ---------------------------------------------------------------------------
# source_file propagation
# ---------------------------------------------------------------------------


def test_source_file_propagated_to_unified() -> None:
    """source_file argument is stored on the resulting UnifiedTransaction."""
    profile = _make_debit_credit_profile()
    row = _make_debit_credit_row()
    unified = to_unified(row, profile, "my_bank.csv")
    assert unified.source_file == "my_bank.csv"


# ---------------------------------------------------------------------------
# Parametrized invariant test: all 5 sources x sample CSV
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "source_type,sample_file",
    list(_SOURCE_SAMPLE_FILES.items()),
    ids=[st.value for st in _SOURCE_SAMPLE_FILES],
)
def test_to_unified_invariants_for_all_sources(
    source_type: SourceType, sample_file: str
) -> None:
    """Every row in the sample CSV produces a valid UnifiedTransaction for each source."""
    profile = get_profile(source_type)
    rows = _read_source_rows(source_type, sample_file)
    sampled = _sample(rows, SAMPLE_SIZE, RANDOM_SEED)

    assert len(sampled) >= 1, f"No rows found in {sample_file}"

    for row in sampled:
        unified = to_unified(row, profile, sample_file)
        assert isinstance(unified, UnifiedTransaction)
        assert unified.amount >= 0
        assert unified.transaction_type in set(TransactionType)
        assert unified.source_file
        assert unified.source is source_type


@pytest.mark.parametrize(
    "source_type,sample_file",
    list(_SOURCE_SAMPLE_FILES.items()),
    ids=[st.value for st in _SOURCE_SAMPLE_FILES],
)
def test_to_unified_returns_unified_transaction_instance(
    source_type: SourceType, sample_file: str
) -> None:
    """to_unified returns UnifiedTransaction (not None) for the first row of every source."""
    profile = load_profiles()[source_type]
    rows = _read_source_rows(source_type, sample_file)
    assert rows, f"No rows found in {sample_file}"
    unified = to_unified(rows[0], profile, sample_file)
    assert isinstance(unified, UnifiedTransaction)
