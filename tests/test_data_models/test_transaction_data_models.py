from __future__ import annotations

import csv
import random

from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from swiss_exp_tracker.pipeline_ingestion.adapters.generic_adapter import to_unified
from swiss_exp_tracker.pipeline_ingestion.data_models.profile_loader import (
    load_profiles,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import Currency
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import TransactionType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    UnifiedTransaction,
)

TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "test_data"
UNIFIED_FIELDS = set(UnifiedTransaction.model_fields)
RANDOM_SEED = 42
SAMPLE_SIZE = 100


def _load_csv_rows(file_name: str) -> list[dict[str, str]]:
    """Load all rows from a CSV test data file as a list of dicts."""
    with (TEST_DATA_DIR / file_name).open(encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def _sample_rows(file_name: str, sample_size: int, seed: int) -> list[dict[str, str]]:
    """Return up to sample_size rows from file_name, sampled with a fixed seed."""
    rows = _load_csv_rows(file_name)
    if len(rows) <= sample_size:
        return rows
    rng = random.Random(seed)
    return rng.sample(rows, sample_size)


def _assert_valid_unified_transaction(unified: UnifiedTransaction) -> None:
    """Assert a UnifiedTransaction round-trips through model_validate and has required fields."""
    validated_unified = UnifiedTransaction.model_validate(
        unified.model_dump(mode="python")
    )
    dumped = validated_unified.model_dump(mode="python")

    assert isinstance(validated_unified, UnifiedTransaction)
    assert set(dumped) == UNIFIED_FIELDS
    assert validated_unified.id is not None
    assert validated_unified.source is not None
    assert validated_unified.reference_id is not None
    assert validated_unified.amount is not None
    assert validated_unified.currency is not None
    assert validated_unified.transaction_type in set(TransactionType)
    assert validated_unified.source_file


# ---------------------------------------------------------------------------
# Factory functions
# ---------------------------------------------------------------------------


def _make_zkb_row(**overrides: object) -> dict[str, object]:
    """Return a valid ZKB row base dict; each test only states what it varies."""
    base: dict[str, object] = {
        "Date": "12.01.2025",
        "Booking text": "Debit TWINT: Test",
        "Curr": "",
        "Amount details": "",
        "ZKB reference": "L99999",
        "Reference number": "",
        "Debit CHF": "10.00",
        "Credit CHF": "",
        "Value date": "12.01.2025",
        "Balance CHF": "1000.00",
        "Payment purpose": "",
        "Details": "",
    }
    base.update(overrides)
    return base


def _make_viseca_row(**overrides: object) -> dict[str, object]:
    """Return a valid Viseca row base dict; each test only states what it varies."""
    base: dict[str, object] = {
        "TransactionId": "T999",
        "CardId": "723",
        "Date": "2023-05-23 06:27:05",
        "ValutaDate": "2023-05-24 00:00:00",
        "Amount": "20.0",
        "Currency": "CHF",
        "OriginalAmount": "20.0",
        "OriginalCurrency": "CHF",
        "MerchantName": "Test Merchant",
        "MerchantPlace": "Lausanne",
        "MerchantCountry": "CHE",
        "StateType": "BOOKED",
        "Details": "Test Merchant",
        "Type": "purchase",
        "Exchange Rate": "1",
    }
    base.update(overrides)
    return base


def _make_revolut_row(**overrides: object) -> dict[str, object]:
    """Return a valid Revolut row base dict; each test only states what it varies."""
    base: dict[str, object] = {
        "Type": "Payment",
        "Product": "Current",
        "Started Date": "2022-11-22 20:31:00",
        "Completed Date": "2022-11-22 20:31:00",
        "Description": "Test Payment",
        "Amount": "-10.0",
        "Fee": "0",
        "Currency": "CHF",
        "State": "COMPLETED",
        "Balance": "100",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Parametrized CSV tests — all three sources
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("row", _sample_rows("zkb_test.csv", SAMPLE_SIZE, RANDOM_SEED))
def test_zkb_debit_to_unified_transactions(row: dict[str, str]) -> None:
    """Validate each ZKB CSV row converts to a valid UnifiedTransaction."""
    zkb_profile = load_profiles()[SourceType.ZKB_DEBIT]
    unified = to_unified(row, zkb_profile, "zkb_test.csv")
    assert isinstance(unified, UnifiedTransaction)
    _assert_valid_unified_transaction(unified)


@pytest.mark.parametrize(
    "row", _sample_rows("viseca_test.csv", SAMPLE_SIZE, RANDOM_SEED)
)
def test_viseca_to_unified_transactions(row: dict[str, str]) -> None:
    """Validate each Viseca CSV row converts to a valid UnifiedTransaction."""
    viseca_profile = load_profiles()[SourceType.VISECA]
    unified = to_unified(row, viseca_profile, "viseca_test.csv")
    assert isinstance(unified, UnifiedTransaction)
    _assert_valid_unified_transaction(unified)


@pytest.mark.parametrize(
    "row",
    _sample_rows("account_statement_EUR_test.csv", SAMPLE_SIZE, RANDOM_SEED),
)
def test_revolut_to_unified_transactions(row: dict[str, str]) -> None:
    """Validate each Revolut CSV row converts to a valid UnifiedTransaction."""
    revolut_profile = load_profiles()[SourceType.REVOLUT]
    unified = to_unified(row, revolut_profile, "account_statement_EUR_test.csv")
    assert isinstance(unified, UnifiedTransaction)
    _assert_valid_unified_transaction(unified)


# ---------------------------------------------------------------------------
# ZKB debit branch tests
# ---------------------------------------------------------------------------


def test_zkb_debit_amount_expense() -> None:
    """Debit CHF present → EXPENSE with positive amount and CHF currency."""
    profile = load_profiles()[SourceType.ZKB_DEBIT]
    row = _make_zkb_row(
        **{"Debit CHF": "45.20", "Credit CHF": "", "Amount details": "", "Curr": ""}
    )
    unified = to_unified(row, profile, "test.csv")
    assert unified.amount >= 0
    assert unified.source == SourceType.ZKB_DEBIT
    assert unified.transaction_type == TransactionType.EXPENSE
    assert unified.amount == pytest.approx(45.20)
    assert unified.currency == Currency.CHF


def test_zkb_credit_amount_income() -> None:
    """Credit CHF present → INCOME with positive amount and CHF currency."""
    profile = load_profiles()[SourceType.ZKB_DEBIT]
    row = _make_zkb_row(**{"Credit CHF": "3500.00", "Debit CHF": ""})
    unified = to_unified(row, profile, "test.csv")
    assert unified.amount >= 0
    assert unified.source == SourceType.ZKB_DEBIT
    assert unified.transaction_type == TransactionType.INCOME
    assert unified.amount == pytest.approx(3500.00)
    assert unified.currency == Currency.CHF


def test_zkb_amount_details_with_foreign_currency() -> None:
    """Amount details + Curr → EXPENSE with foreign currency."""
    profile = load_profiles()[SourceType.ZKB_DEBIT]
    row = _make_zkb_row(
        **{
            "Debit CHF": "",
            "Credit CHF": "",
            "Amount details": "120.00",
            "Curr": "EUR",
        }
    )
    unified = to_unified(row, profile, "test.csv")
    assert unified.amount >= 0
    assert unified.source == SourceType.ZKB_DEBIT
    assert unified.transaction_type == TransactionType.EXPENSE
    assert unified.amount == pytest.approx(120.00)
    assert unified.currency == Currency.EUR


def test_zkb_all_amounts_none_produces_zero() -> None:
    """All amount fields empty → zero EXPENSE in CHF."""
    profile = load_profiles()[SourceType.ZKB_DEBIT]
    row = _make_zkb_row(
        **{"Debit CHF": "", "Credit CHF": "", "Amount details": "", "Curr": ""}
    )
    unified = to_unified(row, profile, "test.csv")
    assert unified.amount >= 0
    assert unified.source == SourceType.ZKB_DEBIT
    assert unified.amount == pytest.approx(0.0)
    assert unified.transaction_type == TransactionType.EXPENSE
    assert unified.currency == Currency.CHF


def test_zkb_negative_debit_amount_is_abs() -> None:
    """Negative Debit CHF value → abs(amount) in unified output."""
    profile = load_profiles()[SourceType.ZKB_DEBIT]
    row = _make_zkb_row(**{"Debit CHF": "-10.00"})
    unified = to_unified(row, profile, "test.csv")
    assert unified.amount == pytest.approx(10.0)


def test_zkb_reference_uses_zkb_reference() -> None:
    """ZKB reference present → reference_id uses ZKB reference."""
    profile = load_profiles()[SourceType.ZKB_DEBIT]
    row = _make_zkb_row(**{"ZKB reference": "L99001", "Reference number": ""})
    unified = to_unified(row, profile, "test.csv")
    assert unified.reference_id == "L99001"


def test_zkb_reference_falls_back_to_reference_number() -> None:
    """ZKB reference empty but Reference number present → uses Reference number."""
    profile = load_profiles()[SourceType.ZKB_DEBIT]
    row = _make_zkb_row(**{"ZKB reference": "", "Reference number": "REF-ABC"})
    unified = to_unified(row, profile, "test.csv")
    assert unified.reference_id == "REF-ABC"


def test_zkb_reference_generates_noid_when_both_missing() -> None:
    """Both reference fields empty → NOID- prefixed UUID."""
    profile = load_profiles()[SourceType.ZKB_DEBIT]
    row = _make_zkb_row(**{"ZKB reference": "", "Reference number": ""})
    unified = to_unified(row, profile, "test.csv")
    assert unified.reference_id.startswith("NOID-")


def test_zkb_date_falls_back_to_value_date() -> None:
    """Date empty → falls back to Value date."""
    profile = load_profiles()[SourceType.ZKB_DEBIT]
    row = _make_zkb_row(**{"Date": "", "Value date": "15.01.2025"})
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert unified.date.day == 15


def test_zkb_date_is_none_when_both_missing() -> None:
    """Date and Value date both empty → unified.date is None."""
    profile = load_profiles()[SourceType.ZKB_DEBIT]
    row = _make_zkb_row(**{"Date": "", "Value date": ""})
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is None


def test_zkb_booking_text_preserved() -> None:
    """Booking text is passed through unchanged."""
    profile = load_profiles()[SourceType.ZKB_DEBIT]
    row = _make_zkb_row(**{"Booking text": "Debit TWINT: Migros"})
    unified = to_unified(row, profile, "test.csv")
    assert unified.booking_text == "Debit TWINT: Migros"


# ---------------------------------------------------------------------------
# ZKB date format tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "date_str",
    [
        "12.01.2025",
        "2025-01-12",
        "2025-01-12T14:30:00",
        "2025-01-12 14:30:00",
        "2025-01-12T14:30:00.123456",
        "2025-01-12T14:30:00+01:00",
    ],
)
def test_zkb_date_formats_accepted(date_str: str) -> None:
    """ZKB Date field accepts these datetime string formats; unified.date is non-None."""
    profile = load_profiles()[SourceType.ZKB_DEBIT]
    row = _make_zkb_row(Date=date_str, **{"Value date": ""})
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert isinstance(unified.date, datetime)


@pytest.mark.parametrize(
    "date_str",
    [
        "12.01.2025 14:30",
        "2025/01/12",
    ],
)
def test_zkb_unrecognized_date_format_produces_none(date_str: str) -> None:
    """ZKB Date field with an unrecognised format → unified.date is None."""
    profile = load_profiles()[SourceType.ZKB_DEBIT]
    row = _make_zkb_row(Date=date_str, **{"Value date": ""})
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is None


# ---------------------------------------------------------------------------
# Viseca branch tests
# ---------------------------------------------------------------------------


def test_viseca_positive_amount_is_expense() -> None:
    """Positive Amount → EXPENSE."""
    profile = load_profiles()[SourceType.VISECA]
    row = _make_viseca_row(Amount="55.0")
    unified = to_unified(row, profile, "test.csv")
    assert unified.amount >= 0
    assert unified.source == SourceType.VISECA
    assert unified.transaction_type == TransactionType.EXPENSE
    assert unified.amount == pytest.approx(55.0)


def test_viseca_negative_amount_is_income() -> None:
    """Negative Amount → INCOME with abs value."""
    profile = load_profiles()[SourceType.VISECA]
    row = _make_viseca_row(Amount="-50.0")
    unified = to_unified(row, profile, "test.csv")
    assert unified.amount >= 0
    assert unified.source == SourceType.VISECA
    assert unified.transaction_type == TransactionType.INCOME
    assert unified.amount == pytest.approx(50.0)


def test_viseca_zero_amount_is_income() -> None:
    """Zero Amount → INCOME (0 is not > 0)."""
    profile = load_profiles()[SourceType.VISECA]
    row = _make_viseca_row(Amount="0.0")
    unified = to_unified(row, profile, "test.csv")
    assert unified.amount >= 0
    assert unified.source == SourceType.VISECA
    assert unified.transaction_type == TransactionType.INCOME
    assert unified.amount == pytest.approx(0.0)


def test_viseca_currency_is_always_chf() -> None:
    """Viseca uses fixed currency mode — CHF regardless of the row Currency field."""
    profile = load_profiles()[SourceType.VISECA]
    row = _make_viseca_row(Currency="EUR")
    unified = to_unified(row, profile, "test.csv")
    assert unified.currency == Currency.CHF


def test_viseca_empty_merchant_falls_back_to_details() -> None:
    """Empty MerchantName → booking_text falls back to Details (e.g. card payments)."""
    profile = load_profiles()[SourceType.VISECA]
    row = _make_viseca_row(MerchantName="", Details="Ihre Zahlung - Danke")
    unified = to_unified(row, profile, "test.csv")
    assert unified.booking_text == "Ihre Zahlung - Danke"


def test_viseca_merchant_name_wins_over_details() -> None:
    """Present MerchantName takes precedence over Details for booking_text."""
    profile = load_profiles()[SourceType.VISECA]
    row = _make_viseca_row(MerchantName="COOP-2440 ZUERICH", Details="ignored")
    unified = to_unified(row, profile, "test.csv")
    assert unified.booking_text == "COOP-2440 ZUERICH"


def test_viseca_null_merchant_and_details_does_not_raise() -> None:
    """Empty MerchantName and Details → adapter does not raise; booking_text is falsy."""
    profile = load_profiles()[SourceType.VISECA]
    row = _make_viseca_row(MerchantName="", Details="")
    unified = to_unified(row, profile, "test.csv")
    assert not unified.booking_text


def test_viseca_empty_transaction_id_generates_noid() -> None:
    """Empty TransactionId → NOID- prefixed reference_id."""
    profile = load_profiles()[SourceType.VISECA]
    row = _make_viseca_row(TransactionId="")
    unified = to_unified(row, profile, "test.csv")
    assert unified.reference_id.startswith("NOID-")


def test_viseca_amount_is_abs() -> None:
    """Negative Amount → abs(amount) in unified output."""
    profile = load_profiles()[SourceType.VISECA]
    row = _make_viseca_row(Amount="-20.0")
    unified = to_unified(row, profile, "test.csv")
    assert unified.amount == pytest.approx(20.0)


# ---------------------------------------------------------------------------
# Viseca date format tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "date_str",
    [
        "2023-05-23 06:27:05",
        "2023-05-23T06:27:05",
        "2023-05-23",
        "2023-05-23T06:27:05+02:00",
    ],
)
def test_viseca_date_formats_accepted(date_str: str) -> None:
    """Viseca Date field accepts ISO datetime string formats; unified.date is non-None."""
    profile = load_profiles()[SourceType.VISECA]
    row = _make_viseca_row(Date=date_str, ValutaDate=date_str)
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert isinstance(unified.date, datetime)


@pytest.mark.parametrize(
    "date_str",
    [
        "23.05.2023",
        "23.05.2023 06:27:05",
        "05/23/2023",
    ],
)
def test_viseca_unrecognized_date_format_produces_none(date_str: str) -> None:
    """Viseca Date and ValutaDate both unrecognised → unified.date is None."""
    profile = load_profiles()[SourceType.VISECA]
    # Empty ValutaDate so there is no iso fallback to succeed.
    row = _make_viseca_row(Date=date_str, ValutaDate="")
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is None


# ---------------------------------------------------------------------------
# Revolut branch tests
# ---------------------------------------------------------------------------


def test_revolut_negative_amount_is_expense() -> None:
    """Negative Amount → EXPENSE with abs value."""
    profile = load_profiles()[SourceType.REVOLUT]
    row = _make_revolut_row(Amount="-28.0")
    unified = to_unified(row, profile, "test.csv")
    assert unified.amount >= 0
    assert unified.source == SourceType.REVOLUT
    assert unified.transaction_type == TransactionType.EXPENSE
    assert unified.amount == pytest.approx(28.0)


def test_revolut_positive_amount_is_income() -> None:
    """Positive Amount → INCOME."""
    profile = load_profiles()[SourceType.REVOLUT]
    row = _make_revolut_row(Amount="50.0")
    unified = to_unified(row, profile, "test.csv")
    assert unified.amount >= 0
    assert unified.source == SourceType.REVOLUT
    assert unified.transaction_type == TransactionType.INCOME
    assert unified.amount == pytest.approx(50.0)


def test_revolut_zero_amount_is_income() -> None:
    """Zero Amount → INCOME (0 is not < 0)."""
    profile = load_profiles()[SourceType.REVOLUT]
    row = _make_revolut_row(Amount="0.0")
    unified = to_unified(row, profile, "test.csv")
    assert unified.amount >= 0
    assert unified.source == SourceType.REVOLUT
    assert unified.transaction_type == TransactionType.INCOME
    assert unified.amount == pytest.approx(0.0)


def test_revolut_completed_date_used_when_set() -> None:
    """Completed Date present → unified.date uses it."""
    profile = load_profiles()[SourceType.REVOLUT]
    row = _make_revolut_row(**{"Completed Date": "2022-11-22 20:31:00"})
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert unified.date.year == 2022


def test_revolut_falls_back_to_started_date_when_completed_is_empty() -> None:
    """Empty Completed Date → falls back to Started Date."""
    profile = load_profiles()[SourceType.REVOLUT]
    row = _make_revolut_row(
        **{"Completed Date": "", "Started Date": "2023-01-09 13:44:17"}
    )
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert unified.date.year == 2023
    assert unified.date.month == 1


def test_revolut_chf_currency_preserved() -> None:
    """CHF Currency field → CHF in unified output."""
    profile = load_profiles()[SourceType.REVOLUT]
    row = _make_revolut_row(Currency="CHF")
    unified = to_unified(row, profile, "test.csv")
    assert unified.currency == Currency.CHF


def test_revolut_eur_currency_preserved() -> None:
    """EUR Currency field → EUR in unified output."""
    profile = load_profiles()[SourceType.REVOLUT]
    row = _make_revolut_row(Currency="EUR")
    unified = to_unified(row, profile, "test.csv")
    assert unified.currency == Currency.EUR


def test_revolut_reference_is_always_noid() -> None:
    """Revolut profile has no reference_columns → NOID- prefixed reference_id."""
    profile = load_profiles()[SourceType.REVOLUT]
    row = _make_revolut_row()
    unified = to_unified(row, profile, "test.csv")
    assert unified.reference_id.startswith("NOID-")


def test_revolut_unknown_currency_falls_back_to_chf() -> None:
    """Unknown Currency value in column mode falls back to the default CHF."""
    profile = load_profiles()[SourceType.REVOLUT]
    row = _make_revolut_row(Currency="XYZ")
    unified = to_unified(row, profile, "test.csv")
    assert unified.currency == Currency.CHF


# ---------------------------------------------------------------------------
# Revolut date format tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "date_str",
    [
        "2022-11-22 20:31:00",
        "2022-11-22T20:31:00",
        "2022-11-22",
    ],
)
def test_revolut_started_date_formats_accepted(date_str: str) -> None:
    """Revolut Started Date field accepts these ISO datetime string formats."""
    profile = load_profiles()[SourceType.REVOLUT]
    row = _make_revolut_row(**{"Started Date": date_str, "Completed Date": ""})
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert isinstance(unified.date, datetime)


@pytest.mark.parametrize(
    "completed_str,expect_date_none",
    [
        ("", False),  # empty Completed Date → falls back to Started Date
        ("2022-11-22 20:31:00", False),
        ("2022-11-22T20:31:00", False),
        ("2022-11-22", False),
    ],
)
def test_revolut_completed_date_formats(
    completed_str: str, expect_date_none: bool
) -> None:
    """Revolut Completed Date field handles empty strings and various ISO formats."""
    profile = load_profiles()[SourceType.REVOLUT]
    row = _make_revolut_row(
        **{"Completed Date": completed_str, "Started Date": "2022-11-22 20:31:00"}
    )
    unified = to_unified(row, profile, "test.csv")
    if expect_date_none:
        assert unified.date is None
    else:
        assert unified.date is not None


# ---------------------------------------------------------------------------
# Cross-source invariant tests (parametrized)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "source_type,row_factory",
    [
        (SourceType.ZKB_DEBIT, _make_zkb_row),
        (SourceType.VISECA, _make_viseca_row),
        (SourceType.REVOLUT, _make_revolut_row),
    ],
)
def test_to_unified_amount_is_always_non_negative(
    source_type: SourceType, row_factory: Any
) -> None:
    """to_unified() always returns amount >= 0 for all sources."""
    profile = load_profiles()[source_type]
    row = row_factory()
    unified = to_unified(row, profile, "test.csv")
    assert unified.amount >= 0


@pytest.mark.parametrize(
    "source_type,row_factory,expected_source",
    [
        (SourceType.ZKB_DEBIT, _make_zkb_row, SourceType.ZKB_DEBIT),
        (SourceType.VISECA, _make_viseca_row, SourceType.VISECA),
        (SourceType.REVOLUT, _make_revolut_row, SourceType.REVOLUT),
    ],
)
def test_to_unified_source_matches_profile(
    source_type: SourceType, row_factory: Any, expected_source: SourceType
) -> None:
    """to_unified() source field matches the profile's declared source for all sources."""
    profile = load_profiles()[source_type]
    row = row_factory()
    unified = to_unified(row, profile, "test.csv")
    assert unified.source == expected_source
