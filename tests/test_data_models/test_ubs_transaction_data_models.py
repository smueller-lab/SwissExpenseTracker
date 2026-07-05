from __future__ import annotations

from pathlib import Path

import pytest

from swiss_exp_tracker.pipeline_ingestion.adapters.coercion import parse_optional_text
from swiss_exp_tracker.pipeline_ingestion.adapters.coercion import parse_swiss_float
from swiss_exp_tracker.pipeline_ingestion.adapters.generic_adapter import to_unified
from swiss_exp_tracker.pipeline_ingestion.data_models.data_sources import get_profile
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


# All values below are synthetic placeholders, not copied from any real export.
def _make_ubs_debit_row(**overrides: object) -> dict[str, object]:
    """Return a valid synthetic UBS debit row keyed by canonical column name."""
    base: dict[str, object] = {
        "Date de transaction": "2024-01-15",
        "Heure de transaction": "",
        "Date de comptabilisation": "2024-01-15",
        "Date de valeur": "2024-01-15",
        "Monnaie": "CHF",
        "Debit": "-10.00",
        "Credit": "",
        "Sous-montant": "",
        "Solde": "1000.00",
        "No de transaction": "TX-001",
        "Description1": "Test Merchant",
        "Description2": "Test Booking",
        "Description3": "Test Merchant",
        "Notes de bas de page": "",
    }
    base.update(overrides)
    return base


def _make_ubs_credit_row(**overrides: object) -> dict[str, object]:
    """Return a valid synthetic UBS credit row keyed by canonical column name."""
    base: dict[str, object] = {
        "Numero de compte": "ACC-1",
        "Numero de carte": "CARD-1",
        "Titulaire de compte/carte": "None",
        "Date d'achat": "15.01.2024",
        "Texte comptable": "Test Merchant",
        "Secteur": "Test Sector",
        "Montant": "5.00",
        "Monnaie originale": "CHF",
        "Cours": "",
        "Monnaie": "CHF",
        "Debit": "5.00",
        "Credit": "",
        "Ecriture": "",
    }
    base.update(overrides)
    return base


def _assert_valid_unified(unified: UnifiedTransaction) -> None:
    """Assert a UnifiedTransaction satisfies the cross-source contract."""
    assert isinstance(unified, UnifiedTransaction)
    assert unified.amount >= 0
    assert unified.transaction_type in set(TransactionType)
    assert unified.source_file


# ---------------------------------------------------------------------------
# parse_swiss_float coercion tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("empty", ["", None])
def test_parse_swiss_float_empty_produces_none(empty: object) -> None:
    """Empty string and None both parse to None."""
    assert parse_swiss_float(empty) is None


def test_parse_swiss_float_comma_decimal_separator() -> None:
    """Swiss-formatted comma decimal separator parsed correctly."""
    assert parse_swiss_float("1,80") == pytest.approx(1.80)


def test_parse_swiss_float_apostrophe_thousands_separator() -> None:
    """Swiss apostrophe thousands separator and comma decimal parse correctly."""
    assert parse_swiss_float("-1'234,50") == pytest.approx(-1234.50)
    assert parse_swiss_float("2'000.00") == pytest.approx(2000.00)


def test_parse_swiss_float_plain_string() -> None:
    """Plain decimal string parses correctly."""
    assert parse_swiss_float("10.50") == pytest.approx(10.50)
    assert parse_swiss_float("-10.50") == pytest.approx(-10.50)


def test_parse_swiss_float_integer_returns_float() -> None:
    """Integer value is returned as float."""
    assert parse_swiss_float(42) == pytest.approx(42.0)
    assert isinstance(parse_swiss_float(42), float)


# ---------------------------------------------------------------------------
# parse_optional_text coercion tests
# ---------------------------------------------------------------------------


def test_parse_optional_text_empty_string_produces_none() -> None:
    """Empty string returns None."""
    assert parse_optional_text("") is None


def test_parse_optional_text_none_produces_none() -> None:
    """None returns None."""
    assert parse_optional_text(None) is None


def test_parse_optional_text_literal_none_string_produces_none() -> None:
    """The literal string 'None' (as UBS exports emit for the account holder) returns None."""
    assert parse_optional_text("None") is None


def test_parse_optional_text_valid_string_returns_stripped() -> None:
    """A non-empty, non-None string is returned stripped."""
    assert parse_optional_text("  Test Merchant  ") == "Test Merchant"


# ---------------------------------------------------------------------------
# UBS debit — generic adapter
# ---------------------------------------------------------------------------


def test_ubs_debit_happy_path_produces_valid_unified() -> None:
    """A valid UBS debit row produces a correct UnifiedTransaction."""
    profile = get_profile(SourceType.UBS_DEBIT)
    row = _make_ubs_debit_row()
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert unified.amount == pytest.approx(10.00)
    assert unified.transaction_type is TransactionType.EXPENSE
    assert unified.currency is Currency.CHF
    assert unified.booking_text == "Test Merchant"
    assert unified.reference_id == "TX-001"


@pytest.mark.parametrize("date_str", ["2024-01-15", "15.01.2024"])
def test_ubs_debit_date_variants(date_str: str) -> None:
    """UBS debit accepts ISO and DD.MM.YYYY transaction dates."""
    profile = get_profile(SourceType.UBS_DEBIT)
    row = _make_ubs_debit_row(**{"Date de transaction": date_str})
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert unified.date.year == 2024


@pytest.mark.parametrize("empty", ["", None])
def test_ubs_debit_nullable_amounts_become_none(empty: object) -> None:
    """Empty Credit / Sous-montant / Solde coerce to None via parse_swiss_float."""
    assert parse_swiss_float(empty) is None


def test_ubs_debit_comma_and_apostrophe_amounts() -> None:
    """Swiss-formatted amounts (comma decimal, apostrophe thousands) parse correctly."""
    assert parse_swiss_float("-1'234,50") == pytest.approx(-1234.50)
    assert parse_swiss_float("2'000.00") == pytest.approx(2000.00)


def test_ubs_debit_comma_amount_reflected_in_unified() -> None:
    """A UBS debit row with comma-decimal Debit produces the correct abs amount."""
    profile = get_profile(SourceType.UBS_DEBIT)
    row = _make_ubs_debit_row(Debit="-1'234,50")
    unified = to_unified(row, profile, "test.csv")
    assert unified.amount == pytest.approx(1234.50)


def test_ubs_debit_debit_row_is_expense() -> None:
    """A Debit value becomes a positive-amount EXPENSE."""
    profile = get_profile(SourceType.UBS_DEBIT)
    row = _make_ubs_debit_row(Debit="-10.00")
    unified = to_unified(row, profile, "test.csv")
    _assert_valid_unified(unified)
    assert unified.amount == pytest.approx(10.00)
    assert unified.transaction_type is TransactionType.EXPENSE
    assert unified.currency is Currency.CHF
    assert unified.booking_text == "Test Merchant"
    assert unified.reference_id == "TX-001"


def test_ubs_debit_credit_row_is_income() -> None:
    """A populated Credit with empty Debit becomes an INCOME."""
    profile = get_profile(SourceType.UBS_DEBIT)
    row = _make_ubs_debit_row(Debit="", Credit="25.00")
    unified = to_unified(row, profile, "test.csv")
    _assert_valid_unified(unified)
    assert unified.amount == pytest.approx(25.00)
    assert unified.transaction_type is TransactionType.INCOME


# ---------------------------------------------------------------------------
# UBS credit — generic adapter
# ---------------------------------------------------------------------------


def test_ubs_credit_happy_path_produces_valid_unified() -> None:
    """A valid UBS credit row produces a correct UnifiedTransaction."""
    profile = get_profile(SourceType.UBS_CREDIT)
    row = _make_ubs_credit_row()
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert unified.amount == pytest.approx(5.00)
    assert unified.transaction_type is TransactionType.EXPENSE
    assert unified.booking_text == "Test Merchant"
    assert unified.currency is Currency.CHF


def test_ubs_credit_none_literal_holder_coerces_to_none() -> None:
    """The literal 'None' emitted by UBS exports for the holder field coerces to None."""
    assert parse_optional_text("None") is None


@pytest.mark.parametrize("date_str", ["15.01.2024", "2024-01-15"])
def test_ubs_credit_date_variants(date_str: str) -> None:
    """UBS credit accepts DD.MM.YYYY and ISO purchase dates."""
    profile = get_profile(SourceType.UBS_CREDIT)
    row = _make_ubs_credit_row(**{"Date d'achat": date_str})
    unified = to_unified(row, profile, "test.csv")
    assert unified.date is not None
    assert unified.date.year == 2024


def test_ubs_credit_debit_row_is_expense() -> None:
    """A positive Debit on the card becomes a positive-amount EXPENSE."""
    profile = get_profile(SourceType.UBS_CREDIT)
    row = _make_ubs_credit_row(Debit="5.00")
    unified = to_unified(row, profile, "test.csv")
    _assert_valid_unified(unified)
    assert unified.amount == pytest.approx(5.00)
    assert unified.transaction_type is TransactionType.EXPENSE
    assert unified.booking_text == "Test Merchant"
    assert unified.reference_id.startswith("NOID-")


def test_ubs_credit_credit_row_is_income() -> None:
    """A populated Credit (refund) with empty Debit becomes an INCOME."""
    profile = get_profile(SourceType.UBS_CREDIT)
    row = _make_ubs_credit_row(Debit="", Credit="50.00")
    unified = to_unified(row, profile, "test.csv")
    _assert_valid_unified(unified)
    assert unified.amount == pytest.approx(50.00)
    assert unified.transaction_type is TransactionType.INCOME


def test_ubs_credit_fallback_to_montant_when_debit_credit_empty() -> None:
    """Empty Debit and Credit with Montant populated → EXPENSE via fallback."""
    profile = get_profile(SourceType.UBS_CREDIT)
    row = _make_ubs_credit_row(Debit="", Credit="", Montant="12.50")
    unified = to_unified(row, profile, "test.csv")
    assert unified.transaction_type is TransactionType.EXPENSE
    assert unified.amount == pytest.approx(12.50)


# ---------------------------------------------------------------------------
# Fixture-file reading — structural only (no transaction values asserted)
# ---------------------------------------------------------------------------


def test_ubs_debit_sample_file_reads_and_converts() -> None:
    """The UBS debit fixture reads without error; every row yields a valid record."""
    profile = get_profile(SourceType.UBS_DEBIT)
    rows = _read_rows(TEST_DATA_DIR / "ubs_debit_test.csv", profile)
    assert len(rows) >= 1
    for row in rows:
        unified = to_unified(row, profile, "ubs_debit_test.csv")
        _assert_valid_unified(unified)
        assert unified.source is SourceType.UBS_DEBIT


def test_ubs_credit_sample_file_reads_and_converts() -> None:
    """The UBS credit fixture reads without error; every row yields a valid record."""
    profile = get_profile(SourceType.UBS_CREDIT)
    rows = _read_rows(TEST_DATA_DIR / "ubs_credit_test.csv", profile)
    assert len(rows) >= 1
    for row in rows:
        unified = to_unified(row, profile, "ubs_credit_test.csv")
        _assert_valid_unified(unified)
        assert unified.source is SourceType.UBS_CREDIT
