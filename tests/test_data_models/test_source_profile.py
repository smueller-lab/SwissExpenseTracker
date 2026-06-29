from __future__ import annotations

import pytest

from pydantic import ValidationError

from swiss_exp_tracker.pipeline_ingestion.data_models.profile_loader import (
    load_profiles,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.source_profile import AmountSpec
from swiss_exp_tracker.pipeline_ingestion.data_models.source_profile import CurrencySpec
from swiss_exp_tracker.pipeline_ingestion.data_models.source_profile import (
    SourceProfile,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType

_EXPECTED_SOURCES = {
    SourceType.ZKB_DEBIT,
    SourceType.VISECA,
    SourceType.REVOLUT,
    SourceType.UBS_DEBIT,
    SourceType.UBS_CREDIT,
}


def _make_amount_spec_dict(**overrides: object) -> dict[str, object]:
    """Return a valid debit_credit AmountSpec dict; each test only states what it varies."""
    base: dict[str, object] = {
        "mode": "debit_credit",
        "debit_column": "Debit",
        "credit_column": "Credit",
    }
    base.update(overrides)
    return base


def _make_profile_dict(**overrides: object) -> dict[str, object]:
    """Return a valid SourceProfile base dict; each test only states what it varies."""
    base: dict[str, object] = {
        "source": "ZKB_DEBIT",
        "detect_required_columns": ["Debit"],
        "date_columns": ["Date"],
        "date_formats": ["%d.%m.%Y"],
        "booking_text_columns": ["Booking text"],
        "amount": _make_amount_spec_dict(),
        "currency": {"mode": "fixed", "default": "CHF"},
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# load_profiles() tests
# ---------------------------------------------------------------------------


def test_load_profiles_returns_five_profiles() -> None:
    """load_profiles() returns exactly 5 profiles."""
    profiles = load_profiles()
    assert len(profiles) == 5


def test_load_profiles_includes_all_expected_sources() -> None:
    """load_profiles() contains exactly the five expected SourceType keys."""
    profiles = load_profiles()
    assert set(profiles.keys()) == _EXPECTED_SOURCES


def test_load_profiles_each_profile_source_matches_key() -> None:
    """Each profile's .source matches its dict key."""
    profiles = load_profiles()
    for source_type, profile in profiles.items():
        assert profile.source is source_type


def test_load_profiles_returns_source_profile_instances() -> None:
    """Each value returned by load_profiles() is a SourceProfile instance."""
    profiles = load_profiles()
    for profile in profiles.values():
        assert isinstance(profile, SourceProfile)


def test_load_profiles_idempotent() -> None:
    """Calling load_profiles() twice returns the same cached mapping."""
    first = load_profiles()
    second = load_profiles()
    assert first is second


# ---------------------------------------------------------------------------
# AmountSpec model_validator tests
# ---------------------------------------------------------------------------


def test_amount_spec_debit_credit_missing_debit_column_raises() -> None:
    """debit_credit mode without debit_column raises ValidationError."""
    with pytest.raises(ValidationError):
        AmountSpec.model_validate({"mode": "debit_credit", "credit_column": "Credit"})


def test_amount_spec_debit_credit_missing_credit_column_raises() -> None:
    """debit_credit mode without credit_column raises ValidationError."""
    with pytest.raises(ValidationError):
        AmountSpec.model_validate({"mode": "debit_credit", "debit_column": "Debit"})


def test_amount_spec_signed_missing_expense_sign_raises() -> None:
    """signed mode without expense_sign raises ValidationError."""
    with pytest.raises(ValidationError):
        AmountSpec.model_validate({"mode": "signed", "amount_column": "Amount"})


def test_amount_spec_signed_missing_amount_column_raises() -> None:
    """signed mode without amount_column raises ValidationError."""
    with pytest.raises(ValidationError):
        AmountSpec.model_validate({"mode": "signed", "expense_sign": "negative"})


def test_amount_spec_debit_credit_valid_accepts() -> None:
    """debit_credit mode with both columns validates without error."""
    spec = AmountSpec.model_validate(
        {"mode": "debit_credit", "debit_column": "Debit", "credit_column": "Credit"}
    )
    assert spec.mode.value == "debit_credit"


def test_amount_spec_signed_valid_accepts() -> None:
    """signed mode with amount_column and expense_sign validates without error."""
    spec = AmountSpec.model_validate(
        {"mode": "signed", "amount_column": "Amount", "expense_sign": "negative"}
    )
    assert spec.mode.value == "signed"


# ---------------------------------------------------------------------------
# CurrencySpec model_validator tests
# ---------------------------------------------------------------------------


def test_currency_spec_column_mode_without_column_raises() -> None:
    """column currency mode without a column name raises ValidationError."""
    with pytest.raises(ValidationError):
        CurrencySpec.model_validate({"mode": "column"})


def test_currency_spec_column_mode_with_column_accepts() -> None:
    """column currency mode with a column name validates without error."""
    spec = CurrencySpec.model_validate({"mode": "column", "column": "Monnaie"})
    assert spec.column == "Monnaie"


def test_currency_spec_fixed_mode_accepts() -> None:
    """fixed currency mode validates without error."""
    spec = CurrencySpec.model_validate({"mode": "fixed", "default": "CHF"})
    assert spec.default.value == "CHF"


# ---------------------------------------------------------------------------
# SourceProfile model_validate tests
# ---------------------------------------------------------------------------


def test_source_profile_valid_dict_accepts() -> None:
    """A correctly formed profile dict validates without error."""
    profile = SourceProfile.model_validate(_make_profile_dict())
    assert isinstance(profile, SourceProfile)


def test_source_profile_signed_without_expense_sign_raises() -> None:
    """A profile with signed amount mode but no expense_sign raises ValidationError."""
    with pytest.raises(ValidationError):
        SourceProfile.model_validate(
            _make_profile_dict(amount={"mode": "signed", "amount_column": "Amount"})
        )


def test_source_profile_debit_credit_without_debit_column_raises() -> None:
    """A profile with debit_credit mode but no debit_column raises ValidationError."""
    with pytest.raises(ValidationError):
        SourceProfile.model_validate(
            _make_profile_dict(
                amount={"mode": "debit_credit", "credit_column": "Credit"}
            )
        )


def test_source_profile_currency_column_without_column_name_raises() -> None:
    """A profile with column currency mode but no column name raises ValidationError."""
    with pytest.raises(ValidationError):
        SourceProfile.model_validate(_make_profile_dict(currency={"mode": "column"}))


def test_source_profile_with_balance_column_accepts() -> None:
    """A profile that specifies a balance_column validates without error."""
    profile = SourceProfile.model_validate(
        _make_profile_dict(balance_column="Balance CHF")
    )
    assert profile.balance_column == "Balance CHF"


def test_source_profile_with_column_aliases_accepts() -> None:
    """A profile with column_aliases validates and stores them."""
    aliases = {"Débit": "Debit", "Crédit": "Credit"}
    profile = SourceProfile.model_validate(
        _make_profile_dict(column_aliases=aliases, header_signature="Débit")
    )
    assert profile.column_aliases == aliases
    assert profile.header_signature == "Débit"
