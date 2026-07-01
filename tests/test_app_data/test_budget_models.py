"""Tests for swiss_exp_tracker.app.data.budget_models.CategoryBudget."""

from __future__ import annotations

import pytest

from swiss_exp_tracker.app.data.budget_models import CategoryBudget


def _make_budget_payload(**overrides: object) -> dict[str, object]:
    """Return a valid CategoryBudget payload; each test states only what it varies."""
    base: dict[str, object] = {
        "category": "Groceries",
        "budget_chf": 500.0,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_budget_model_happy_path_fields_populated() -> None:
    """model_validate with valid inputs populates both fields correctly."""
    m = CategoryBudget.model_validate(_make_budget_payload())
    assert m.category == "Groceries"
    assert m.budget_chf == pytest.approx(500.0)


def test_budget_model_zero_budget_is_valid() -> None:
    """budget_chf=0.0 is accepted and stays 0.0."""
    m = CategoryBudget.model_validate(_make_budget_payload(budget_chf=0.0))
    assert m.budget_chf == pytest.approx(0.0)


def test_budget_model_large_positive_budget() -> None:
    """Large positive budget_chf is stored without modification."""
    m = CategoryBudget.model_validate(_make_budget_payload(budget_chf=99999.99))
    assert m.budget_chf == pytest.approx(99999.99)


# ---------------------------------------------------------------------------
# Coercion: empty string / None -> 0.0
# ---------------------------------------------------------------------------


def test_budget_model_empty_string_coerces_to_zero() -> None:
    """budget_chf '' is coerced to 0.0 and the model does not raise."""
    m = CategoryBudget.model_validate(_make_budget_payload(budget_chf=""))
    assert m.budget_chf == pytest.approx(0.0)


def test_budget_model_none_coerces_to_zero() -> None:
    """budget_chf None is coerced to 0.0 and the model does not raise."""
    m = CategoryBudget.model_validate(_make_budget_payload(budget_chf=None))
    assert m.budget_chf == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Negative clamping
# ---------------------------------------------------------------------------


def test_budget_model_negative_clamped_to_zero() -> None:
    """Negative budget_chf is clamped to 0.0, not silently accepted."""
    m = CategoryBudget.model_validate(_make_budget_payload(budget_chf=-50.0))
    assert m.budget_chf == pytest.approx(0.0)


def test_budget_model_large_negative_clamped_to_zero() -> None:
    """Large negative budget_chf is clamped to 0.0."""
    m = CategoryBudget.model_validate(_make_budget_payload(budget_chf=-99999.99))
    assert m.budget_chf == pytest.approx(0.0)


def test_budget_model_minus_one_clamped_to_zero() -> None:
    """budget_chf=-1 is clamped to 0.0."""
    m = CategoryBudget.model_validate(_make_budget_payload(budget_chf=-1))
    assert m.budget_chf == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Numeric string coercion
# ---------------------------------------------------------------------------


def test_budget_model_string_number_parses() -> None:
    """A numeric string budget_chf is coerced to float via float()."""
    m = CategoryBudget.model_validate(_make_budget_payload(budget_chf="123.45"))
    assert m.budget_chf == pytest.approx(123.45)


# ---------------------------------------------------------------------------
# model_dump() roundtrip
# ---------------------------------------------------------------------------


def test_budget_model_dump_roundtrip() -> None:
    """model_dump() + model_validate() roundtrip preserves both fields."""
    original = CategoryBudget.model_validate(_make_budget_payload())
    dumped = original.model_dump()
    restored = CategoryBudget.model_validate(dumped)
    assert restored.category == original.category
    assert restored.budget_chf == pytest.approx(original.budget_chf)


def test_budget_model_dump_has_expected_keys() -> None:
    """model_dump() returns a dict with exactly category and budget_chf keys."""
    m = CategoryBudget.model_validate(_make_budget_payload())
    d = m.model_dump()
    assert set(d.keys()) == {"category", "budget_chf"}


def test_budget_model_dump_values_match_fields() -> None:
    """model_dump() dict values match the model's field values."""
    m = CategoryBudget.model_validate(
        _make_budget_payload(category="Housing", budget_chf=1800.0)
    )
    d = m.model_dump()
    assert d["category"] == "Housing"
    assert d["budget_chf"] == pytest.approx(1800.0)
