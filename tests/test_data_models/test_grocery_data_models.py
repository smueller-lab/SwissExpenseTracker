from __future__ import annotations

from datetime import datetime

import pytest

from pydantic import ValidationError

from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryData
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryDetail
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryMain
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryResult
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryRow


def _make_category_data(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "article": "Milk 1L",
        "category_main": GroceryCategoryMain.DAIRY_EGGS,
        "category_detail": GroceryCategoryDetail.MILK_CREAM,
    }
    base.update(overrides)
    return base


def _make_grocery_row(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "rfn_id": 1,
        "article": "Milch 1L",
        "article_normalized": "milch 1l",
        "location": "Zurich HB",
    }
    base.update(overrides)
    return base


def _make_category_result(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "current_datetime": datetime(2026, 1, 15, 10, 30),
        "rfn_id": 1,
        "article": "Milch 1L",
        "matched_article": "Milk 1L",
        "cache_hit": True,
        "similarity": 0.95,
        "category_main": GroceryCategoryMain.DAIRY_EGGS,
        "category_detail": GroceryCategoryDetail.MILK_CREAM,
    }
    base.update(overrides)
    return base


def test_grocery_category_main_values_are_strings() -> None:
    assert GroceryCategoryMain.DAIRY_EGGS.value == "Dairy & Eggs"
    assert GroceryCategoryMain.OTHER.value == "Other"


def test_grocery_category_detail_values_are_strings() -> None:
    assert GroceryCategoryDetail.MILK_CREAM.value == "Milk & Cream"
    assert GroceryCategoryDetail.UNKNOWN.value == "Unknown"


def test_grocery_category_data_parses_valid_row() -> None:
    model = GroceryCategoryData.model_validate(_make_category_data())
    assert model.article == "Milk 1L"
    assert model.category_main == GroceryCategoryMain.DAIRY_EGGS
    assert model.category_detail == GroceryCategoryDetail.MILK_CREAM


def test_grocery_category_data_accepts_enum_value_strings() -> None:
    model = GroceryCategoryData.model_validate(
        _make_category_data(category_main="Dairy & Eggs", category_detail="Cheese")
    )
    assert model.category_main == GroceryCategoryMain.DAIRY_EGGS
    assert model.category_detail == GroceryCategoryDetail.CHEESE


def test_grocery_category_data_rejects_unknown_category() -> None:
    with pytest.raises(ValidationError):
        GroceryCategoryData.model_validate(_make_category_data(category_main="Toys"))


def test_grocery_row_parses_valid_row() -> None:
    model = GroceryRow.model_validate(_make_grocery_row())
    assert model.rfn_id == 1
    assert model.article == "Milch 1L"
    assert model.article_normalized == "milch 1l"
    assert model.location == "Zurich HB"


def test_grocery_row_requires_rfn_id() -> None:
    row = _make_grocery_row()
    del row["rfn_id"]
    with pytest.raises(ValidationError):
        GroceryRow.model_validate(row)


def test_grocery_category_result_parses_valid_row() -> None:
    model = GroceryCategoryResult.model_validate(_make_category_result())
    assert model.rfn_id == 1
    assert model.cache_hit is True
    assert model.similarity == pytest.approx(0.95)
    assert model.category_main == GroceryCategoryMain.DAIRY_EGGS
    assert model.category_detail == GroceryCategoryDetail.MILK_CREAM


def test_grocery_category_result_allows_none_similarity() -> None:
    model = GroceryCategoryResult.model_validate(
        _make_category_result(similarity=None, cache_hit=False)
    )
    assert model.similarity is None
    assert model.cache_hit is False


def test_grocery_category_result_roundtrip() -> None:
    model = GroceryCategoryResult.model_validate(_make_category_result())
    dumped = model.model_dump(mode="python")
    revalidated = GroceryCategoryResult.model_validate(dumped)
    assert revalidated == model
