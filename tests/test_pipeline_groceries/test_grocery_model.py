from __future__ import annotations

from datetime import date
from datetime import time

import pytest

from swiss_exp_tracker.pipeline_ingestion.data_models.grocery import GroceryItem
from swiss_exp_tracker.pipeline_ingestion.data_models.grocery import GroceryUnit


def _make_row(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "Datum": "17.05.2026",
        "Zeit": "09:04:29",
        "Filiale": "M EX Altstetten",
        "Kassennummer": "257",
        "Transaktionsnummer": "51",
        "Artikel": "Vollmilch Past 1l",
        "Menge": "1",
        "Aktion": "0.00",
        "Umsatz": "1.80",
    }
    base.update(overrides)
    return base


def test_german_aliases_parsed() -> None:
    item = GroceryItem.model_validate(_make_row())
    assert item.date == date(2026, 5, 17)
    assert item.time == time(9, 4, 29)
    assert item.location == "M EX Altstetten"
    assert item.article == "Vollmilch Past 1l"
    assert item.quantity == 1.0
    assert item.discount == 0.0
    assert item.price == 1.80


def test_extra_columns_ignored() -> None:
    row = _make_row()
    row["UnknownColumn"] = "garbage"
    item = GroceryItem.model_validate(row)
    assert item.article == "Vollmilch Past 1l"


def test_unit_qty_for_integer_quantity() -> None:
    item = GroceryItem.model_validate(_make_row(Menge="2"))
    assert item.unit == GroceryUnit.QTY


def test_unit_kg_for_decimal_quantity() -> None:
    item = GroceryItem.model_validate(_make_row(Menge="0.250"))
    assert item.unit == GroceryUnit.KG


def test_is_bonus_row_true_when_price_and_discount_zero() -> None:
    item = GroceryItem.model_validate(
        _make_row(Menge="1", Aktion="0.00", Umsatz="0.00")
    )
    assert item.is_bonus_row is True


def test_is_bonus_row_false_when_price_nonzero() -> None:
    item = GroceryItem.model_validate(_make_row())
    assert item.is_bonus_row is False


def test_is_return_row_true_for_negative_quantity() -> None:
    item = GroceryItem.model_validate(_make_row(Menge="-1", Umsatz="-1.80"))
    assert item.is_return_row is True


def test_is_return_row_false_for_positive_quantity() -> None:
    item = GroceryItem.model_validate(_make_row())
    assert item.is_return_row is False


def test_comma_decimal_separator_parsed() -> None:
    item = GroceryItem.model_validate(_make_row(Menge="0,250", Umsatz="2,10"))
    assert item.quantity == pytest.approx(0.25)
    assert item.price == pytest.approx(2.10)


def test_date_format_parsed() -> None:
    item = GroceryItem.model_validate(_make_row(Datum="01.01.2026"))
    assert item.date == date(2026, 1, 1)


def test_time_format_parsed() -> None:
    item = GroceryItem.model_validate(_make_row(Zeit="23:59:59"))
    assert item.time == time(23, 59, 59)
