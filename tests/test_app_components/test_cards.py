"""Tests for app/components/cards.make_number_card — must not crash on missing values."""

from __future__ import annotations

import math

import pytest

from dash import html

from swiss_exp_tracker.app.components.cards import make_number_card


def _value_text(card: html.Div) -> str:
    """Return the rendered text of the card's value paragraph."""
    value_p = card.children[1]
    return str(value_p.children)


def test_make_number_card_formats_number() -> None:
    """A normal value renders formatted with its unit."""
    card = make_number_card("Current Balance", 1234.5)
    assert _value_text(card) == "1,234.50 CHF"


@pytest.mark.parametrize("missing", [None, math.nan])
def test_make_number_card_handles_missing_value(missing: float | None) -> None:
    """None / NaN render a placeholder instead of raising (regression for the KPI crash)."""
    card = make_number_card("Current Balance", missing)
    assert _value_text(card) == "—"
    assert "kpi-value" in card.children[1].className


def test_make_number_card_negative_gets_negative_class() -> None:
    """A negative value is styled with the negative KPI class."""
    card = make_number_card("Net", -10.0)
    assert "kpi-negative" in card.children[1].className
