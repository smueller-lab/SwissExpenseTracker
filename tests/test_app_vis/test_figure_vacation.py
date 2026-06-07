"""Tests for vacation-related figure methods in swiss_exp_tracker.app.vis.figure.Fig."""

from __future__ import annotations

from typing import Any
from typing import cast

import numpy as np
import pandas as pd
import plotly.graph_objects as go  # pyright: ignore[reportMissingTypeStubs]
import pytest

from swiss_exp_tracker.app.vis.figure import Fig

F = Fig()

# ── factory helpers ───────────────────────────────────────────────────────────

# Six category names — 4 high-spend, 2 low-spend (for top-4 filter tests)
_CATEGORIES = ["Accommodation", "Transport", "Food", "Activities", "Shopping", "Misc"]
_N_TRANSACTIONS = 5


def _make_vacation_txn_row(**overrides: object) -> dict[str, object]:
    """Return a valid vacation transaction row; each test states only what it varies."""
    base: dict[str, object] = {
        "category_second": "Accommodation",
        "amount": 200.0,
    }
    base.update(overrides)
    return base


def _make_vacation_txn_df_6cats() -> pd.DataFrame:
    """Return a DataFrame with 6 categories x 5 transactions, varied amounts.

    The first 4 categories have higher total spend than the last 2.
    """
    rng = np.random.default_rng(42)
    rows: list[dict[str, object]] = []
    # High-spend categories: amounts 100-500
    for cat in _CATEGORIES[:4]:
        for _ in range(_N_TRANSACTIONS):
            rows.append(
                _make_vacation_txn_row(
                    category_second=cat, amount=float(rng.integers(100, 500))
                )
            )
    # Low-spend categories: amounts 10-30
    for cat in _CATEGORIES[4:]:
        for _ in range(_N_TRANSACTIONS):
            rows.append(
                _make_vacation_txn_row(
                    category_second=cat, amount=float(rng.integers(10, 30))
                )
            )
    return pd.DataFrame(rows)


def _make_vacation_txn_df_simple() -> pd.DataFrame:
    """Return a three-category DataFrame with distinct amounts per category."""
    rows: list[dict[str, object]] = []
    amounts = {
        "Alpha": [300.0, 310.0, 290.0],
        "Beta": [100.0, 110.0, 90.0],
        "Gamma": [50.0, 60.0, 40.0],
    }
    for cat, vals in amounts.items():
        for v in vals:
            rows.append(_make_vacation_txn_row(category_second=cat, amount=v))
    return pd.DataFrame(rows)


# ── fig_BoxplotVacation happy path ────────────────────────────────────────────


def test_fig_boxplot_vacation_returns_figure() -> None:
    """fig_BoxplotVacation returns a go.Figure for valid non-empty input."""
    df = _make_vacation_txn_df_6cats()
    fig = F.fig_BoxplotVacation(df)
    assert isinstance(fig, go.Figure)


def test_fig_boxplot_vacation_has_four_traces() -> None:
    """fig_BoxplotVacation limits output to exactly 4 Box traces (top 4 by spend)."""
    df = _make_vacation_txn_df_6cats()
    fig = F.fig_BoxplotVacation(df)
    assert len(fig.data) == 4


def test_fig_boxplot_vacation_top4_by_total_spend() -> None:
    """The 4 traces correspond to the 4 high-spend categories; the 2 low-spend ones are absent."""
    df = _make_vacation_txn_df_6cats()
    fig = F.fig_BoxplotVacation(df)
    trace_names = {cast("go.Box", trace).name for trace in fig.data}
    # High-spend categories must all be present
    for cat in _CATEGORIES[:4]:
        assert cat in trace_names, f"Expected high-spend category '{cat}' in traces"
    # Low-spend categories must be absent
    for cat in _CATEGORIES[4:]:
        assert cat not in trace_names, f"Low-spend category '{cat}' should be excluded"


def test_fig_boxplot_vacation_sorted_by_median_descending() -> None:
    """First trace name is the category with the highest median among the top 4."""
    # Build DataFrame where medians are clearly ranked: Alpha > Beta > Gamma (all in top 4)
    rows: list[dict[str, object]] = []
    # Alpha: median ~300; Beta: median ~100; Gamma: median ~50
    for v in [280.0, 300.0, 320.0, 290.0, 310.0]:
        rows.append(_make_vacation_txn_row(category_second="Alpha", amount=v))
    for v in [90.0, 100.0, 110.0, 95.0, 105.0]:
        rows.append(_make_vacation_txn_row(category_second="Beta", amount=v))
    for v in [40.0, 50.0, 60.0, 45.0, 55.0]:
        rows.append(_make_vacation_txn_row(category_second="Gamma", amount=v))
    for v in [20.0, 25.0, 22.0, 18.0, 23.0]:
        rows.append(_make_vacation_txn_row(category_second="Delta", amount=v))
    df = pd.DataFrame(rows)

    fig = F.fig_BoxplotVacation(df)
    assert cast("go.Box", fig.data[0]).name == "Alpha"


def test_fig_boxplot_vacation_yaxis_tick_alignment() -> None:
    """Y-axis range end is an exact multiple of the dtick (no remainder)."""
    df = _make_vacation_txn_df_6cats()
    fig = F.fig_BoxplotVacation(df)
    layout: Any = fig.layout
    d_tick = layout.yaxis.dtick
    range_end = layout.yaxis.range[1]
    assert d_tick is not None
    assert d_tick > 0
    assert range_end % d_tick == pytest.approx(0, abs=1e-6)


def test_fig_boxplot_vacation_each_trace_is_box() -> None:
    """Every data trace in the figure is a go.Box instance."""
    df = _make_vacation_txn_df_6cats()
    fig = F.fig_BoxplotVacation(df)
    for trace in fig.data:
        assert isinstance(trace, go.Box)


# ── fig_BoxplotVacation empty input ──────────────────────────────────────────


def test_fig_boxplot_vacation_empty_input_returns_figure() -> None:
    """Empty DataFrame returns a go.Figure (no-data placeholder)."""
    df = pd.DataFrame(columns=["category_second", "amount"])
    fig = F.fig_BoxplotVacation(df)
    assert isinstance(fig, go.Figure)


def test_fig_boxplot_vacation_empty_input_annotation_text() -> None:
    """The placeholder figure's annotation contains 'vacation transaction data'."""
    df = pd.DataFrame(columns=["category_second", "amount"])
    fig = F.fig_BoxplotVacation(df)
    layout: Any = fig.layout
    texts = [ann.text for ann in layout.annotations]
    assert any("vacation transaction data" in t for t in texts)


# ── fig_BoxplotVacation fewer than 4 categories ──────────────────────────────


def test_fig_boxplot_vacation_fewer_than_4_categories() -> None:
    """With only 3 categories, all 3 appear as traces (no crash on nlargest(4))."""
    df = _make_vacation_txn_df_simple()
    fig = F.fig_BoxplotVacation(df)
    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 3


# ── fig_DonutByCategory smoke test ───────────────────────────────────────────


def _make_donut_vacation_df() -> pd.DataFrame:
    """Return a three-row DataFrame with category_second and Total columns."""
    return pd.DataFrame(
        [
            {"category_second": "Accommodation", "Total": 1200.0},
            {"category_second": "Transport", "Total": 800.0},
            {"category_second": "Activities", "Total": 400.0},
        ]
    )


def test_fig_donut_by_category_vacation_returns_figure() -> None:
    """fig_DonutByCategory works correctly with vacation category/Total columns."""
    df = _make_donut_vacation_df()
    fig = F.fig_DonutByCategory(df, col_category="category_second", col_amount="Total")
    assert isinstance(fig, go.Figure)


def test_fig_donut_by_category_vacation_single_trace() -> None:
    """fig_DonutByCategory returns exactly one Pie trace."""
    df = _make_donut_vacation_df()
    fig = F.fig_DonutByCategory(df, col_category="category_second", col_amount="Total")
    assert len(tuple(fig.data)) == 1


def test_fig_donut_by_category_vacation_hole_value() -> None:
    """fig_DonutByCategory always produces a donut with hole=0.4."""
    df = _make_donut_vacation_df()
    fig = F.fig_DonutByCategory(df, col_category="category_second", col_amount="Total")
    assert cast("go.Pie", fig.data[0]).hole == pytest.approx(0.4)


def test_fig_donut_by_category_vacation_legend_hidden() -> None:
    """fig_DonutByCategory hides the legend."""
    df = _make_donut_vacation_df()
    fig = F.fig_DonutByCategory(df, col_category="category_second", col_amount="Total")
    layout: Any = fig.layout
    assert layout.showlegend is False
