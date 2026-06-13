"""Tests for swiss_exp_tracker.app.vis.figure.Fig."""

from __future__ import annotations

import calendar

from datetime import datetime
from typing import Any
from typing import Literal
from typing import cast

import pandas as pd
import plotly.graph_objects as go  # pyright: ignore[reportMissingTypeStubs]
import pytest

from swiss_exp_tracker.app.config import config
from swiss_exp_tracker.app.vis.figure import Fig

cfg = config()
F = Fig()

# ── factory helpers ───────────────────────────────────────────────────────────


def _make_grocery_row(**overrides: object) -> dict[str, object]:
    """Return a valid grocery aggregation row; each test states only what it varies."""
    base: dict[str, object] = {
        "Freq": "Monthly",
        "Period": "2024-01-01",
        "Merchant": "DailyStore",
        "total_CHF": 150.0,
        "totalPeriod_CHF": 150.0,
        "pct": 100.0,
    }
    base.update(overrides)
    return base


def _make_grocery_df(**overrides: object) -> pd.DataFrame:
    """Return a single-row grocery DataFrame; accepts the same overrides as _make_grocery_row."""
    return pd.DataFrame([_make_grocery_row(**overrides)])


def _make_cat_main_df() -> pd.DataFrame:
    """Return a two-row DataFrame with main category spend columns."""
    return pd.DataFrame(
        [
            {"category_main": "Groceries", "amount_CHF": 500.0},
            {"category_main": "Food", "amount_CHF": 200.0},
        ]
    )


def _make_donut_df() -> pd.DataFrame:
    """Return a two-row DataFrame suitable for fig_DonutByCategory."""
    return pd.DataFrame(
        [
            {"my_cat": "A", "my_amt": 300.0},
            {"my_cat": "B", "my_amt": 100.0},
        ]
    )


def _make_balance_df() -> pd.DataFrame:
    """Return 12 monthly balance rows spanning 2024-01-01 to 2024-12-01."""
    dates = pd.date_range("2024-01-01", periods=12, freq="MS")
    values = [5000.0 + i * 1000.0 for i in range(12)]
    return pd.DataFrame({"Date": dates, "Balance_CHF": values})


def _make_freq_cat_row(
    freq: str, period: str, amount: float, **overrides: object
) -> dict[str, object]:
    """Return a single row for the generic category frequency bar chart."""
    base: dict[str, object] = {
        "Freq": freq,
        "Period": period,
        "category": "Sport",
        "amount": amount,
    }
    base.update(overrides)
    return base


def _make_freq_cat_df(freq: str) -> pd.DataFrame:
    """Return a two-row DataFrame with two distinct periods and category='Sport'."""
    if freq == "Monthly":
        p1, p2 = "2024-01-01", "2024-02-01"
    else:
        p1, p2 = "2024", "2023"
    return pd.DataFrame(
        [
            _make_freq_cat_row(freq, p1, 80.0),
            _make_freq_cat_row(freq, p2, 120.0),
        ]
    )


def _make_heatmap_monthly_df() -> pd.DataFrame:
    """Return one row per calendar month (Month_num 1-12) for the monthly heatmap."""
    return pd.DataFrame(
        {
            "Month_num": list(range(1, 13)),
            "Month_name": [calendar.month_abbr[i] for i in range(1, 13)],
            "Year": [2024] * 12,
            "amount_CHF": [100.0 + i * 10.0 for i in range(12)],
        }
    )


# ── fig_BarGrocery ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("freq", ["Monthly", "Yearly"])
def test_fig_bar_grocery_returns_figure(freq: Literal["Monthly", "Yearly"]) -> None:
    total = 150.0 if freq == "Monthly" else 1500.0
    df = _make_grocery_df(Freq=freq, totalPeriod_CHF=total)
    fig = F.fig_BarGrocery(df, Freq=freq)
    assert isinstance(fig, go.Figure)
    assert fig.data


@pytest.mark.parametrize("freq", ["Monthly", "Yearly"])
def test_fig_bar_grocery_tick_alignment(freq: Literal["Monthly", "Yearly"]) -> None:
    total = 150.0 if freq == "Monthly" else 1500.0
    df = _make_grocery_df(Freq=freq, totalPeriod_CHF=total)
    fig = F.fig_BarGrocery(df, Freq=freq)
    layout: Any = fig.layout
    d_tick = cfg.vk_dTick_Grocery[freq]
    assert layout.yaxis.range[1] % d_tick == pytest.approx(0, abs=1e-6)


@pytest.mark.parametrize("freq", ["Monthly", "Yearly"])
def test_fig_bar_grocery_height_positive(freq: Literal["Monthly", "Yearly"]) -> None:
    total = 150.0 if freq == "Monthly" else 1500.0
    df = _make_grocery_df(Freq=freq, totalPeriod_CHF=total)
    fig = F.fig_BarGrocery(df, Freq=freq)
    layout: Any = fig.layout
    assert layout.height > 0


def test_fig_bar_grocery_empty_returns_figure() -> None:
    df = pd.DataFrame(
        columns=["Freq", "Period", "Merchant", "total_CHF", "totalPeriod_CHF", "pct"]
    )
    fig = F.fig_BarGrocery(df, Freq="Monthly")
    assert isinstance(fig, go.Figure)


# ── fig_BarGrocery_pct ────────────────────────────────────────────────────────


@pytest.mark.parametrize("freq", ["Monthly", "Yearly"])
def test_fig_bar_grocery_pct_returns_figure(freq: Literal["Monthly", "Yearly"]) -> None:
    df = _make_grocery_df(Freq=freq)
    fig = F.fig_BarGrocery_pct(df, Freq=freq)
    assert isinstance(fig, go.Figure)
    assert fig.data


@pytest.mark.parametrize("freq", ["Monthly", "Yearly"])
def test_fig_bar_grocery_pct_yaxis_range_fixed(
    freq: Literal["Monthly", "Yearly"],
) -> None:
    df = _make_grocery_df(Freq=freq)
    fig = F.fig_BarGrocery_pct(df, Freq=freq)
    layout: Any = fig.layout
    assert layout.yaxis.range[0] == pytest.approx(0)
    assert layout.yaxis.range[1] == pytest.approx(100)


def test_fig_bar_grocery_pct_empty_returns_figure() -> None:
    df = pd.DataFrame(
        columns=["Freq", "Period", "Merchant", "total_CHF", "totalPeriod_CHF", "pct"]
    )
    fig = F.fig_BarGrocery_pct(df, Freq="Monthly")
    assert isinstance(fig, go.Figure)


# ── fig_DonutCategoryMain ─────────────────────────────────────────────────────


def test_fig_donut_category_main_returns_single_trace() -> None:
    df = _make_cat_main_df()
    fig = F.fig_DonutCategoryMain(df)
    assert isinstance(fig, go.Figure)
    assert len(tuple(fig.data)) == 1


def test_fig_donut_category_main_hole_value() -> None:
    df = _make_cat_main_df()
    fig = F.fig_DonutCategoryMain(df)
    assert cast("go.Pie", fig.data[0]).hole == pytest.approx(0.4)


def test_fig_donut_category_main_legend_hidden() -> None:
    df = _make_cat_main_df()
    fig = F.fig_DonutCategoryMain(df)
    layout: Any = fig.layout
    assert layout.showlegend is False


def test_fig_donut_category_main_empty_returns_figure() -> None:
    df = pd.DataFrame(columns=["category_main", "amount_CHF"])
    fig = F.fig_DonutCategoryMain(df)
    assert isinstance(fig, go.Figure)


# ── fig_DonutByCategory ───────────────────────────────────────────────────────


def test_fig_donut_by_category_returns_single_trace() -> None:
    df = _make_donut_df()
    fig = F.fig_DonutByCategory(df, col_category="my_cat", col_amount="my_amt")
    assert isinstance(fig, go.Figure)
    assert len(tuple(fig.data)) == 1


def test_fig_donut_by_category_hole_value() -> None:
    df = _make_donut_df()
    fig = F.fig_DonutByCategory(df, col_category="my_cat", col_amount="my_amt")
    assert cast("go.Pie", fig.data[0]).hole == pytest.approx(0.4)


def test_fig_donut_by_category_empty_returns_figure() -> None:
    df = pd.DataFrame(columns=["my_cat", "my_amt"])
    fig = F.fig_DonutByCategory(df, col_category="my_cat", col_amount="my_amt")
    assert isinstance(fig, go.Figure)


# ── fig_BalancePerDay ─────────────────────────────────────────────────────────


def test_fig_balance_per_day_returns_figure() -> None:
    df = _make_balance_df()
    fig = F.fig_BalancePerDay(df)
    assert isinstance(fig, go.Figure)
    assert fig.data


def test_fig_balance_per_day_tickvals_present() -> None:
    df = _make_balance_df()
    fig = F.fig_BalancePerDay(df)
    layout: Any = fig.layout
    assert layout.xaxis.tickvals is not None
    assert len(layout.xaxis.tickvals) > 0


def test_fig_balance_per_day_ticktext_parseable() -> None:
    df = _make_balance_df()
    fig = F.fig_BalancePerDay(df)
    layout: Any = fig.layout
    for label in layout.xaxis.ticktext:
        datetime.strptime(label, "%b %y")


def test_fig_balance_per_day_height_positive() -> None:
    df = _make_balance_df()
    fig = F.fig_BalancePerDay(df)
    layout: Any = fig.layout
    assert layout.height > 0


def test_fig_balance_per_day_empty_returns_figure() -> None:
    df = pd.DataFrame(columns=["Date", "Balance_CHF"])
    fig = F.fig_BalancePerDay(df)
    assert isinstance(fig, go.Figure)


# ── fig_BarFreqByCategory ─────────────────────────────────────────────────────


@pytest.mark.parametrize("freq", ["Monthly", "Yearly"])
def test_fig_bar_freq_by_category_returns_figure(
    freq: Literal["Monthly", "Yearly"],
) -> None:
    df = _make_freq_cat_df(freq)
    fig = F.fig_BarFreqByCategory(
        df,
        col_catgeory="category",
        col_amount="amount",
        Freq=freq,
        dTick=100,
        npixel=60,
    )
    assert isinstance(fig, go.Figure)
    assert fig.data


@pytest.mark.parametrize("freq", ["Monthly", "Yearly"])
def test_fig_bar_freq_by_category_tick_alignment(
    freq: Literal["Monthly", "Yearly"],
) -> None:
    df = _make_freq_cat_df(freq)
    fig = F.fig_BarFreqByCategory(
        df,
        col_catgeory="category",
        col_amount="amount",
        Freq=freq,
        dTick=100,
        npixel=60,
    )
    layout: Any = fig.layout
    assert layout.yaxis.range[1] % 100 == pytest.approx(0, abs=1e-6)


@pytest.mark.parametrize("freq", ["Monthly", "Yearly"])
def test_fig_bar_freq_by_category_height_positive(
    freq: Literal["Monthly", "Yearly"],
) -> None:
    df = _make_freq_cat_df(freq)
    fig = F.fig_BarFreqByCategory(
        df,
        col_catgeory="category",
        col_amount="amount",
        Freq=freq,
        dTick=100,
        npixel=60,
    )
    layout: Any = fig.layout
    assert layout.height > 0


def test_fig_bar_freq_by_category_empty_returns_figure() -> None:
    df = pd.DataFrame(columns=["Freq", "Period", "category", "amount"])
    fig = F.fig_BarFreqByCategory(
        df,
        col_catgeory="category",
        col_amount="amount",
        Freq="Monthly",
        dTick=100,
        npixel=60,
    )
    assert isinstance(fig, go.Figure)


# ── fig_HeatmapMonthly ────────────────────────────────────────────────────────


def test_fig_heatmap_monthly_returns_figure() -> None:
    df = _make_heatmap_monthly_df()
    fig = F.fig_HeatmapMonthly(df)
    assert isinstance(fig, go.Figure)
    assert fig.data


def test_fig_heatmap_monthly_empty_returns_figure() -> None:
    df = pd.DataFrame(columns=["Month_num", "Month_name", "Year", "amount_CHF"])
    fig = F.fig_HeatmapMonthly(df)
    assert isinstance(fig, go.Figure)
