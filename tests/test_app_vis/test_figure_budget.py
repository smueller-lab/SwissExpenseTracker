"""Tests for get_fig_BudgetForecast in swiss_exp_tracker.app.vis.figure."""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.graph_objects as go  # pyright: ignore[reportMissingTypeStubs]
import pytest

from swiss_exp_tracker.app.libs import get_adaptive_dTick
from swiss_exp_tracker.app.vis.figure import get_fig_BudgetForecast

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_VK_MARGIN: dict[str, int] = {"l": 40, "r": 40, "t": 20, "b": 40}
_NPIXEL = 60


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def _make_lines_row(**overrides: object) -> dict[str, object]:
    """Return a valid pdf_lines row; each test states only what it varies."""
    base: dict[str, object] = {
        "category": "Groceries",
        "period_end": pd.Timestamp("2024-01-01"),
        "cumulative_chf": 200.0,
        "segment": "actual",
    }
    base.update(overrides)
    return base


def _make_lines_df_daily() -> pd.DataFrame:
    """Return a minimal pdf_lines DataFrame at daily cadence with one category."""
    dates = pd.date_range("2024-01-01", periods=30, freq="D")
    rows = [
        _make_lines_row(
            category="Groceries",
            period_end=d,
            cumulative_chf=float(i + 1) * 10.0,
            segment="actual",
        )
        for i, d in enumerate(dates)
    ]
    return pd.DataFrame(rows)


def _make_lines_df_monthly() -> pd.DataFrame:
    """Return a minimal pdf_lines DataFrame at monthly cadence with two categories."""
    dates = pd.date_range("2024-01-01", periods=6, freq="MS")
    rows: list[dict[str, object]] = []
    for i, d in enumerate(dates):
        rows.append(
            _make_lines_row(
                category="Groceries",
                period_end=d,
                cumulative_chf=float(i + 1) * 200.0,
                segment="actual",
            )
        )
        rows.append(
            _make_lines_row(
                category="Housing",
                period_end=d,
                cumulative_chf=float(i + 1) * 1800.0,
                segment="actual",
            )
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Daily cadence tests
# ---------------------------------------------------------------------------


def test_fig_budget_forecast_daily_returns_figure() -> None:
    """get_fig_BudgetForecast returns a go.Figure for daily-cadence input."""
    df = _make_lines_df_daily()
    fig = get_fig_BudgetForecast(df, _NPIXEL, _VK_MARGIN)
    assert isinstance(fig, go.Figure)


def test_fig_budget_forecast_daily_has_traces() -> None:
    """Daily-cadence figure has at least one data trace."""
    df = _make_lines_df_daily()
    fig = get_fig_BudgetForecast(df, _NPIXEL, _VK_MARGIN)
    assert len(fig.data) >= 1


def test_fig_budget_forecast_daily_height_positive() -> None:
    """Daily-cadence figure has a positive layout height."""
    df = _make_lines_df_daily()
    fig = get_fig_BudgetForecast(df, _NPIXEL, _VK_MARGIN)
    layout: Any = fig.layout
    assert layout.height > 0


def test_fig_budget_forecast_daily_yaxis_range_divisible_by_dtick() -> None:
    """Daily-cadence y-axis range end is an exact multiple of dtick."""
    df = _make_lines_df_daily()
    fig = get_fig_BudgetForecast(df, _NPIXEL, _VK_MARGIN)
    layout: Any = fig.layout
    d_tick = layout.yaxis.dtick
    range_end = layout.yaxis.range[1]
    assert d_tick is not None
    assert d_tick > 0
    assert range_end % d_tick == pytest.approx(0, abs=1e-6)


def test_fig_budget_forecast_daily_dtick_matches_adaptive() -> None:
    """dtick equals get_adaptive_dTick applied to the maximum cumulative_chf."""
    df = _make_lines_df_daily()
    expected_dtick = get_adaptive_dTick(float(df["cumulative_chf"].max()))
    fig = get_fig_BudgetForecast(df, _NPIXEL, _VK_MARGIN)
    layout: Any = fig.layout
    assert layout.yaxis.dtick == pytest.approx(expected_dtick)


# ---------------------------------------------------------------------------
# Monthly cadence tests
# ---------------------------------------------------------------------------


def test_fig_budget_forecast_monthly_returns_figure() -> None:
    """get_fig_BudgetForecast returns a go.Figure for monthly-cadence input."""
    df = _make_lines_df_monthly()
    fig = get_fig_BudgetForecast(df, _NPIXEL, _VK_MARGIN)
    assert isinstance(fig, go.Figure)


def test_fig_budget_forecast_monthly_has_traces() -> None:
    """Monthly-cadence figure has at least one data trace per category."""
    df = _make_lines_df_monthly()
    fig = get_fig_BudgetForecast(df, _NPIXEL, _VK_MARGIN)
    assert len(fig.data) >= 1


def test_fig_budget_forecast_monthly_height_positive() -> None:
    """Monthly-cadence figure has a positive layout height."""
    df = _make_lines_df_monthly()
    fig = get_fig_BudgetForecast(df, _NPIXEL, _VK_MARGIN)
    layout: Any = fig.layout
    assert layout.height > 0


def test_fig_budget_forecast_monthly_yaxis_range_divisible_by_dtick() -> None:
    """Monthly-cadence y-axis range end is an exact multiple of dtick."""
    df = _make_lines_df_monthly()
    fig = get_fig_BudgetForecast(df, _NPIXEL, _VK_MARGIN)
    layout: Any = fig.layout
    d_tick = layout.yaxis.dtick
    range_end = layout.yaxis.range[1]
    assert d_tick is not None
    assert d_tick > 0
    assert range_end % d_tick == pytest.approx(0, abs=1e-6)


def test_fig_budget_forecast_monthly_dtick_matches_adaptive() -> None:
    """dtick equals get_adaptive_dTick applied to the maximum cumulative_chf."""
    df = _make_lines_df_monthly()
    expected_dtick = get_adaptive_dTick(float(df["cumulative_chf"].max()))
    fig = get_fig_BudgetForecast(df, _NPIXEL, _VK_MARGIN)
    layout: Any = fig.layout
    assert layout.yaxis.dtick == pytest.approx(expected_dtick)


def test_fig_budget_forecast_monthly_two_categories_two_actual_traces() -> None:
    """Monthly figure with two categories produces at least two visible (showlegend) traces."""
    df = _make_lines_df_monthly()
    fig = get_fig_BudgetForecast(df, _NPIXEL, _VK_MARGIN)
    visible_traces = [t for t in fig.data if getattr(t, "showlegend", True)]
    assert len(visible_traces) >= 2


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_fig_budget_forecast_empty_df_returns_figure() -> None:
    """Empty DataFrame does not raise; a placeholder go.Figure is returned."""
    df = pd.DataFrame(columns=["category", "period_end", "cumulative_chf", "segment"])
    fig = get_fig_BudgetForecast(df, _NPIXEL, _VK_MARGIN)
    assert isinstance(fig, go.Figure)


def test_fig_budget_forecast_single_point_returns_figure() -> None:
    """A single data point does not raise and returns a go.Figure."""
    df = pd.DataFrame([_make_lines_row()])
    fig = get_fig_BudgetForecast(df, _NPIXEL, _VK_MARGIN)
    assert isinstance(fig, go.Figure)
    assert len(fig.data) >= 1
