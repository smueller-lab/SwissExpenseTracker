"""Tests for Fig.fig_BarTripCostByYear in swiss_exp_tracker.app.vis.figure."""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.graph_objects as go  # pyright: ignore[reportMissingTypeStubs]
import pytest

from swiss_exp_tracker.app.config import config
from swiss_exp_tracker.app.libs import get_adaptive_dTick
from swiss_exp_tracker.app.vis.figure import Fig

cfg = config()

# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def _make_trip_cat_row(**overrides: object) -> dict[str, object]:
    """Return a valid pdf_TripsByCategoryYear row; each test states only what it varies."""
    base: dict[str, object] = {
        "year": 2025,
        "trip_id": 1,
        "trip_name": "Ibiza Summer",
        "category_main": "Travel",
        "total_chf": 500.0,
    }
    base.update(overrides)
    return base


def _make_minimal_df(*rows: dict[str, object]) -> pd.DataFrame:
    """Build a pdf_TripsByCategoryYear DataFrame from one or more row dicts."""
    return pd.DataFrame(list(rows))


def _make_empty_df() -> pd.DataFrame:
    """Return an empty DataFrame with the pdf_TripsByCategoryYear columns."""
    return pd.DataFrame(
        columns=["year", "trip_id", "trip_name", "category_main", "total_chf"]
    )


# ---------------------------------------------------------------------------
# Standard single-trip figure
# ---------------------------------------------------------------------------


def test_fig_bar_trip_cost_by_year_returns_figure() -> None:
    """fig_BarTripCostByYear returns a go.Figure for valid input."""
    df = _make_minimal_df(
        _make_trip_cat_row(category_main="Travel", total_chf=300.0),
        _make_trip_cat_row(category_main="Restaurant", total_chf=200.0),
    )
    fig = Fig().fig_BarTripCostByYear(df)
    assert isinstance(fig, go.Figure)


def test_fig_bar_trip_cost_by_year_has_at_least_one_trace() -> None:
    """fig_BarTripCostByYear produces at least one data trace per category."""
    df = _make_minimal_df(
        _make_trip_cat_row(category_main="Travel", total_chf=300.0),
        _make_trip_cat_row(category_main="Restaurant", total_chf=150.0),
    )
    fig = Fig().fig_BarTripCostByYear(df)
    assert len(fig.data) >= 1


def test_fig_bar_trip_cost_by_year_height_positive() -> None:
    """Layout height is strictly positive."""
    df = _make_minimal_df(
        _make_trip_cat_row(category_main="Travel", total_chf=400.0),
    )
    fig = Fig().fig_BarTripCostByYear(df)
    layout: Any = fig.layout
    assert layout.height > 0


def test_fig_bar_trip_cost_by_year_yaxis_range_end_divisible_by_dtick() -> None:
    """Y-axis range end is an exact multiple of dtick (snapped to tick boundary)."""
    df = _make_minimal_df(
        _make_trip_cat_row(category_main="Travel", total_chf=350.0),
        _make_trip_cat_row(category_main="Restaurant", total_chf=180.0),
    )
    fig = Fig().fig_BarTripCostByYear(df)
    layout: Any = fig.layout
    d_tick = layout.yaxis.dtick
    range_end = layout.yaxis.range[1]
    assert d_tick is not None
    assert d_tick > 0
    assert range_end % d_tick == pytest.approx(0, abs=1e-6)


def test_fig_bar_trip_cost_by_year_two_categories_two_traces() -> None:
    """Two categories produce at least two Bar traces."""
    df = _make_minimal_df(
        _make_trip_cat_row(category_main="Travel", total_chf=400.0),
        _make_trip_cat_row(category_main="Groceries", total_chf=100.0),
    )
    fig = Fig().fig_BarTripCostByYear(df)
    assert len(fig.data) >= 2


def test_fig_bar_trip_cost_by_year_barmode_is_stack() -> None:
    """barmode is set to 'stack'."""
    df = _make_minimal_df(
        _make_trip_cat_row(category_main="Travel", total_chf=300.0),
    )
    fig = Fig().fig_BarTripCostByYear(df)
    layout: Any = fig.layout
    assert layout.barmode == "stack"


# ---------------------------------------------------------------------------
# y-range computed from per-trip totals, not per-year sums
# ---------------------------------------------------------------------------


def test_fig_bar_trip_cost_by_year_yrange_from_per_trip_not_per_year_sum() -> None:
    """With two trips in the same year, dTick is based on the larger single-trip total
    (not the sum of both trips in that year).
    """
    # Trip A total: 600, Trip B total: 400, year sum: 1000
    # dTick should accommodate max per-trip total = 600, not 1000.
    df = _make_minimal_df(
        _make_trip_cat_row(
            year=2025,
            trip_id=1,
            trip_name="Trip A",
            category_main="Travel",
            total_chf=600.0,
        ),
        _make_trip_cat_row(
            year=2025,
            trip_id=2,
            trip_name="Trip B",
            category_main="Travel",
            total_chf=400.0,
        ),
    )
    fig = Fig().fig_BarTripCostByYear(df)
    layout: Any = fig.layout

    # The y-range must accommodate the max per-trip total (600), not the year sum (1000)
    range_end = layout.yaxis.range[1]
    d_tick = layout.yaxis.dtick
    assert range_end >= 600.0

    # dTick must be computed from the max per-trip total (600), not from the year sum (1000).
    expected_dtick_per_trip = get_adaptive_dTick(600.0)
    assert d_tick == pytest.approx(expected_dtick_per_trip, rel=1e-6)
    # The range end must NOT be derived from the 1000 year sum.
    assert range_end < 1000.0


def test_fig_bar_trip_cost_by_year_yrange_end_covers_max_trip_total() -> None:
    """Y-range end is >= max per-trip total, regardless of number of trips per year."""
    trip_a_total = 750.0
    trip_b_total = 300.0
    df = _make_minimal_df(
        _make_trip_cat_row(
            year=2026,
            trip_id=1,
            trip_name="Long Trip",
            category_main="Transport",
            total_chf=trip_a_total,
        ),
        _make_trip_cat_row(
            year=2026,
            trip_id=2,
            trip_name="Short Trip",
            category_main="Restaurant",
            total_chf=trip_b_total,
        ),
    )
    fig = Fig().fig_BarTripCostByYear(df)
    layout: Any = fig.layout
    range_end = layout.yaxis.range[1]
    assert range_end >= trip_a_total


# ---------------------------------------------------------------------------
# "Other" bucket — present and last in stack order
# ---------------------------------------------------------------------------


def test_fig_bar_trip_cost_by_year_other_bucket_when_many_categories() -> None:
    """When a trip has more than category_top_n_bar categories, an 'Other' trace is present."""
    # Build a single trip with more than cfg.category_top_n_bar distinct categories.
    n_categories = cfg.category_top_n_bar + 3
    rows = [
        _make_trip_cat_row(
            trip_id=1,
            trip_name="Mega Trip",
            category_main=f"Category{i}",
            total_chf=float(100 - i),  # decreasing totals so top-N cuts the tail
        )
        for i in range(n_categories)
    ]
    df = _make_minimal_df(*rows)
    fig = Fig().fig_BarTripCostByYear(df)

    trace_names = [str(t.name) for t in fig.data]
    assert "Other" in trace_names


def test_fig_bar_trip_cost_by_year_other_bucket_is_last_in_stack() -> None:
    """When 'Other' is present, its trace is the last in fig.data (last in stack order)."""
    n_categories = cfg.category_top_n_bar + 2
    rows = [
        _make_trip_cat_row(
            trip_id=1,
            trip_name="Many Cat Trip",
            category_main=f"Category{i}",
            total_chf=float(200 - i),
        )
        for i in range(n_categories)
    ]
    df = _make_minimal_df(*rows)
    fig = Fig().fig_BarTripCostByYear(df)

    trace_names = [str(t.name) for t in fig.data]
    assert "Other" in trace_names
    assert trace_names[-1] == "Other"


# ---------------------------------------------------------------------------
# Empty input — placeholder path does not raise
# ---------------------------------------------------------------------------


def test_fig_bar_trip_cost_by_year_empty_df_returns_figure() -> None:
    """Empty DataFrame returns a go.Figure without raising."""
    df = _make_empty_df()
    fig = Fig().fig_BarTripCostByYear(df)
    assert isinstance(fig, go.Figure)


def test_fig_bar_trip_cost_by_year_empty_df_placeholder_has_no_data_traces() -> None:
    """Placeholder figure for empty input has no Bar traces (only annotation)."""
    df = _make_empty_df()
    fig = Fig().fig_BarTripCostByYear(df)
    bar_traces = [t for t in fig.data if isinstance(t, go.Bar)]
    assert len(bar_traces) == 0


# ---------------------------------------------------------------------------
# Multi-year grouping
# ---------------------------------------------------------------------------


def test_fig_bar_trip_cost_by_year_multiple_years_produces_traces() -> None:
    """Trips in two different years each produce Bar traces."""
    df = _make_minimal_df(
        _make_trip_cat_row(
            year=2024,
            trip_id=1,
            trip_name="Past Trip",
            category_main="Travel",
            total_chf=400.0,
        ),
        _make_trip_cat_row(
            year=2025,
            trip_id=2,
            trip_name="Recent Trip",
            category_main="Travel",
            total_chf=500.0,
        ),
    )
    fig = Fig().fig_BarTripCostByYear(df)
    # At least one trace per figure
    assert len(fig.data) >= 1
    # Multicategory x-axis: each trace x should be a list/tuple of two arrays
    first_trace: Any = fig.data[0]
    x_val = first_trace.x
    assert len(x_val) == 2  # [years_array, trip_names_array]
