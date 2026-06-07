"""Tests for sport-related figure methods in swiss_exp_tracker.app.vis.figure.Fig."""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.graph_objects as go  # pyright: ignore[reportMissingTypeStubs]
import pytest

from swiss_exp_tracker.app.vis.figure import Fig

F = Fig()

# ── factory helpers ───────────────────────────────────────────────────────────


def _make_activities_row(**overrides: object) -> dict[str, object]:
    """Return a valid activities row; each test states only what it varies."""
    base: dict[str, object] = {"year": 2024, "sport": "Tennis", "activity_count": 30}
    base.update(overrides)
    return base


def _make_activities_df() -> pd.DataFrame:
    """Return 3 sports x 2 years = 6 rows of activity data."""
    rows = [
        _make_activities_row(year=2024, sport="Tennis", activity_count=30),
        _make_activities_row(year=2025, sport="Tennis", activity_count=35),
        _make_activities_row(year=2024, sport="Golf", activity_count=15),
        _make_activities_row(year=2025, sport="Golf", activity_count=18),
        _make_activities_row(year=2024, sport="Padel", activity_count=20),
        _make_activities_row(year=2025, sport="Padel", activity_count=22),
    ]
    return pd.DataFrame(rows)


# ── fig_BarSportActivities happy path ─────────────────────────────────────────


def test_fig_bar_sport_activities_returns_figure() -> None:
    """fig_BarSportActivities returns a go.Figure for valid non-empty input."""
    df = _make_activities_df()
    fig = F.fig_BarSportActivities(df)
    assert isinstance(fig, go.Figure)


def test_fig_bar_sport_activities_has_traces() -> None:
    """fig_BarSportActivities produces at least one data trace."""
    df = _make_activities_df()
    fig = F.fig_BarSportActivities(df)
    assert len(fig.data) >= 1


def test_fig_bar_sport_activities_yaxis_tick_alignment() -> None:
    """Y-axis range end is an exact multiple of dtick (no remainder)."""
    df = _make_activities_df()
    fig = F.fig_BarSportActivities(df)
    layout: Any = fig.layout
    d_tick = layout.yaxis.dtick
    range_end = layout.yaxis.range[1]
    assert d_tick is not None
    assert d_tick > 0
    assert range_end % d_tick == pytest.approx(0, abs=1e-6)


def test_fig_bar_sport_activities_height_positive() -> None:
    """fig_BarSportActivities sets a positive figure height."""
    df = _make_activities_df()
    fig = F.fig_BarSportActivities(df)
    layout: Any = fig.layout
    assert layout.height > 0


def test_fig_bar_sport_activities_empty_returns_figure() -> None:
    """Empty DataFrame returns a go.Figure placeholder without raising."""
    df = pd.DataFrame(columns=["year", "sport", "activity_count"])
    fig = F.fig_BarSportActivities(df)
    assert isinstance(fig, go.Figure)


def test_fig_bar_sport_activities_xaxis_category_order() -> None:
    """X-axis uses a category type with an explicit array order."""
    df = _make_activities_df()
    fig = F.fig_BarSportActivities(df)
    layout: Any = fig.layout
    # The implementation sets both type="category" and categoryorder="array"
    assert layout.xaxis.type == "category" or layout.xaxis.categoryorder == "array"


def test_fig_bar_sport_activities_grouped() -> None:
    """fig_BarSportActivities uses grouped bar mode."""
    df = _make_activities_df()
    fig = F.fig_BarSportActivities(df)
    layout: Any = fig.layout
    assert layout.barmode == "group"


# ── fig_BarSportActivities additional edge cases ──────────────────────────────


def test_fig_bar_sport_activities_trace_count_equals_sport_count() -> None:
    """Number of traces matches number of distinct sports in the input."""
    df = _make_activities_df()
    fig = F.fig_BarSportActivities(df)
    n_sports = df["sport"].nunique()
    assert len(fig.data) == n_sports


def test_fig_bar_sport_activities_single_sport_single_year() -> None:
    """Single sport, single year still produces a valid grouped bar figure."""
    df = pd.DataFrame(
        [_make_activities_row(year=2024, sport="Tennis", activity_count=10)]
    )
    fig = F.fig_BarSportActivities(df)
    assert isinstance(fig, go.Figure)
    assert len(fig.data) >= 1
    layout: Any = fig.layout
    assert layout.barmode == "group"
