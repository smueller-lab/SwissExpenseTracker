"""Tests for swiss_exp_tracker.app.vis.figure_investing standalone functions."""

from __future__ import annotations

from typing import cast

import pandas as pd
import plotly.graph_objects as go  # pyright: ignore[reportMissingTypeStubs]
import pytest

from swiss_exp_tracker.app.vis.figure_investing import fig_allocation_donut
from swiss_exp_tracker.app.vis.figure_investing import fig_portfolio_progression
from swiss_exp_tracker.app.vis.figure_investing import fig_position_pct
from swiss_exp_tracker.app.vis.figure_investing import fig_position_value

# ── factory helpers ───────────────────────────────────────────────────────────


def _make_allocation_row(**overrides: object) -> dict[str, object]:
    """Return a valid position-allocation row; each test states only what it varies."""
    base: dict[str, object] = {
        "symbol": "VWRL",
        "value_chf": 10000.0,
        "name": "Vanguard FTSE All-World",
    }
    base.update(overrides)
    return base


def _make_allocation_df(**overrides: object) -> pd.DataFrame:
    """Return a single-row allocation DataFrame."""
    return pd.DataFrame([_make_allocation_row(**overrides)])


def _make_portfolio_row(
    date: str, value_chf: float, invested_chf: float
) -> dict[str, object]:
    """Return one portfolio-history row."""
    return {"date": date, "value_chf": value_chf, "invested_chf": invested_chf}


def _make_portfolio_df() -> pd.DataFrame:
    """Return three monthly portfolio-history rows."""
    return pd.DataFrame(
        [
            _make_portfolio_row("2024-01-01", 10000.0, 9000.0),
            _make_portfolio_row("2024-02-01", 10500.0, 9500.0),
            _make_portfolio_row("2024-03-01", 11000.0, 10000.0),
        ]
    )


def _make_position_row(
    symbol: str,
    date: str,
    value_chf: float,
    pnl_pct: float = 0.05,
) -> dict[str, object]:
    """Return one position time-series row."""
    return {
        "symbol": symbol,
        "date": date,
        "value_chf": value_chf,
        "pnl_pct": pnl_pct,
        "name": "Vanguard FTSE All-World",
    }


def _make_position_df(symbol: str = "VWRL") -> pd.DataFrame:
    """Return three monthly rows for the given symbol."""
    return pd.DataFrame(
        [
            _make_position_row(symbol, "2024-01-01", 10000.0),
            _make_position_row(symbol, "2024-02-01", 10500.0),
            _make_position_row(symbol, "2024-03-01", 11000.0),
        ]
    )


# ── fig_allocation_donut ──────────────────────────────────────────────────────


def test_fig_allocation_donut_returns_single_trace() -> None:
    df = _make_allocation_df()
    fig = fig_allocation_donut(df)
    assert isinstance(fig, go.Figure)
    assert len(tuple(fig.data)) == 1


def test_fig_allocation_donut_hole_value() -> None:
    df = _make_allocation_df()
    fig = fig_allocation_donut(df)
    assert cast("go.Pie", fig.data[0]).hole == pytest.approx(0.4)


def test_fig_allocation_donut_empty_returns_figure() -> None:
    df = pd.DataFrame(columns=["symbol", "value_chf", "name"])
    fig = fig_allocation_donut(df)
    assert isinstance(fig, go.Figure)


# ── fig_portfolio_progression ─────────────────────────────────────────────────


def test_fig_portfolio_progression_returns_figure() -> None:
    df = _make_portfolio_df()
    fig = fig_portfolio_progression(df)
    assert isinstance(fig, go.Figure)
    assert fig.data


def test_fig_portfolio_progression_has_two_traces() -> None:
    """Invested baseline and portfolio-value traces must both be present."""
    df = _make_portfolio_df()
    fig = fig_portfolio_progression(df)
    assert len(tuple(fig.data)) == 2


def test_fig_portfolio_progression_empty_returns_figure() -> None:
    df = pd.DataFrame(columns=["date", "value_chf", "invested_chf"])
    fig = fig_portfolio_progression(df)
    assert isinstance(fig, go.Figure)


# ── fig_position_value ────────────────────────────────────────────────────────


def test_fig_position_value_returns_figure() -> None:
    df = _make_position_df()
    fig = fig_position_value(df, symbols=["VWRL"])
    assert isinstance(fig, go.Figure)
    assert fig.data


def test_fig_position_value_one_trace_per_symbol() -> None:
    df = _make_position_df()
    fig = fig_position_value(df, symbols=["VWRL"])
    assert len(tuple(fig.data)) == 1


def test_fig_position_value_empty_df_returns_figure() -> None:
    df = pd.DataFrame(columns=["symbol", "date", "value_chf", "pnl_pct", "name"])
    fig = fig_position_value(df, symbols=["VWRL"])
    assert isinstance(fig, go.Figure)


def test_fig_position_value_empty_symbols_returns_figure() -> None:
    df = _make_position_df()
    fig = fig_position_value(df, symbols=[])
    assert isinstance(fig, go.Figure)


# ── fig_position_pct ──────────────────────────────────────────────────────────


def test_fig_position_pct_returns_figure() -> None:
    df = _make_position_df()
    fig = fig_position_pct(df, symbols=["VWRL"])
    assert isinstance(fig, go.Figure)
    assert fig.data


def test_fig_position_pct_one_trace_per_symbol() -> None:
    df = _make_position_df()
    fig = fig_position_pct(df, symbols=["VWRL"])
    # one scatter trace; hline adds a shape, not a data trace
    assert len(tuple(fig.data)) == 1


def test_fig_position_pct_empty_df_returns_figure() -> None:
    df = pd.DataFrame(columns=["symbol", "date", "value_chf", "pnl_pct", "name"])
    fig = fig_position_pct(df, symbols=["VWRL"])
    assert isinstance(fig, go.Figure)


def test_fig_position_pct_empty_symbols_returns_figure() -> None:
    df = _make_position_df()
    fig = fig_position_pct(df, symbols=[])
    assert isinstance(fig, go.Figure)
