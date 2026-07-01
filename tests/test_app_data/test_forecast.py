"""Tests for pure forecast functions in swiss_exp_tracker.app.data.forecast.

No DB or fixtures required — all helpers are exercised with synthetic DataFrames.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from swiss_exp_tracker.app.data.budget_models import CategoryBudget
from swiss_exp_tracker.app.data.forecast import build_budget_table
from swiss_exp_tracker.app.data.forecast import build_forecast_line
from swiss_exp_tracker.app.data.forecast import forecast_year_end
from swiss_exp_tracker.app.data.forecast import pacing_fraction
from swiss_exp_tracker.app.data.forecast import seasonal_pacing_curve

# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _make_linear_history(
    category: str,
    years: list[int],
    freq: str,
    spend_per_bucket: float = 100.0,
) -> pd.DataFrame:
    """Return uniform-spend history spanning the given years at freq granularity."""
    n_buckets = {"M": 12, "W": 52, "D": 365}[freq]
    fracs = [b / n_buckets for b in range(1, n_buckets + 1)]
    rows = [
        {
            "category": category,
            "year": yr,
            "year_fraction": f,
            "spend_chf": spend_per_bucket,
        }
        for yr in years
        for f in fracs
    ]
    return pd.DataFrame(rows)


def _make_front_loaded_history(
    category: str,
    years: list[int],
    freq: str,
    front_months: int = 6,
) -> pd.DataFrame:
    """Return history where all spending falls in the first front_months months."""
    n_buckets = {"M": 12, "W": 52, "D": 365}[freq]
    front_fraction = front_months / 12.0
    rows = []
    for yr in years:
        for b in range(1, n_buckets + 1):
            frac = b / n_buckets
            rows.append(
                {
                    "category": category,
                    "year": yr,
                    "year_fraction": frac,
                    "spend_chf": 200.0 if frac <= front_fraction else 0.0,
                }
            )
    return pd.DataFrame(rows)


def _make_linear_curve(freq: str) -> pd.DataFrame:
    """Return a linear pacing curve (cum_share == year_fraction) at freq resolution."""
    n_buckets = {"M": 12, "W": 52, "D": 365}[freq]
    fracs = [b / n_buckets for b in range(1, n_buckets + 1)]
    return pd.DataFrame({"year_fraction": fracs, "cum_share": fracs})


def _make_cumulative_df(
    periods: list[str],
    amounts: list[float],
) -> pd.DataFrame:
    """Return a cumulative spend DataFrame with period_end (Timestamps) and cumulative_chf."""
    return pd.DataFrame(
        {
            "period_end": pd.to_datetime(periods),
            "cumulative_chf": amounts,
        }
    )


def _make_spend(category: str, amount: float) -> pd.DataFrame:
    """Return a minimal spend DataFrame suitable for build_budget_table."""
    return pd.DataFrame({"category": [category], "spend_chf": [amount]})


# ---------------------------------------------------------------------------
# seasonal_pacing_curve — grid size
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "freq,expected_len",
    [("M", 12), ("W", 52), ("D", 365)],
)
def test_seasonal_pacing_curve_sample_count_tracks_freq(
    freq: str, expected_len: int
) -> None:
    """Output row count equals the canonical grid size for the given frequency."""
    empty = pd.DataFrame(columns=["category", "year", "year_fraction", "spend_chf"])
    curve = seasonal_pacing_curve(empty, "Groceries", freq)
    assert len(curve) == expected_len


# ---------------------------------------------------------------------------
# seasonal_pacing_curve — no history → linear
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("freq", ["D", "W", "M"])
def test_seasonal_pacing_curve_no_history_is_linear(freq: str) -> None:
    """With no prior history the curve is exactly the linear grid (cum_share == year_fraction)."""
    empty = pd.DataFrame(columns=["category", "year", "year_fraction", "spend_chf"])
    curve = seasonal_pacing_curve(empty, "Groceries", freq)
    assert list(curve.columns) == ["year_fraction", "cum_share"]
    np.testing.assert_allclose(
        curve["cum_share"].to_numpy(),
        curve["year_fraction"].to_numpy(),
        rtol=1e-9,
    )


# ---------------------------------------------------------------------------
# seasonal_pacing_curve — monotone and ends at 1.0
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("freq", ["D", "W", "M"])
def test_seasonal_pacing_curve_monotone_non_decreasing(freq: str) -> None:
    """cum_share is monotonically non-decreasing across the full curve."""
    history = _make_linear_history("Groceries", [2022, 2023], freq)
    curve = seasonal_pacing_curve(history, "Groceries", freq)
    diffs = np.diff(curve["cum_share"].to_numpy())
    assert (diffs >= -1e-9).all()


@pytest.mark.parametrize("freq", ["D", "W", "M"])
def test_seasonal_pacing_curve_ends_at_one(freq: str) -> None:
    """The last cum_share value is exactly 1.0."""
    history = _make_linear_history("Groceries", [2022, 2023], freq)
    curve = seasonal_pacing_curve(history, "Groceries", freq)
    assert curve["cum_share"].iloc[-1] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# seasonal_pacing_curve — one prior year → half-shrink toward linear
# ---------------------------------------------------------------------------


def test_seasonal_pacing_curve_one_prior_year_blended_above_linear() -> None:
    """With exactly one prior front-loaded year, blended cum_share > linear at mid-year."""
    # Front-loaded: all spending in first 6 months → learned cum_share = 1.0 at month 6.
    # Half-shrink: blended = 0.5*1.0 + 0.5*0.5 = 0.75 > linear 0.5.
    history = _make_front_loaded_history("Groceries", [2022], "M", front_months=6)
    curve = seasonal_pacing_curve(history, "Groceries", "M")
    linear_curve = _make_linear_curve("M")

    half_idx = 5  # 0-based index of month 6 (year_fraction = 6/12)
    curve_at_half = float(curve["cum_share"].iloc[half_idx])
    linear_at_half = float(linear_curve["cum_share"].iloc[half_idx])

    assert curve_at_half > linear_at_half


# ---------------------------------------------------------------------------
# seasonal_pacing_curve — front-loaded history rises faster early
# ---------------------------------------------------------------------------


def test_seasonal_pacing_curve_front_loaded_rises_faster_early() -> None:
    """Two years of front-loaded history raise cum_share well above linear at mid-year."""
    # Two years → no half-shrink: learned curve is used directly.
    history = _make_front_loaded_history("Groceries", [2022, 2023], "M", front_months=6)
    curve = seasonal_pacing_curve(history, "Groceries", "M")
    linear_curve = _make_linear_curve("M")

    half_idx = 5  # month 6 (year_fraction = 0.5)
    assert float(curve["cum_share"].iloc[half_idx]) > float(
        linear_curve["cum_share"].iloc[half_idx]
    )


# ---------------------------------------------------------------------------
# pacing_fraction
# ---------------------------------------------------------------------------


def test_pacing_fraction_in_valid_range() -> None:
    """pacing_fraction is in (0, 1] for every month of the year."""
    curve = _make_linear_curve("M")
    for month in range(1, 13):
        ts = pd.Timestamp(f"2024-{month:02d}-15")
        f = pacing_fraction(curve, ts)
        assert 0 < f <= 1.0 + 1e-9


def test_pacing_fraction_non_zero_at_jan1() -> None:
    """pacing_fraction is strictly positive on Jan 1 (floor prevents a zero denominator)."""
    curve = _make_linear_curve("M")
    f = pacing_fraction(curve, pd.Timestamp("2024-01-01"))
    assert f > 0


def test_pacing_fraction_increases_with_as_of() -> None:
    """pacing_fraction is non-decreasing as as_of advances through the year."""
    curve = _make_linear_curve("M")
    timestamps = [pd.Timestamp(f"2024-{m:02d}-01") for m in range(1, 13)]
    fracs = [pacing_fraction(curve, ts) for ts in timestamps]
    diffs = [fracs[i + 1] - fracs[i] for i in range(len(fracs) - 1)]
    assert all(d >= -1e-9 for d in diffs)


def test_pacing_fraction_at_year_end_close_to_one() -> None:
    """pacing_fraction is 1.0 (or very close) at Dec 31."""
    curve = _make_linear_curve("M")
    f = pacing_fraction(curve, pd.Timestamp("2024-12-31"))
    assert f == pytest.approx(1.0, abs=1e-6)


# ---------------------------------------------------------------------------
# forecast_year_end
# ---------------------------------------------------------------------------


def test_forecast_year_end_returns_spend_on_jan1() -> None:
    """forecast_year_end returns spend_to_date unchanged when as_of is Jan 1 (day <= 1)."""
    curve = _make_linear_curve("M")
    spend = 500.0
    result = forecast_year_end(spend, curve, pd.Timestamp("2024-01-01"))
    assert result == pytest.approx(spend)


def test_forecast_year_end_scales_up_from_mid_year() -> None:
    """At mid-year the forecast is greater than the current spend."""
    curve = _make_linear_curve("M")
    spend = 600.0
    result = forecast_year_end(spend, curve, pd.Timestamp("2024-07-01"))
    assert result > spend


def test_forecast_year_end_front_loaded_lower_than_flat() -> None:
    """Front-loaded curve projects a LOWER year-end than flat run-rate for the same spend."""
    # Two prior years → no half-shrink; learned curve has cum_share ≈ 1.0 by month 6.
    history = _make_front_loaded_history("Groceries", [2022, 2023], "M", front_months=6)
    curve_front = seasonal_pacing_curve(history, "Groceries", "M")
    curve_flat = _make_linear_curve("M")

    spend = 600.0
    # as_of = start of H2; front-loaded pacing_fraction ≈ 1.0, flat ≈ 0.5
    as_of = pd.Timestamp("2024-07-01")

    fc_front = forecast_year_end(spend, curve_front, as_of)
    fc_flat = forecast_year_end(spend, curve_flat, as_of)

    assert fc_front < fc_flat


# ---------------------------------------------------------------------------
# build_forecast_line
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("freq", ["D", "W", "M"])
def test_build_forecast_line_output_columns(freq: str) -> None:
    """Output DataFrame has exactly [period_end, cumulative_chf, segment] columns."""
    curve = _make_linear_curve(freq)
    cumulative = _make_cumulative_df(
        ["2024-01-01", "2024-02-01", "2024-03-01"],
        [200.0, 400.0, 600.0],
    )
    as_of = pd.Timestamp("2024-06-15")
    result = build_forecast_line(cumulative, curve, 2024, as_of, freq)
    assert set(result.columns) == {"period_end", "cumulative_chf", "segment"}


@pytest.mark.parametrize("freq", ["D", "W", "M"])
def test_build_forecast_line_segment_values_valid(freq: str) -> None:
    """Output 'segment' column contains only 'actual' and 'forecast'."""
    curve = _make_linear_curve(freq)
    cumulative = _make_cumulative_df(
        ["2024-01-01", "2024-02-01", "2024-03-01"],
        [200.0, 400.0, 600.0],
    )
    as_of = pd.Timestamp("2024-06-15")
    result = build_forecast_line(cumulative, curve, 2024, as_of, freq)
    assert set(result["segment"].unique()).issubset({"actual", "forecast"})


@pytest.mark.parametrize("freq", ["D", "W", "M"])
def test_build_forecast_line_actual_rows_at_or_before_as_of(freq: str) -> None:
    """All 'actual' rows have period_end <= as_of."""
    curve = _make_linear_curve(freq)
    cumulative = _make_cumulative_df(
        ["2024-01-01", "2024-02-01", "2024-03-01"],
        [200.0, 400.0, 600.0],
    )
    as_of = pd.Timestamp("2024-06-15")
    result = build_forecast_line(cumulative, curve, 2024, as_of, freq)
    actual_rows = result[result["segment"] == "actual"]
    assert not actual_rows.empty
    assert (actual_rows["period_end"] <= as_of).all()


@pytest.mark.parametrize("freq", ["D", "W", "M"])
def test_build_forecast_line_forecast_rows_at_or_after_as_of(freq: str) -> None:
    """All 'forecast' rows (including boundary) have period_end >= as_of."""
    curve = _make_linear_curve(freq)
    cumulative = _make_cumulative_df(
        ["2024-01-01", "2024-02-01", "2024-03-01"],
        [200.0, 400.0, 600.0],
    )
    as_of = pd.Timestamp("2024-06-15")
    result = build_forecast_line(cumulative, curve, 2024, as_of, freq)
    forecast_rows = result[result["segment"] == "forecast"]
    assert not forecast_rows.empty
    assert (forecast_rows["period_end"] >= as_of).all()


@pytest.mark.parametrize("freq", ["D", "W", "M"])
def test_build_forecast_line_boundary_point_joins_segments(freq: str) -> None:
    """The boundary point at as_of appears in the forecast segment for trace continuity."""
    curve = _make_linear_curve(freq)
    cumulative = _make_cumulative_df(
        ["2024-01-01", "2024-02-01", "2024-03-01"],
        [200.0, 400.0, 600.0],
    )
    as_of = pd.Timestamp("2024-06-15")
    result = build_forecast_line(cumulative, curve, 2024, as_of, freq)
    boundary = result[
        (result["segment"] == "forecast") & (result["period_end"] == as_of)
    ]
    assert len(boundary) == 1


# ---------------------------------------------------------------------------
# build_budget_table
# ---------------------------------------------------------------------------


def test_build_budget_table_output_columns() -> None:
    """Output DataFrame has exactly the eight expected columns."""
    curve = _make_linear_curve("M")
    spend = _make_spend("Groceries", 300.0)
    budgets = [CategoryBudget(category="Groceries", budget_chf=500.0)]
    as_of = pd.Timestamp("2024-06-15")
    table = build_budget_table(spend, budgets, {"Groceries": curve}, 2024, as_of)
    expected = {
        "category",
        "spend_chf",
        "budget_chf",
        "forecast_chf",
        "over_under_now_chf",
        "over_under_now_pct",
        "over_under_eoy_chf",
        "over_under_eoy_pct",
    }
    assert set(table.columns) == expected


def test_build_budget_table_over_budget_positive_over_under_eoy() -> None:
    """When forecasted spend exceeds budget, over_under_eoy_chf is positive."""
    curve = _make_linear_curve("M")
    # 800 CHF to date at mid-year on a 500 CHF budget → will exceed year-end
    spend = _make_spend("Groceries", 800.0)
    budgets = [CategoryBudget(category="Groceries", budget_chf=500.0)]
    as_of = pd.Timestamp("2024-06-15")
    table = build_budget_table(spend, budgets, {"Groceries": curve}, 2024, as_of)
    assert not table.empty
    assert float(table.iloc[0]["over_under_eoy_chf"]) > 0


def test_build_budget_table_zero_budget_no_zero_division() -> None:
    """budget_chf=0 sets both pct columns to 0.0 without raising ZeroDivisionError."""
    curve = _make_linear_curve("M")
    spend = _make_spend("Groceries", 300.0)
    budgets = [CategoryBudget(category="Groceries", budget_chf=0.0)]
    as_of = pd.Timestamp("2024-06-15")
    table = build_budget_table(spend, budgets, {"Groceries": curve}, 2024, as_of)
    assert table.iloc[0]["over_under_now_pct"] == pytest.approx(0.0)
    assert table.iloc[0]["over_under_eoy_pct"] == pytest.approx(0.0)


def test_build_budget_table_over_under_eoy_equals_forecast_minus_budget() -> None:
    """over_under_eoy_chf == forecast_chf - budget_chf for every row."""
    curve = _make_linear_curve("M")
    spend = _make_spend("Groceries", 600.0)
    budgets = [CategoryBudget(category="Groceries", budget_chf=800.0)]
    as_of = pd.Timestamp("2024-06-15")
    table = build_budget_table(spend, budgets, {"Groceries": curve}, 2024, as_of)
    row = table.iloc[0]
    assert float(row["over_under_eoy_chf"]) == pytest.approx(
        float(row["forecast_chf"]) - 800.0
    )


def test_build_budget_table_no_spend_category_defaults_to_zero() -> None:
    """A category absent from the spend DataFrame defaults to 0.0 spend."""
    curve = _make_linear_curve("M")
    spend = pd.DataFrame({"category": ["Housing"], "spend_chf": [1000.0]})
    budgets = [CategoryBudget(category="Groceries", budget_chf=500.0)]
    as_of = pd.Timestamp("2024-06-15")
    table = build_budget_table(spend, budgets, {"Groceries": curve}, 2024, as_of)
    assert table.iloc[0]["spend_chf"] == pytest.approx(0.0)


def test_build_budget_table_multiple_categories_one_row_each() -> None:
    """Output has exactly one row per budget entry when multiple categories are present."""
    curve = _make_linear_curve("M")
    spend = pd.DataFrame(
        {"category": ["Groceries", "Housing"], "spend_chf": [600.0, 1200.0]}
    )
    budgets = [
        CategoryBudget(category="Groceries", budget_chf=500.0),
        CategoryBudget(category="Housing", budget_chf=1800.0),
    ]
    as_of = pd.Timestamp("2024-06-15")
    table = build_budget_table(
        spend,
        budgets,
        {"Groceries": curve, "Housing": curve},
        2024,
        as_of,
    )
    assert len(table) == 2
