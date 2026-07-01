from __future__ import annotations

import calendar

from typing import Literal

import numpy as np
import numpy.typing as npt
import pandas as pd

from swiss_exp_tracker.app.data.budget_models import CategoryBudget


def _build_freq_grid(freq: Literal["D", "W", "M"]) -> npt.NDArray[np.float64]:
    """Return the canonical year_fraction grid (1-indexed) for the given frequency."""
    if freq == "M":
        return np.array([m / 12.0 for m in range(1, 13)])
    if freq == "W":
        return np.array([w / 52.0 for w in range(1, 53)])
    return np.array([d / 365.0 for d in range(1, 366)])


def seasonal_pacing_curve(
    history: pd.DataFrame,
    category: str,
    freq: Literal["D", "W", "M"],
) -> pd.DataFrame:
    """Return (year_fraction, cum_share) pacing curve at freq resolution.
    Averages prior years' normalized cumulative spend; falls back to linear when no usable history.
    Applies half-shrink toward linear for exactly one prior year.
    """
    grid = _build_freq_grid(freq)
    linear: npt.NDArray[np.float64] = grid.copy()

    cat_hist = history[history["category"] == category]
    years = sorted(cat_hist["year"].unique())

    year_curves: list[npt.NDArray[np.float64]] = []
    for yr in years:
        yr_data = cat_hist[cat_hist["year"] == yr].sort_values("year_fraction")
        total = float(yr_data["spend_chf"].sum())
        if total == 0.0:
            continue
        cum: npt.NDArray[np.float64] = (
            yr_data["spend_chf"].cumsum().to_numpy(dtype=float)
        )
        cum_share: npt.NDArray[np.float64] = cum / total
        yr_fracs: npt.NDArray[np.float64] = yr_data["year_fraction"].to_numpy(
            dtype=float
        )
        # Anchor at (0, 0) so interpolation starts at the origin
        xp: npt.NDArray[np.float64] = np.asarray(
            np.concatenate([[0.0], yr_fracs]), dtype=np.float64
        )
        fp: npt.NDArray[np.float64] = np.asarray(
            np.concatenate([[0.0], cum_share]), dtype=np.float64
        )
        interpolated: npt.NDArray[np.float64] = np.asarray(
            np.interp(grid, xp, fp), dtype=np.float64
        )
        year_curves.append(interpolated)

    if not year_curves:
        return pd.DataFrame({"year_fraction": grid, "cum_share": linear})

    stacked: npt.NDArray[np.float64] = np.asarray(
        np.array(year_curves), dtype=np.float64
    )
    learned: npt.NDArray[np.float64] = np.asarray(
        stacked.mean(axis=0), dtype=np.float64
    )
    learned = np.asarray(np.maximum.accumulate(learned), dtype=np.float64)
    learned[-1] = 1.0

    if len(year_curves) == 1:
        blended: npt.NDArray[np.float64] = 0.5 * learned + 0.5 * linear
        blended = np.asarray(np.maximum.accumulate(blended), dtype=np.float64)
        blended[-1] = 1.0
        return pd.DataFrame({"year_fraction": grid, "cum_share": blended})

    return pd.DataFrame({"year_fraction": grid, "cum_share": learned})


def pacing_fraction(curve: pd.DataFrame, as_of: pd.Timestamp) -> float:
    """Interpolate the pacing curve at as_of's day-of-year position; returns value in (0, 1].
    Floors at 1/365 to prevent a zero denominator at the very start of the year.
    """
    days_in_year = 366.0 if calendar.isleap(as_of.year) else 365.0
    day_of_year = float(as_of.timetuple().tm_yday)
    fraction = min(max(day_of_year / days_in_year, 1.0 / 365.0), 1.0)
    xp: npt.NDArray[np.float64] = curve["year_fraction"].to_numpy(dtype=float)
    fp: npt.NDArray[np.float64] = curve["cum_share"].to_numpy(dtype=float)
    return max(float(np.interp(fraction, xp, fp)), 1e-9)


def forecast_year_end(
    spend_to_date: float,
    curve: pd.DataFrame,
    as_of: pd.Timestamp,
) -> float:
    """Return spend_to_date / pacing_fraction(curve, as_of) as the seasonal year-end forecast.
    Returns spend_to_date on Jan 1 to avoid a meaningless single-day projection.
    """
    if as_of.timetuple().tm_yday <= 1:
        return spend_to_date
    return spend_to_date / pacing_fraction(curve, as_of)


def build_forecast_line(
    cumulative: pd.DataFrame,
    curve: pd.DataFrame,
    year: int,
    as_of: pd.Timestamp,
    freq: Literal["D", "W", "M"],
) -> pd.DataFrame:
    """Return actual and forecast cumulative series joined at as_of for a continuous line.
    Output columns: period_end (Timestamp), cumulative_chf (float), segment ("actual"/"forecast").
    """
    actual = cumulative[cumulative["period_end"] <= as_of][
        ["period_end", "cumulative_chf"]
    ].copy()
    actual["segment"] = "actual"

    spend_to_date = (
        float(actual["cumulative_chf"].iloc[-1]) if not actual.empty else 0.0
    )
    year_end_fc = forecast_year_end(spend_to_date, curve, as_of)

    freq_alias: dict[str, str] = {"D": "D", "W": "W", "M": "ME"}
    all_periods = pd.date_range(
        start=f"{year}-01-01",
        end=f"{year}-12-31",
        freq=freq_alias[freq],
    )
    future_periods = all_periods[all_periods > as_of]

    # Shared boundary point makes actual and forecast traces connect seamlessly
    boundary = pd.DataFrame(
        {
            "period_end": [as_of],
            "cumulative_chf": [spend_to_date],
            "segment": ["forecast"],
        }
    )

    days_in_year = 366.0 if calendar.isleap(year) else 365.0
    xp: npt.NDArray[np.float64] = curve["year_fraction"].to_numpy(dtype=float)
    fp: npt.NDArray[np.float64] = curve["cum_share"].to_numpy(dtype=float)
    fc_fracs: npt.NDArray[np.float64] = np.array(
        [float(p.timetuple().tm_yday) / days_in_year for p in future_periods],
        dtype=np.float64,
    )
    fc_cum: npt.NDArray[np.float64] = np.asarray(
        year_end_fc * np.interp(fc_fracs, xp, fp), dtype=np.float64
    )

    forecast_rows = pd.DataFrame(
        {
            "period_end": future_periods,
            "cumulative_chf": fc_cum,
            "segment": "forecast",
        }
    )

    return pd.concat([actual, boundary, forecast_rows], ignore_index=True)


def build_budget_table(
    spend: pd.DataFrame,
    budgets: list[CategoryBudget],
    curves: dict[str, pd.DataFrame],
    year: int,
    as_of: pd.Timestamp,
) -> pd.DataFrame:
    """Return one row per budgeted category with spend, forecast, and over/under columns.
    Uses year to build a leap-year-aware linear fallback for categories absent from curves.
    """
    n_days = 366 if calendar.isleap(year) else 365
    _grid: npt.NDArray[np.float64] = np.array(
        [d / float(n_days) for d in range(1, n_days + 1)]
    )
    linear_fallback = pd.DataFrame({"year_fraction": _grid, "cum_share": _grid.copy()})

    spend_lookup: dict[str, float] = {
        str(k): float(v)
        for k, v in zip(spend["category"], spend["spend_chf"], strict=False)
    }

    rows: list[dict[str, object]] = []
    for b in budgets:
        cat = b.category
        budget_chf = b.budget_chf
        spend_chf = spend_lookup.get(cat, 0.0)
        curve = curves.get(cat, linear_fallback)

        pacing_frac = pacing_fraction(curve, as_of)
        forecast_chf = forecast_year_end(spend_chf, curve, as_of)
        pace_budget = budget_chf * pacing_frac
        over_under_now_chf = spend_chf - pace_budget
        over_under_eoy_chf = forecast_chf - budget_chf
        over_under_now_pct = (
            0.0 if budget_chf == 0.0 else over_under_now_chf / budget_chf
        )
        over_under_eoy_pct = (
            0.0 if budget_chf == 0.0 else over_under_eoy_chf / budget_chf
        )

        rows.append(
            {
                "category": cat,
                "spend_chf": spend_chf,
                "budget_chf": budget_chf,
                "forecast_chf": forecast_chf,
                "over_under_now_chf": over_under_now_chf,
                "over_under_now_pct": over_under_now_pct,
                "over_under_eoy_chf": over_under_eoy_chf,
                "over_under_eoy_pct": over_under_eoy_pct,
            }
        )

    return pd.DataFrame(rows)
