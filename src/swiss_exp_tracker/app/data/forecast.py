from __future__ import annotations

import calendar
import hashlib

from typing import Literal

import numpy as np
import numpy.typing as npt
import pandas as pd

from swiss_exp_tracker.app.data.budget_models import CategoryBudget

# Fraction the multi-year pacing curve is pulled toward the flat/linear curve. A light
# pull bounds the run-rate amplification from a sparse or lopsided history without erasing
# genuine seasonality; a single prior year is regularized harder (0.5, see below).
# 0.35: with only 2-4 noisy years of personal data, hedge the shape ~a third toward flat.
_CURVE_LINEAR_REG = 0.35
# Years whose total is below this fraction of the median year total are ignored when
# learning the seasonal shape (their within-year timing is noise, not seasonality).
_CURVE_MIN_YEAR_FRAC = 0.2


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
    Drops negligible years (total < _CURVE_MIN_YEAR_FRAC of the median year) so tiny early
    years don't distort the shape, then takes the median across the remaining years' normalized
    cumulative curves so a single one-time-purchase year (e.g. a car bought in one month) can't
    dominate. Falls back to linear when no usable history; regularizes toward linear (fully for
    one year, lightly otherwise) to bound run-rate blow-up from a sparse or lopsided history.
    """
    grid = _build_freq_grid(freq)
    linear: npt.NDArray[np.float64] = grid.copy()

    cat_hist = history[history["category"] == category]
    years = sorted(cat_hist["year"].unique())

    year_curves: list[npt.NDArray[np.float64]] = []
    year_totals: list[float] = []
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
        year_totals.append(total)

    if not year_curves:
        return pd.DataFrame({"year_fraction": grid, "cum_share": linear})

    # Drop negligible years (their timing is noise); keep all if that would empty the set.
    totals_arr: npt.NDArray[np.float64] = np.asarray(year_totals, dtype=np.float64)
    threshold = _CURVE_MIN_YEAR_FRAC * float(np.median(totals_arr))
    kept = [c for c, t in zip(year_curves, year_totals, strict=False) if t >= threshold]
    if not kept:
        kept = year_curves

    stacked: npt.NDArray[np.float64] = np.asarray(np.array(kept), dtype=np.float64)
    # Median across the kept years: robust to a lone spike year (a one-time purchase
    # concentrated in one month) that would otherwise skew the seasonal shape.
    learned: npt.NDArray[np.float64] = np.asarray(
        np.median(stacked, axis=0), dtype=np.float64
    )
    learned = np.asarray(np.maximum.accumulate(learned), dtype=np.float64)

    # Regularize toward the linear (flat) curve: fully for a single year, lightly for
    # more. This caps the run-rate blow-up when history is sparse or oddly front/back-loaded.
    lin_weight = 0.5 if len(kept) == 1 else _CURVE_LINEAR_REG
    blended: npt.NDArray[np.float64] = (
        1.0 - lin_weight
    ) * learned + lin_weight * linear
    blended = np.asarray(np.maximum.accumulate(blended), dtype=np.float64)
    blended[-1] = 1.0
    return pd.DataFrame({"year_fraction": grid, "cum_share": blended})


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


# Library fallback for the shrinkage constant; the app passes config.forecast_shrinkage_k.
# With time-based weighting, k=0.1 gives ~80% weight to current-year pace by June.
DEFAULT_SHRINKAGE_K = 0.1
# Number of most-recent prior years averaged into the history level anchor.
_LEVEL_RECENT_YEARS = 2
# A category is "lumpy" (few big transactions rather than a steady stream) when its mean
# monthly spend exceeds this multiple of its median monthly spend — i.e. a handful of big
# months pull the mean well above the typical month. Lumpy categories forecast the remaining
# year from the spike-resistant median rate instead of extrapolating the run-rate.
_LUMPY_MEAN_MEDIAN_RATIO = 1.3


def is_lumpy_category(monthly_totals: list[float]) -> bool:
    """Return True if spend is concentrated in a few big months (mean >> median) or mostly empty.
    Continuous categories (restaurants, groceries) have mean ~= median and return False.
    """
    arr = np.asarray([t for t in monthly_totals if t >= 0.0], dtype=np.float64)
    if arr.size == 0:
        return False
    median = float(np.median(arr))
    if median <= 0.0:  # spend sits in a minority of months → lumpy by definition
        return True
    return float(arr.mean()) > _LUMPY_MEAN_MEDIAN_RATIO * median


def robust_monthly_rate(monthly_totals: list[float]) -> float:
    """Return the median monthly spend — the typical month's rate, robust to occasional big months."""
    arr = np.asarray([t for t in monthly_totals if t >= 0.0], dtype=np.float64)
    if arr.size == 0:
        return 0.0
    return float(np.median(arr))


def robust_monthly_deviation(monthly_totals: list[float]) -> float:
    """Return the median absolute deviation of monthly spend, scaled to std-equivalent units.
    Sizes the forecast jitter for lumpy categories (see _jitter_monthly_path); uses the same
    outlier-resistant statistic as robust_monthly_rate instead of a plain std, which a single
    big month would otherwise inflate.
    """
    arr = np.asarray([t for t in monthly_totals if t >= 0.0], dtype=np.float64)
    if arr.size == 0:
        return 0.0
    median = float(np.median(arr))
    mad = float(np.median(np.abs(arr - median)))
    return mad * 1.4826


# A single month over this multiple of the category's typical (median) month is treated as a
# one-off, not the new normal. Only matters for non-lumpy categories: a lumpy category already
# forecasts off the median rate, but a continuous category (e.g. Retail) can still take one big
# purchase in a month, and forecast_year_end's pace divides by the seasonal pacing_fraction —
# early in the year that fraction is small, so an un-flattened one-off gets extrapolated as if
# it recurred every remaining period and blows the forecast up.
_CONTINUOUS_MONTH_SPIKE_CAP_MULTIPLE = 2.0


def spike_excess_this_year(this_year_months: list[float], median_rate: float) -> float:
    """Return how much this year's completed months exceed cap_multiple x median_rate, summed.
    forecast_year_end subtracts this from spend_to_date before computing the pace (so a one-off
    month isn't extrapolated as a recurring rate) and adds it back once afterward (the money was
    still genuinely spent). Zero when median_rate is 0 (handled instead by is_lumpy_category).
    """
    if median_rate <= 0.0:
        return 0.0
    cap = _CONTINUOUS_MONTH_SPIKE_CAP_MULTIPLE * median_rate
    return float(sum(max(0.0, m - cap) for m in this_year_months))


def _deterministic_seed(key: str) -> int:
    """Return a stable 32-bit seed derived from key, so jitter is reproducible across reloads."""
    return int(hashlib.blake2b(key.encode(), digest_size=4).hexdigest(), 16)


def _jitter_monthly_path(
    spend_to_date: float,
    monthly_rate: float,
    monthly_deviation: float,
    months_ahead: npt.NDArray[np.float64],
    seed_key: str,
) -> npt.NDArray[np.float64]:
    """Return a cumulative path that wobbles like real spending instead of a straight ramp.
    Per-period increments get lognormal noise sized by monthly_deviation relative to
    monthly_rate, then are rescaled so the path still lands exactly on the monthly_rate-based
    year-end total (matches the point estimate used in the budget table). Increments stay
    positive so cumulative spend never dips; noise is seeded from seed_key so the wobble is
    stable across reloads of the same category/year instead of reshuffling every refresh.
    """
    step_months = np.diff(np.concatenate([[0.0], months_ahead]))
    baseline_incr: npt.NDArray[np.float64] = monthly_rate * step_months
    total_incr = float(baseline_incr.sum())
    if total_incr <= 0.0 or monthly_rate <= 0.0:
        return np.asarray(spend_to_date + np.cumsum(baseline_incr), dtype=np.float64)

    # Coefficient of variation in log-space, capped so a noisy/near-zero rate can't produce
    # wild swings that dwarf the underlying signal.
    cv = min(monthly_deviation / monthly_rate, 1.5)
    rng = np.random.default_rng(_deterministic_seed(seed_key))
    noise_factors = np.exp(rng.normal(0.0, cv, size=step_months.size))
    jittered = baseline_incr * noise_factors
    jittered *= total_incr / float(jittered.sum())
    return np.asarray(spend_to_date + np.cumsum(jittered), dtype=np.float64)


def _months_remaining(as_of: pd.Timestamp) -> float:
    """Return the fraction of the year still ahead of as_of, expressed in months (0-12)."""
    days_in_year = 366.0 if calendar.isleap(as_of.year) else 365.0
    return float(12.0 * (1.0 - as_of.timetuple().tm_yday / days_in_year))


def forecast_year_end_lumpy(
    spend_to_date: float,
    monthly_rate: float,
    as_of: pd.Timestamp,
) -> float:
    """Return spend-to-date plus the robust monthly rate over the months remaining.
    Big purchases already made stay counted (they are in spend_to_date), but the rest of the
    year is projected at the typical rate — so a one-off does not inflate the whole forecast.
    """
    return float(spend_to_date) + monthly_rate * _months_remaining(as_of)


def historical_annual_level(
    history: pd.DataFrame,
    category: str,
) -> float | None:
    """Return the expected annual total for category from prior years, or None if no history.
    Averages the most recent _LEVEL_RECENT_YEARS years so the anchor tracks the current spending
    level instead of lagging on a long, growing history; ignores zero-spend years.
    """
    totals = (
        history[history["category"] == category]
        .groupby("year")["spend_chf"]
        .sum()
        .sort_index()
    )
    totals = totals[totals > 0.0]
    if totals.empty:
        return None
    return float(totals.iloc[-_LEVEL_RECENT_YEARS:].mean())


def forecast_year_end(
    spend_to_date: float,
    curve: pd.DataFrame,
    as_of: pd.Timestamp,
    prior_level: float | None = None,
    k: float = DEFAULT_SHRINKAGE_K,
    spike_excess: float = 0.0,
) -> float:
    """Return the year-end forecast: the current-year seasonal pace nudged toward history.
    Geometric blend pace**w * prior_level**(1-w) with w = elapsed_time / (elapsed_time + k),
    elapsed_time being the calendar fraction of the year observed. The log-space (geometric)
    blend makes history a mild *proportional* nudge, not an absolute pull, so a genuinely
    low-spend year is not dragged up toward last year's total. Weighting on elapsed time (not
    seasonal share) keeps the current-year pace dominant by mid-year even for back-loaded
    categories; only the first weeks — when little data exists — lean on history for noise
    control. prior_level=None disables the nudge (pure pace, used by the back-test notebook).
    spike_excess (see spike_excess_this_year) is held out of spend_to_date before the pace is
    computed — so a one-off month isn't extrapolated as if it recurs all year — then added back
    once at the end, since that spend still genuinely happened.
    """
    day_of_year = as_of.timetuple().tm_yday
    if day_of_year <= 1:
        return spend_to_date if prior_level is None else prior_level
    baseline_spend = float(spend_to_date) - spike_excess
    naive_pace = baseline_spend / pacing_fraction(curve, as_of)
    if prior_level is None:
        return naive_pace + spike_excess
    days_in_year = 366.0 if calendar.isleap(as_of.year) else 365.0
    elapsed_time = day_of_year / days_in_year
    w = elapsed_time / (elapsed_time + k)
    if naive_pace <= 0.0:
        # Nothing spent yet: fall back to history discounted by current confidence.
        return float((1.0 - w) * prior_level) + spike_excess
    return (
        float(np.exp(w * np.log(naive_pace) + (1.0 - w) * np.log(prior_level)))
        + spike_excess
    )


def build_forecast_line(
    cumulative: pd.DataFrame,
    curve: pd.DataFrame,
    year: int,
    as_of: pd.Timestamp,
    freq: Literal["D", "W", "M"],
    prior_level: float | None = None,
    k: float = DEFAULT_SHRINKAGE_K,
    monthly_rate: float | None = None,
    monthly_deviation: float | None = None,
    seed_key: str | None = None,
    spike_excess: float = 0.0,
) -> pd.DataFrame:
    """Return actual and forecast cumulative series joined at as_of for a continuous line.
    Output columns: period_end (Timestamp), cumulative_chf (float), segment ("actual"/"forecast").
    prior_level shrinks the year-end target toward the historical annual level (see forecast_year_end).
    monthly_rate (set for lumpy categories) projects the remainder at that spike-resistant rate
    instead of following the seasonal curve. monthly_deviation + seed_key (also lumpy-only) wobble
    that projection to look like real spending instead of a dead-straight ramp, while still
    landing on the same year-end total (see _jitter_monthly_path). spike_excess (non-lumpy only)
    flattens a one-off month out of the seasonal pace before it lands here (see forecast_year_end).
    """
    actual = cumulative[cumulative["period_end"] <= as_of][
        ["period_end", "cumulative_chf"]
    ].copy()
    actual["segment"] = "actual"

    spend_to_date = (
        float(actual["cumulative_chf"].iloc[-1]) if not actual.empty else 0.0
    )

    # Extend the solid actual line flat to as_of when the last data point predates it
    # (a category whose most recent bucket is weeks old). Cumulative spend is unchanged
    # since then, and this makes the actual line meet the forecast start — no visible gap.
    if not actual.empty and actual["period_end"].iloc[-1] < as_of:
        actual = pd.concat(
            [
                actual,
                pd.DataFrame(
                    {
                        "period_end": [as_of],
                        "cumulative_chf": [spend_to_date],
                        "segment": ["actual"],
                    }
                ),
            ],
            ignore_index=True,
        )

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
    if monthly_rate is not None:
        # Lumpy category: extend linearly at the median monthly rate from as_of onward.
        as_of_frac = float(as_of.timetuple().tm_yday) / days_in_year
        months_ahead: npt.NDArray[np.float64] = np.array(
            [
                12.0 * (float(p.timetuple().tm_yday) / days_in_year - as_of_frac)
                for p in future_periods
            ],
            dtype=np.float64,
        )
        fc_cum: npt.NDArray[np.float64]
        if (
            monthly_deviation is not None
            and monthly_deviation > 0.0
            and seed_key is not None
        ):
            fc_cum = _jitter_monthly_path(
                spend_to_date, monthly_rate, monthly_deviation, months_ahead, seed_key
            )
        else:
            fc_cum = np.asarray(
                spend_to_date + monthly_rate * months_ahead, dtype=np.float64
            )
    else:
        year_end_fc = forecast_year_end(
            spend_to_date, curve, as_of, prior_level, k, spike_excess
        )
        # The blend (prior_level nudge, spike_excess add-back) can pull year_end_fc away from
        # spend_to_date / pacing_fraction(as_of) — the only ratio under which curve_share * pace
        # would land exactly on spend_to_date at as_of. Scaling the raw curve by year_end_fc
        # (the old approach) ignored that and could jump - even downward - right at the seam.
        # Instead anchor at spend_to_date and spread (year_end_fc - spend_to_date) across the
        # remaining periods in proportion to the curve's remaining share, so the forecast always
        # starts exactly at spend_to_date and lands exactly on year_end_fc, monotonically.
        year_end_fc = max(year_end_fc, spend_to_date)
        xp: npt.NDArray[np.float64] = curve["year_fraction"].to_numpy(dtype=float)
        fp: npt.NDArray[np.float64] = curve["cum_share"].to_numpy(dtype=float)
        as_of_share = pacing_fraction(curve, as_of)
        remaining_share = max(1.0 - as_of_share, 1e-9)
        fc_fracs: npt.NDArray[np.float64] = np.array(
            [float(p.timetuple().tm_yday) / days_in_year for p in future_periods],
            dtype=np.float64,
        )
        fc_shares = np.interp(fc_fracs, xp, fp)
        progress = np.clip((fc_shares - as_of_share) / remaining_share, 0.0, None)
        fc_cum = np.asarray(
            spend_to_date + (year_end_fc - spend_to_date) * progress, dtype=np.float64
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
    levels: dict[str, float | None],
    year: int,
    as_of: pd.Timestamp,
    k: float = DEFAULT_SHRINKAGE_K,
    lumpy_rates: dict[str, float] | None = None,
    spike_excess: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Return one row per budgeted category with spend, forecast, and over/under columns.
    Uses year to build a leap-year-aware linear fallback for categories absent from curves;
    levels holds each category's historical annual level for the shrinkage blend (None to skip).
    lumpy_rates maps lumpy categories to their median monthly rate; those forecast from that rate
    instead of the seasonal pace. spike_excess (non-lumpy only) flattens a one-off month out of
    the seasonal pace before forecasting (see forecast_year_end).
    """
    rates = lumpy_rates or {}
    excesses = spike_excess or {}
    n_days = 366 if calendar.isleap(year) else 365
    _grid: npt.NDArray[np.float64] = np.array(
        [d / float(n_days) for d in range(1, n_days + 1)]
    )
    linear_fallback = pd.DataFrame({"year_fraction": _grid, "cum_share": _grid.copy()})

    spend_lookup: dict[str, float] = {
        str(k): float(v)
        for k, v in zip(spend["category"], spend["spend_chf"], strict=False)
    }

    # Linear pro-ration of the year elapsed so far: the "Now" pace target assumes even
    # spending (5/12 of the budget by June 1), independent of any category's seasonality.
    elapsed_frac = min(as_of.timetuple().tm_yday / n_days, 1.0)

    rows: list[dict[str, object]] = []
    for b in budgets:
        cat = b.category
        budget_chf = b.budget_chf
        spend_chf = spend_lookup.get(cat, 0.0)
        curve = curves.get(cat, linear_fallback)

        if cat in rates:
            forecast_chf = forecast_year_end_lumpy(spend_chf, rates[cat], as_of)
        else:
            forecast_chf = forecast_year_end(
                spend_chf, curve, as_of, levels.get(cat), k, excesses.get(cat, 0.0)
            )
        # Round each magnitude to whole CHF (the table shows no decimals) and derive the
        # deltas from the rounded values, so the displayed columns stay internally
        # consistent: Spent minus Budget-used-Now equals Now delta exactly, no rounding drift.
        spend_r = round(spend_chf)
        budget_r = round(budget_chf)
        forecast_r = round(forecast_chf)
        pace_r = round(budget_chf * elapsed_frac)  # spent at an even pace by now
        over_under_now_chf = spend_r - pace_r
        over_under_eoy_chf = forecast_r - budget_r
        # Now Δ % is relative to the pro-rated pace target (how far off pace right now);
        # EOY Δ % is relative to the full annual budget (forecast vs budget).
        over_under_now_pct = 0.0 if pace_r == 0 else over_under_now_chf / pace_r
        over_under_eoy_pct = 0.0 if budget_r == 0 else over_under_eoy_chf / budget_r

        rows.append(
            {
                "category": cat,
                "spend_chf": spend_r,
                "budget_chf": budget_r,
                "forecast_chf": forecast_r,
                "pace_budget_chf": pace_r,
                "over_under_now_chf": over_under_now_chf,
                "over_under_now_pct": over_under_now_pct,
                "over_under_eoy_chf": over_under_eoy_chf,
                "over_under_eoy_pct": over_under_eoy_pct,
            }
        )

    return pd.DataFrame(rows)
