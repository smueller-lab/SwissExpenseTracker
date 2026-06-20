"""Tests for DataLoader balance-sheet attributes (pdf_BalanceSheet, scalars, pivot, YoY).

Uses a copy of the real DB in tmp_path — never touches database/transactions.db.
The dashboard pipeline is run on the copy to ensure balance-sheet tables exist.
"""

from __future__ import annotations

import math
import shutil

from pathlib import Path

import pandas as pd
import pytest

from swiss_exp_tracker.pipeline_dash.config import BALANCE_SHEET_MAJOR_CATEGORIES

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Constants and fixtures
# ---------------------------------------------------------------------------


def _real_db() -> Path:
    """Return the path to the real transactions DB from the pipeline ingestion config."""
    from swiss_exp_tracker.pipeline_ingestion.config import INGESTION_DB_PATH

    return Path(INGESTION_DB_PATH)


_EXPECTED_BS_COLUMNS = {
    "year",
    "income",
    "expense",
    "invested",
    "saved",
    "total_plus",
    "savings_rate_pct",
    "invested_rate_pct",
    "expense_rate_pct",
    "cumulative_saved",
    "yoy_saved_pct",
    "in_plus",
}

_EXPECTED_CAT_COLUMNS = {"year", "category", "amount"}

_MAJOR_CATEGORY_LABELS = [label for label, _, _ in BALANCE_SHEET_MAJOR_CATEGORIES]


@pytest.fixture(scope="module")
def populated_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Copy the real DB to a temp directory and run the dashboard pipeline on it."""
    if not _real_db().exists():
        pytest.skip("real transactions DB not available")
    tmp_path = tmp_path_factory.mktemp("loader_bs")
    dest = tmp_path / "transactions.db"
    shutil.copy(_real_db(), dest)

    from swiss_exp_tracker.pipeline_dash.pipeline import run_dashboard_pipeline

    run_dashboard_pipeline(db_path=dest)
    return dest


@pytest.fixture(scope="module")
def loader(populated_db: Path) -> object:
    """Initialise DataLoader against the populated temp DB; reused across module tests."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    return DataLoader(db_path=populated_db)


# ---------------------------------------------------------------------------
# pdf_BalanceSheet — presence, columns, non-empty
# ---------------------------------------------------------------------------


def test_pdf_balance_sheet_not_none(loader: object) -> None:
    """DataLoader.pdf_BalanceSheet is not None after initialisation."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert loader.pdf_BalanceSheet is not None


def test_pdf_balance_sheet_non_empty(loader: object) -> None:
    """pdf_BalanceSheet contains at least one row."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert not loader.pdf_BalanceSheet.empty


def test_pdf_balance_sheet_expected_columns(loader: object) -> None:
    """pdf_BalanceSheet has all expected column names."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    actual_cols = set(loader.pdf_BalanceSheet.columns)
    missing = _EXPECTED_BS_COLUMNS - actual_cols
    assert not missing, f"Missing columns: {missing}"


def test_pdf_balance_sheet_year_column_is_integer(loader: object) -> None:
    """year column contains integer values (never pd.Period or string)."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    years = loader.pdf_BalanceSheet["year"]
    assert pd.api.types.is_integer_dtype(years), f"year dtype is {years.dtype}"


# ---------------------------------------------------------------------------
# pdf_BalanceSheetCategories — presence, columns, labels
# ---------------------------------------------------------------------------


def test_pdf_balance_sheet_categories_not_none(loader: object) -> None:
    """DataLoader.pdf_BalanceSheetCategories is not None."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert loader.pdf_BalanceSheetCategories is not None


def test_pdf_balance_sheet_categories_non_empty(loader: object) -> None:
    """pdf_BalanceSheetCategories contains at least one row."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert not loader.pdf_BalanceSheetCategories.empty


def test_pdf_balance_sheet_categories_expected_columns(loader: object) -> None:
    """pdf_BalanceSheetCategories has all expected column names."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    actual_cols = set(loader.pdf_BalanceSheetCategories.columns)
    missing = _EXPECTED_CAT_COLUMNS - actual_cols
    assert not missing, f"Missing columns: {missing}"


def test_pdf_balance_sheet_categories_only_major_labels(loader: object) -> None:
    """All category values in pdf_BalanceSheetCategories are BALANCE_SHEET_MAJOR_CATEGORIES labels."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    actual_labels = set(loader.pdf_BalanceSheetCategories["category"].unique())
    expected = set(_MAJOR_CATEGORY_LABELS)
    assert actual_labels.issubset(
        expected
    ), f"Unexpected labels: {actual_labels - expected}"


# ---------------------------------------------------------------------------
# v_BS_* scalars — types and finiteness
# ---------------------------------------------------------------------------


def test_v_bs_latest_year_is_int(loader: object) -> None:
    """v_BS_LatestYear is an int > 0."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert isinstance(loader.v_BS_LatestYear, int)
    assert loader.v_BS_LatestYear > 0


def test_v_bs_total_invested_all_time_is_finite_float(loader: object) -> None:
    """v_BS_TotalInvestedAllTime is a finite non-negative float (sum of yearly invested)."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert isinstance(loader.v_BS_TotalInvestedAllTime, float)
    assert math.isfinite(loader.v_BS_TotalInvestedAllTime)
    assert loader.v_BS_TotalInvestedAllTime >= 0.0


def test_v_bs_total_invested_equals_invested_sum(loader: object) -> None:
    """v_BS_TotalInvestedAllTime matches the sum of the invested column."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    expected = float(loader.pdf_BalanceSheet["invested"].sum())
    assert loader.v_BS_TotalInvestedAllTime == pytest.approx(expected)


def test_v_bs_savings_rate_avg_is_finite_float(loader: object) -> None:
    """v_BS_SavingsRateAvg is a finite float (mean across all years)."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert isinstance(loader.v_BS_SavingsRateAvg, float)
    assert math.isfinite(loader.v_BS_SavingsRateAvg)


def test_v_bs_lifetime_net_gain_is_finite_float(loader: object) -> None:
    """v_BS_LifetimeNetGain is a finite float (latest cumulative_saved)."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert isinstance(loader.v_BS_LifetimeNetGain, float)
    assert math.isfinite(loader.v_BS_LifetimeNetGain)


def test_v_bs_lifetime_net_gain_equals_latest_cumulative(loader: object) -> None:
    """v_BS_LifetimeNetGain matches cumulative_saved in the first (newest) row of pdf_BalanceSheet."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    expected = float(loader.pdf_BalanceSheet.iloc[0]["cumulative_saved"])
    assert loader.v_BS_LifetimeNetGain == pytest.approx(expected)


def test_v_bs_avg_annual_net_gain_is_finite_float(loader: object) -> None:
    """v_BS_AvgAnnualNetGain is a finite float."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert isinstance(loader.v_BS_AvgAnnualNetGain, float)
    assert math.isfinite(loader.v_BS_AvgAnnualNetGain)


def test_v_bs_avg_annual_net_gain_equals_total_plus_mean(loader: object) -> None:
    """v_BS_AvgAnnualNetGain equals the mean of the total_plus column."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    expected = float(loader.pdf_BalanceSheet["total_plus"].mean())
    assert loader.v_BS_AvgAnnualNetGain == pytest.approx(expected)


# ---------------------------------------------------------------------------
# pdf_CategorySpendPivot — structure
# ---------------------------------------------------------------------------


def test_category_spend_pivot_not_none(loader: object) -> None:
    """pdf_CategorySpendPivot is not None."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert loader.pdf_CategorySpendPivot is not None


def test_category_spend_pivot_non_empty(loader: object) -> None:
    """pdf_CategorySpendPivot has at least one row."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert not loader.pdf_CategorySpendPivot.empty


def test_category_spend_pivot_all_major_category_columns(loader: object) -> None:
    """pdf_CategorySpendPivot has one column per BALANCE_SHEET_MAJOR_CATEGORIES label in order."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    pivot = loader.pdf_CategorySpendPivot
    for label in _MAJOR_CATEGORY_LABELS:
        assert label in pivot.columns, f"Missing category column: {label}"


def test_category_spend_pivot_sorted_year_descending(loader: object) -> None:
    """pdf_CategorySpendPivot index is sorted descending (newest year first)."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    pivot = loader.pdf_CategorySpendPivot
    years = list(pivot.index)
    assert years == sorted(years, reverse=True), f"Index not DESC: {years}"


def test_category_spend_pivot_no_negative_values(loader: object) -> None:
    """All values in pdf_CategorySpendPivot are >= 0 (zero-filled for missing combos)."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    pivot = loader.pdf_CategorySpendPivot
    assert (pivot.values >= 0).all(), "Negative values found in category pivot"


def test_category_spend_pivot_column_order_matches_config(loader: object) -> None:
    """Column order in pdf_CategorySpendPivot matches BALANCE_SHEET_MAJOR_CATEGORIES order."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    pivot_cols = list(loader.pdf_CategorySpendPivot.columns)
    assert pivot_cols == _MAJOR_CATEGORY_LABELS


# ---------------------------------------------------------------------------
# pdf_CategorySpendYoY — structure and oldest-row NaN
# ---------------------------------------------------------------------------


def test_category_spend_yoy_not_none(loader: object) -> None:
    """pdf_CategorySpendYoY is not None."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert loader.pdf_CategorySpendYoY is not None


def test_category_spend_yoy_same_shape_as_pivot(loader: object) -> None:
    """pdf_CategorySpendYoY has the same shape as pdf_CategorySpendPivot."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert loader.pdf_CategorySpendYoY.shape == loader.pdf_CategorySpendPivot.shape


def test_category_spend_yoy_oldest_row_all_nan(loader: object) -> None:
    """The oldest year's row in pdf_CategorySpendYoY is entirely NaN (no prior year)."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    yoy = loader.pdf_CategorySpendYoY
    if len(yoy) < 2:
        pytest.skip("Need at least two years in the YoY table for this check")

    oldest_year = yoy.index.min()
    oldest_row = yoy.loc[oldest_year]
    # Every cell in the oldest row should be NaN (no prior year to compare against)
    assert (
        oldest_row.isna().all()
    ), f"Oldest year {oldest_year} row is not all NaN: {oldest_row.to_dict()}"


def test_category_spend_yoy_non_oldest_rows_have_values(loader: object) -> None:
    """Rows newer than the oldest year have at least one non-NaN YoY value."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    yoy = loader.pdf_CategorySpendYoY
    if len(yoy) < 2:
        pytest.skip("Need at least two years for this check")

    oldest_year = yoy.index.min()
    newer_rows = yoy.loc[yoy.index > oldest_year]
    assert not newer_rows.isna().all(
        axis=None
    ), "All non-oldest rows are NaN — expected at least some computed YoY values"


def test_category_spend_yoy_spot_check_one_cell_delta(loader: object) -> None:
    """YoY value for a specific cell matches (pivot_curr - pivot_prev) / |pivot_prev| * 100."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    pivot = loader.pdf_CategorySpendPivot
    yoy = loader.pdf_CategorySpendYoY

    if len(pivot) < 2:
        pytest.skip("Need at least two years for YoY spot-check")

    years_asc = sorted(pivot.index.tolist())
    # Use the second-oldest year as curr (it has a valid prior year)
    curr_year = years_asc[1]
    prev_year = years_asc[0]

    # Pick "Groceries" as the test category (likely present in real data)
    category = "Groceries"
    curr_val = float(pivot.loc[curr_year, category])
    prev_val = float(pivot.loc[prev_year, category])

    if prev_val == 0:
        pytest.skip(
            f"Groceries prior-year value is 0 for year {prev_year}; skip spot-check"
        )

    expected_yoy = (curr_val - prev_val) / abs(prev_val) * 100
    actual_yoy = float(yoy.loc[curr_year, category])
    assert actual_yoy == pytest.approx(expected_yoy, rel=1e-6)


def test_category_spend_yoy_sorted_year_descending(loader: object) -> None:
    """pdf_CategorySpendYoY index is sorted descending (newest year first)."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    yoy = loader.pdf_CategorySpendYoY
    years = list(yoy.index)
    assert years == sorted(years, reverse=True), f"YoY index not DESC: {years}"


# ---------------------------------------------------------------------------
# DataLoader initialisation guard — empty-DB fallback
# ---------------------------------------------------------------------------


def test_dataloader_initialises_without_error(populated_db: Path) -> None:
    """DataLoader(db_path=...) does not raise when the balance-sheet tables exist."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    loader_fresh = DataLoader(db_path=populated_db)
    assert loader_fresh.pdf_BalanceSheet is not None
    assert loader_fresh.pdf_BalanceSheetCategories is not None
    assert loader_fresh.pdf_CategorySpendPivot is not None
    assert loader_fresh.pdf_CategorySpendYoY is not None
