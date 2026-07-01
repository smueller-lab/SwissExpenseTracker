"""Tests for DataLoader budget-related attributes and methods.

Uses a fixture DB built from deterministic seed data — never touches the real transactions.db.
Module-scoped loader covers read-only tests; function-scoped writable_loader covers save_budgets.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def loader(populated_db: Path) -> object:
    """Initialise DataLoader against the module-scoped populated temp DB."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    return DataLoader(db_path=populated_db)


@pytest.fixture()
def writable_loader(tmp_path: Path) -> object:
    """Fresh DB + DataLoader per test — safe for tests that call save_budgets."""
    from swiss_exp_tracker.app.data.loader import DataLoader
    from swiss_exp_tracker.pipeline_dash.pipeline import run_dashboard_pipeline
    from tests.fixtures.db_builder import build_fixture_db
    from tests.fixtures.seed_data import make_seed_groceries
    from tests.fixtures.seed_data import make_seed_transactions

    db_path = tmp_path / "transactions.db"
    build_fixture_db(db_path, make_seed_transactions(), make_seed_groceries())
    run_dashboard_pipeline(db_path=db_path)
    return DataLoader(db_path=db_path)


# ---------------------------------------------------------------------------
# pdf_Budget — presence and structure
# ---------------------------------------------------------------------------


def test_pdf_budget_is_not_none(loader: object) -> None:
    """DataLoader.pdf_Budget is not None after initialisation."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert loader.pdf_Budget is not None


def test_pdf_budget_has_expected_columns(loader: object) -> None:
    """pdf_Budget has all four expected columns regardless of content."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert set(loader.pdf_Budget.columns) == {
        "category",
        "year",
        "budget_chf",
        "updated_at",
    }


def test_pdf_budget_empty_initially(loader: object) -> None:
    """pdf_Budget is empty before any budgets are saved to the fixture DB."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert loader.pdf_Budget.empty


# ---------------------------------------------------------------------------
# budget_categories
# ---------------------------------------------------------------------------


def test_budget_categories_is_sorted(loader: object) -> None:
    """budget_categories is in ascending alphabetical order."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    cats = loader.budget_categories
    assert cats == sorted(cats)


def test_budget_categories_are_unique(loader: object) -> None:
    """budget_categories has no duplicate entries."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    cats = loader.budget_categories
    assert len(cats) == len(set(cats))


def test_budget_categories_non_empty(loader: object) -> None:
    """budget_categories is non-empty because seed data has EXPENSE transactions."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert len(loader.budget_categories) > 0


def test_budget_categories_only_expense_types(loader: object) -> None:
    """Every category in budget_categories has at least one EXPENSE row in pdf_Master."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    expense_cats = set(
        loader.pdf_Master.loc[
            loader.pdf_Master["transaction_type"] == "EXPENSE", "category_main"
        ].unique()
    )
    assert set(loader.budget_categories) == expense_cats


# ---------------------------------------------------------------------------
# current_year
# ---------------------------------------------------------------------------


def test_current_year_is_int(loader: object) -> None:
    """current_year is a positive integer."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    assert isinstance(loader.current_year, int)
    assert loader.current_year > 0


def test_current_year_matches_max_date_year(loader: object) -> None:
    """current_year equals the maximum year found in pdf_Master['date']."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    expected = int(loader.pdf_Master["date"].dt.year.max())
    assert loader.current_year == expected


# ---------------------------------------------------------------------------
# save_budgets / get_budgets
# ---------------------------------------------------------------------------


def test_save_and_get_budgets_roundtrip(writable_loader: object) -> None:
    """save_budgets then get_budgets returns CategoryBudget models with matching fields."""
    from swiss_exp_tracker.app.data.budget_models import CategoryBudget
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    budgets = [
        CategoryBudget(category="Groceries", budget_chf=500.0),
        CategoryBudget(category="Housing", budget_chf=1800.0),
    ]
    writable_loader.save_budgets(2024, budgets)
    fetched = writable_loader.get_budgets(2024)

    assert len(fetched) == 2
    cat_map = {b.category: b.budget_chf for b in fetched}
    assert cat_map["Groceries"] == pytest.approx(500.0)
    assert cat_map["Housing"] == pytest.approx(1800.0)


def test_save_budgets_overwrite_no_duplicate(writable_loader: object) -> None:
    """Re-saving with a new budget_chf overwrites the existing row without adding a duplicate."""
    from swiss_exp_tracker.app.data.budget_models import CategoryBudget
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    writable_loader.save_budgets(
        2024, [CategoryBudget(category="Groceries", budget_chf=400.0)]
    )
    writable_loader.save_budgets(
        2024, [CategoryBudget(category="Groceries", budget_chf=600.0)]
    )
    fetched = writable_loader.get_budgets(2024)
    groceries_rows = [b for b in fetched if b.category == "Groceries"]
    assert len(groceries_rows) == 1
    assert groceries_rows[0].budget_chf == pytest.approx(600.0)


def test_get_budgets_empty_for_unconfigured_year(loader: object) -> None:
    """get_budgets returns an empty list for a year that has no saved budgets."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    result = loader.get_budgets(1990)
    assert result == []


# ---------------------------------------------------------------------------
# get_category_year_spend
# ---------------------------------------------------------------------------


def test_get_category_year_spend_columns(loader: object) -> None:
    """get_category_year_spend returns [category, spend_chf] columns."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    df = loader.get_category_year_spend(2024)
    assert set(df.columns) == {"category", "spend_chf"}


def test_get_category_year_spend_non_negative(loader: object) -> None:
    """All spend_chf values returned are >= 0."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    df = loader.get_category_year_spend(2024)
    assert (df["spend_chf"] >= 0).all()


def test_get_category_year_spend_excludes_income(loader: object) -> None:
    """No INCOME category_main values appear in the result."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    spend = loader.get_category_year_spend(2024)
    income_cats = set(
        loader.pdf_Master.loc[
            loader.pdf_Master["transaction_type"] == "INCOME", "category_main"
        ].unique()
    )
    assert not set(spend["category"]).intersection(income_cats)


# ---------------------------------------------------------------------------
# get_category_cumulative (parametrized by freq)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("freq", ["D", "W", "M"])
def test_get_category_cumulative_has_expected_columns(
    loader: object, freq: str
) -> None:
    """Output has exactly [category, period_end, cumulative_chf] columns."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    df = loader.get_category_cumulative(2024, ["Groceries"], freq)
    assert set(df.columns) == {"category", "period_end", "cumulative_chf"}


@pytest.mark.parametrize("freq", ["D", "W", "M"])
def test_get_category_cumulative_period_end_is_datetime(
    loader: object, freq: str
) -> None:
    """period_end contains real datetime64 values, not pd.Period."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    df = loader.get_category_cumulative(2024, ["Groceries"], freq)
    assert not df.empty
    assert pd.api.types.is_datetime64_any_dtype(df["period_end"])


@pytest.mark.parametrize("freq", ["D", "W", "M"])
def test_get_category_cumulative_chf_non_decreasing(loader: object, freq: str) -> None:
    """cumulative_chf is monotonically non-decreasing within the Groceries category."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    df = loader.get_category_cumulative(2024, ["Groceries"], freq)
    assert not df.empty
    diffs = df["cumulative_chf"].diff().dropna()
    assert (diffs >= -1e-9).all()


def test_get_category_cumulative_monthly_cadence(loader: object) -> None:
    """Monthly freq: all period_end values are on the first day of the month (month-start)."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    df = loader.get_category_cumulative(2024, ["Groceries"], "M")
    assert not df.empty
    assert (df["period_end"].dt.day == 1).all()


def test_get_category_cumulative_daily_cadence(loader: object) -> None:
    """Daily freq: consecutive period_end values differ by exactly 1 day."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    df = loader.get_category_cumulative(2024, ["Groceries"], "D")
    assert len(df) > 1
    diffs = df["period_end"].diff().dt.days.dropna()
    assert (diffs == 1).all()


def test_get_category_cumulative_weekly_cadence(loader: object) -> None:
    """Weekly freq: consecutive period_end values differ by approximately 7 days."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    df = loader.get_category_cumulative(2024, ["Groceries"], "W")
    assert len(df) > 1
    diffs = df["period_end"].diff().dt.days.dropna()
    assert diffs.between(6, 8).all()


def test_get_category_cumulative_empty_for_absent_category(loader: object) -> None:
    """Returns an empty DataFrame for a category not present in the requested year."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    df = loader.get_category_cumulative(2024, ["NonExistentCategory"], "M")
    assert df.empty


# ---------------------------------------------------------------------------
# get_category_period_history (parametrized by freq)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("freq", ["D", "W", "M"])
def test_get_category_period_history_columns(loader: object, freq: str) -> None:
    """Output has exactly [category, year, year_fraction, spend_chf] columns."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    df = loader.get_category_period_history(["Groceries"], 2024, freq)
    assert set(df.columns) == {"category", "year", "year_fraction", "spend_chf"}


@pytest.mark.parametrize("freq", ["D", "W", "M"])
def test_get_category_period_history_excludes_target_year(
    loader: object, freq: str
) -> None:
    """All rows have year strictly less than exclude_year=2024."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    df = loader.get_category_period_history(["Groceries"], 2024, freq)
    assert not df.empty
    assert (df["year"] < 2024).all()


@pytest.mark.parametrize("freq", ["D", "W", "M"])
def test_get_category_period_history_year_fraction_in_range(
    loader: object, freq: str
) -> None:
    """year_fraction is strictly positive and at most 1.0 for every row."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    df = loader.get_category_period_history(["Groceries"], 2024, freq)
    assert not df.empty
    assert (df["year_fraction"] > 0).all()
    assert (df["year_fraction"] <= 1.0 + 1e-9).all()


def test_get_category_period_history_bucket_count_scales_with_freq(
    loader: object,
) -> None:
    """Monthly history has fewer rows than weekly, which has fewer rows than daily."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    hist_m = loader.get_category_period_history(["Groceries"], 2024, "M")
    hist_w = loader.get_category_period_history(["Groceries"], 2024, "W")
    hist_d = loader.get_category_period_history(["Groceries"], 2024, "D")
    assert len(hist_m) < len(hist_w) < len(hist_d)


def test_get_category_period_history_monthly_has_12_per_year(
    loader: object,
) -> None:
    """Monthly history for Groceries: exactly 12 rows per year (one bucket per month)."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    df = loader.get_category_period_history(["Groceries"], 2024, "M")
    rows_per_year = df.groupby("year").size()
    assert (rows_per_year == 12).all()


def test_get_category_period_history_empty_for_absent_category(
    loader: object,
) -> None:
    """Returns an empty DataFrame for a category absent from the history."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    df = loader.get_category_period_history(["NonExistentCategory"], 2024, "M")
    assert df.empty
