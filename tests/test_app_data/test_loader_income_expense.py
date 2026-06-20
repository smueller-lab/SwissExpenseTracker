"""Tests for DataLoader.get_IncomeExpenseMonthly().

Uses a fixture DB built from deterministic seed data in tmp_path — never touches
database/transactions.db.  The dashboard pipeline is run once per module (via the
conftest populated_db fixture) to ensure the net-balance-month table exists.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def loader(populated_db: Path) -> object:
    """Initialise DataLoader against the populated temp DB; reused across module tests."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    return DataLoader(db_path=populated_db)


# ---------------------------------------------------------------------------
# get_IncomeExpenseMonthly — columns and basic shape
# ---------------------------------------------------------------------------


def test_get_income_expense_monthly_returns_dataframe(loader: object) -> None:
    """get_IncomeExpenseMonthly returns a non-None DataFrame."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    result = loader.get_IncomeExpenseMonthly()
    assert result is not None


def test_get_income_expense_monthly_has_expected_columns(loader: object) -> None:
    """Result contains Month, Expense and Income columns."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    result = loader.get_IncomeExpenseMonthly()
    for col in ("Month", "Expense", "Income"):
        assert col in result.columns, f"Missing column: {col}"


# ---------------------------------------------------------------------------
# Window constraint — at most 15 rows
# ---------------------------------------------------------------------------


def test_get_income_expense_monthly_window_at_most_15_rows(loader: object) -> None:
    """Result contains at most 15 rows (the trailing 15-month window)."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    result = loader.get_IncomeExpenseMonthly()
    assert len(result) <= 15


# ---------------------------------------------------------------------------
# Salary gate — every returned month has a salary transaction
# ---------------------------------------------------------------------------


def test_get_income_expense_monthly_salary_gate(loader: object) -> None:
    """All months in the result correspond to a salary month in pdf_Master."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)

    salary_months: set[str] = {
        str(p)
        for p in loader.pdf_Master.loc[
            loader.pdf_Master["category_main"] == "Salary", "date"
        ].dt.to_period("M")
    }
    result = loader.get_IncomeExpenseMonthly()

    # Empty result is trivially a subset — guard passes
    assert set(result["Month"]).issubset(
        salary_months
    ), f"Non-salary months found: {set(result['Month']) - salary_months}"


# ---------------------------------------------------------------------------
# Sort order
# ---------------------------------------------------------------------------


def test_get_income_expense_monthly_sorted_ascending(loader: object) -> None:
    """Result is sorted by Month in ascending (chronological) order."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    result = loader.get_IncomeExpenseMonthly()

    if result.empty:
        return  # trivially passes when no salary months exist

    months = result["Month"].tolist()
    assert months == sorted(months), f"Month column not sorted ascending: {months}"


# ---------------------------------------------------------------------------
# Non-mutation guard — pdf_Master and pdf_NetBalanceMonth unchanged
# ---------------------------------------------------------------------------


def test_get_income_expense_monthly_does_not_mutate_master(loader: object) -> None:
    """Calling get_IncomeExpenseMonthly does not mutate pdf_Master shape or columns."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    shape_before = loader.pdf_Master.shape
    cols_before = list(loader.pdf_Master.columns)

    loader.get_IncomeExpenseMonthly()

    assert loader.pdf_Master.shape == shape_before
    assert list(loader.pdf_Master.columns) == cols_before


def test_get_income_expense_monthly_does_not_mutate_net_balance_month(
    loader: object,
) -> None:
    """Calling get_IncomeExpenseMonthly does not mutate pdf_NetBalanceMonth columns."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    cols_before = list(loader.pdf_NetBalanceMonth.columns)
    shape_before = loader.pdf_NetBalanceMonth.shape

    loader.get_IncomeExpenseMonthly()

    assert loader.pdf_NetBalanceMonth.shape == shape_before
    assert list(loader.pdf_NetBalanceMonth.columns) == cols_before
