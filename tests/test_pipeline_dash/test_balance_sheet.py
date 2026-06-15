"""Tests for pipeline_dash/tables/balance_sheet.py.

Tests both the internal helpers (_aggregate_yearly, _compute_balance_sheet,
_compute_category_spend) as pure functions and the top-level build() which
writes to SQLite.
"""

from __future__ import annotations

import math
import sqlite3

from pathlib import Path

import pandas as pd
import pytest

from swiss_exp_tracker.pipeline_dash.config import BALANCE_SHEET_MAJOR_CATEGORIES
from swiss_exp_tracker.pipeline_dash.tables.balance_sheet import _aggregate_yearly
from swiss_exp_tracker.pipeline_dash.tables.balance_sheet import _compute_balance_sheet
from swiss_exp_tracker.pipeline_dash.tables.balance_sheet import _compute_category_spend
from swiss_exp_tracker.pipeline_dash.tables.balance_sheet import build

# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def _make_row(**overrides: object) -> dict[str, object]:
    """Return a valid transactions_use-like dict; each test only states what it varies."""
    base: dict[str, object] = {
        "date": pd.Timestamp("2024-06-15"),
        "amount": 100.0,
        "transaction_type": "EXPENSE",
        "category_main": "Groceries",
        "category_second": None,
        "merchant": "Migros",
    }
    base.update(overrides)
    return base


def _make_df(rows: list[dict[str, object]]) -> pd.DataFrame:
    """Build a DataFrame from row dicts and ensure date column is datetime."""
    if not rows:
        return pd.DataFrame(
            {
                "date": pd.Series([], dtype="datetime64[ns]"),
                "amount": pd.Series([], dtype=float),
                "transaction_type": pd.Series([], dtype=str),
                "category_main": pd.Series([], dtype=str),
                "category_second": pd.Series([], dtype=object),
                "merchant": pd.Series([], dtype=str),
            }
        )
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    """Create an empty SQLite DB; build() writes tables via to_sql."""
    db_path = tmp_path / "test.db"
    with sqlite3.connect(str(db_path)) as db:
        db.commit()
    return db_path


# ---------------------------------------------------------------------------
# _aggregate_yearly — pure function tests
# ---------------------------------------------------------------------------


def test_aggregate_yearly_income_expense_invested_split() -> None:
    """Income, standard expense, and invested are separated into the correct columns."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=80000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2024-03-01",
            amount=60000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
        _make_row(
            date="2024-06-01",
            amount=5000.0,
            transaction_type="EXPENSE",
            category_main="Investing",
        ),
    ]
    df = _make_df(rows)
    result = _aggregate_yearly(df)

    assert len(result) == 1
    row = result[result["year"] == 2024].iloc[0]
    assert row["income"] == pytest.approx(80000.0)
    assert row["expense"] == pytest.approx(60000.0)
    assert row["invested"] == pytest.approx(5000.0)


def test_aggregate_yearly_salary_expense_excluded_from_both() -> None:
    """Salary category_main EXPENSE rows are excluded from expense and invested."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=90000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2024-02-01",
            amount=500.0,
            transaction_type="EXPENSE",
            category_main="Salary",
        ),
        _make_row(
            date="2024-03-01",
            amount=1000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
    ]
    df = _make_df(rows)
    result = _aggregate_yearly(df)

    row = result.iloc[0]
    assert row["expense"] == pytest.approx(1000.0)
    assert row["invested"] == pytest.approx(0.0)


def test_aggregate_yearly_investing_not_in_expense() -> None:
    """Investing EXPENSE rows flow into invested, never into expense."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=50000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2024-02-01",
            amount=3000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
        _make_row(
            date="2024-03-01",
            amount=7000.0,
            transaction_type="EXPENSE",
            category_main="Investing",
        ),
    ]
    df = _make_df(rows)
    result = _aggregate_yearly(df)

    row = result.iloc[0]
    assert row["expense"] == pytest.approx(3000.0)
    assert row["invested"] == pytest.approx(7000.0)


def test_aggregate_yearly_multi_year_produces_one_row_per_year() -> None:
    """Each distinct year in the data produces exactly one output row."""
    rows = [
        _make_row(
            date="2022-01-01",
            amount=100.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
        _make_row(
            date="2023-01-01",
            amount=200.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
        _make_row(
            date="2024-01-01",
            amount=300.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
    ]
    df = _make_df(rows)
    result = _aggregate_yearly(df)

    assert len(result) == 3
    assert set(result["year"].tolist()) == {2022, 2023, 2024}


# ---------------------------------------------------------------------------
# _compute_balance_sheet — pure function tests
# ---------------------------------------------------------------------------


def test_compute_balance_sheet_saved_formula() -> None:
    """saved = income - expense - invested."""
    agg = pd.DataFrame(
        [{"year": 2024, "income": 80000.0, "expense": 60000.0, "invested": 5000.0}]
    )
    result = _compute_balance_sheet(agg)

    assert result.iloc[0]["saved"] == pytest.approx(15000.0)


def test_compute_balance_sheet_total_plus_equals_saved_plus_invested_each_year() -> (
    None
):
    """total_plus == saved + invested for every row in multi-year output."""
    agg = pd.DataFrame(
        [
            {"year": 2023, "income": 80000.0, "expense": 60000.0, "invested": 5000.0},
            {"year": 2024, "income": 90000.0, "expense": 70000.0, "invested": 8000.0},
        ]
    )
    result = _compute_balance_sheet(agg)

    for _, row in result.iterrows():
        assert row["total_plus"] == pytest.approx(row["saved"] + row["invested"])


def test_compute_balance_sheet_in_plus_positive_saved() -> None:
    """in_plus == 1 when saved >= 0."""
    agg = pd.DataFrame(
        [{"year": 2024, "income": 80000.0, "expense": 60000.0, "invested": 5000.0}]
    )
    result = _compute_balance_sheet(agg)

    assert int(result.iloc[0]["in_plus"]) == 1


def test_compute_balance_sheet_in_plus_negative_saved() -> None:
    """in_plus == 0 when saved < 0 (deliberately negative year)."""
    # saved = 70000 - 76000 - 0 = -6000
    agg = pd.DataFrame(
        [{"year": 2022, "income": 70000.0, "expense": 76000.0, "invested": 0.0}]
    )
    result = _compute_balance_sheet(agg)

    assert int(result.iloc[0]["in_plus"]) == 0
    assert result.iloc[0]["saved"] == pytest.approx(-6000.0)


def test_compute_balance_sheet_zero_income_rates_are_zero_and_finite() -> None:
    """Zero-income year: all rate columns are 0.0, not NaN or inf."""
    agg = pd.DataFrame(
        [{"year": 2020, "income": 0.0, "expense": 3000.0, "invested": 0.0}]
    )
    result = _compute_balance_sheet(agg)
    row = result.iloc[0]

    assert row["savings_rate_pct"] == pytest.approx(0.0)
    assert row["invested_rate_pct"] == pytest.approx(0.0)
    assert row["expense_rate_pct"] == pytest.approx(0.0)
    assert math.isfinite(float(row["savings_rate_pct"]))
    assert math.isfinite(float(row["invested_rate_pct"]))
    assert math.isfinite(float(row["expense_rate_pct"]))


def test_compute_balance_sheet_cumulative_saved_running_total() -> None:
    """cumulative_saved is the running sum of total_plus ordered by year ascending."""
    # 2022: saved=1500, invested=500 → total_plus=2000, cumulative=2000
    # 2023: saved=4000, invested=1000 → total_plus=5000, cumulative=7000
    agg = pd.DataFrame(
        [
            {"year": 2022, "income": 50000.0, "expense": 48000.0, "invested": 500.0},
            {"year": 2023, "income": 60000.0, "expense": 55000.0, "invested": 1000.0},
        ]
    )
    result = _compute_balance_sheet(agg)  # output is year DESC

    row_2023 = result[result["year"] == 2023].iloc[0]
    row_2022 = result[result["year"] == 2022].iloc[0]
    assert row_2022["cumulative_saved"] == pytest.approx(2000.0)
    assert row_2023["cumulative_saved"] == pytest.approx(7000.0)


def test_compute_balance_sheet_yoy_saved_pct_first_year_is_zero() -> None:
    """yoy_saved_pct is 0.0 for the oldest year (no prior year to compare)."""
    agg = pd.DataFrame(
        [
            {"year": 2023, "income": 80000.0, "expense": 65000.0, "invested": 5000.0},
            {"year": 2024, "income": 90000.0, "expense": 73000.0, "invested": 8000.0},
        ]
    )
    result = _compute_balance_sheet(agg)

    oldest_row = result[result["year"] == 2023].iloc[0]
    assert oldest_row["yoy_saved_pct"] == pytest.approx(0.0)


def test_compute_balance_sheet_yoy_saved_pct_second_year_hand_computed() -> None:
    """yoy_saved_pct for year N = (net_gain_N - net_gain_N-1) / |net_gain_N-1| * 100."""
    # net_gain (total_plus) = income - expense (investing not counted as expense)
    # 2023: net_gain = 80000 - 65000 = 15000
    # 2024: net_gain = 90000 - 73000 = 17000
    # yoy_2024 = (17000 - 15000) / 15000 * 100 = 13.3333...
    agg = pd.DataFrame(
        [
            {"year": 2023, "income": 80000.0, "expense": 65000.0, "invested": 5000.0},
            {"year": 2024, "income": 90000.0, "expense": 73000.0, "invested": 8000.0},
        ]
    )
    result = _compute_balance_sheet(agg)

    row_2024 = result[result["year"] == 2024].iloc[0]
    assert row_2024["yoy_saved_pct"] == pytest.approx(2000.0 / 15000.0 * 100.0)


def test_compute_balance_sheet_sorted_year_descending() -> None:
    """Output DataFrame is sorted by year descending (newest row at index 0)."""
    agg = pd.DataFrame(
        [
            {"year": 2022, "income": 70000.0, "expense": 65000.0, "invested": 1000.0},
            {"year": 2023, "income": 80000.0, "expense": 70000.0, "invested": 2000.0},
            {"year": 2024, "income": 90000.0, "expense": 80000.0, "invested": 3000.0},
        ]
    )
    result = _compute_balance_sheet(agg)

    years = list(result["year"])
    assert years == sorted(years, reverse=True)


def test_compute_balance_sheet_rate_columns_hand_computed() -> None:
    """savings_rate_pct, invested_rate_pct, expense_rate_pct match expected percentages."""
    # income=80000, expense=60000, invested=5000 → total_plus=20000
    # savings_rate = 20000/80000*100 = 25.0
    # invested_rate = 5000/80000*100 = 6.25
    # expense_rate = 60000/80000*100 = 75.0
    agg = pd.DataFrame(
        [{"year": 2024, "income": 80000.0, "expense": 60000.0, "invested": 5000.0}]
    )
    result = _compute_balance_sheet(agg)
    row = result.iloc[0]

    assert row["savings_rate_pct"] == pytest.approx(25.0)
    assert row["invested_rate_pct"] == pytest.approx(6.25)
    assert row["expense_rate_pct"] == pytest.approx(75.0)


# ---------------------------------------------------------------------------
# _compute_category_spend — pure function tests
# ---------------------------------------------------------------------------


def test_compute_category_spend_rent_housing_rent_only() -> None:
    """Housing/Rent rows contribute to Rent; hand-computed yearly total verified."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=1800.0,
            transaction_type="EXPENSE",
            category_main="Housing",
            category_second="Rent",
        ),
        _make_row(
            date="2024-02-01",
            amount=1800.0,
            transaction_type="EXPENSE",
            category_main="Housing",
            category_second="Rent",
        ),
        _make_row(
            date="2024-03-01",
            amount=250.0,
            transaction_type="EXPENSE",
            category_main="Housing",
            category_second="Utilities",
        ),
    ]
    df = _make_df(rows)
    result = _compute_category_spend(df)

    rent_rows = result[result["category"] == "Rent"]
    assert len(rent_rows) == 1
    assert rent_rows.iloc[0]["amount"] == pytest.approx(3600.0)


def test_compute_category_spend_non_rent_housing_not_in_rent_category() -> None:
    """Housing rows with category_second != 'Rent' do not appear under Rent."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=500.0,
            transaction_type="EXPENSE",
            category_main="Housing",
            category_second="Utilities",
        ),
    ]
    df = _make_df(rows)
    result = _compute_category_spend(df)

    rent_rows = result[result["category"] == "Rent"]
    assert len(rent_rows) == 0


def test_compute_category_spend_only_expense_rows_counted() -> None:
    """INCOME rows are never included in category spend totals."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=5000.0,
            transaction_type="INCOME",
            category_main="Groceries",
        ),
        _make_row(
            date="2024-01-15",
            amount=300.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
    ]
    df = _make_df(rows)
    result = _compute_category_spend(df)

    grocery_rows = result[result["category"] == "Groceries"]
    assert len(grocery_rows) == 1
    assert grocery_rows.iloc[0]["amount"] == pytest.approx(300.0)


def test_compute_category_spend_only_major_labels_in_output() -> None:
    """Only BALANCE_SHEET_MAJOR_CATEGORIES labels appear; no other categories."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=100.0,
            transaction_type="EXPENSE",
            category_main="Entertainment",
        ),
        _make_row(
            date="2024-01-01",
            amount=200.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
    ]
    df = _make_df(rows)
    result = _compute_category_spend(df)

    expected_labels = {label for label, _, _ in BALANCE_SHEET_MAJOR_CATEGORIES}
    actual_labels = set(result["category"].unique())
    assert actual_labels.issubset(expected_labels)


def test_compute_category_spend_entertainment_absent() -> None:
    """Entertainment is never in the output — it is not a major category."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=500.0,
            transaction_type="EXPENSE",
            category_main="Entertainment",
        ),
    ]
    df = _make_df(rows)
    result = _compute_category_spend(df)

    assert "Entertainment" not in result["category"].values


def test_compute_category_spend_empty_df_returns_empty() -> None:
    """Empty input returns an empty DataFrame with the expected columns."""
    df = _make_df([])
    result = _compute_category_spend(df)

    assert result.empty
    assert set(result.columns) == {"year", "category", "amount"}


# ---------------------------------------------------------------------------
# build() — integration + idempotency
# ---------------------------------------------------------------------------


def test_build_happy_path_multi_year_income_expense_invested(tmp_db: Path) -> None:
    """build() writes dash_balance_sheet; values match hand-computed results."""
    rows = [
        # 2023: income=80000, expense=60000, invested=5000 → saved=15000
        _make_row(
            date="2023-07-01",
            amount=80000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2023-08-01",
            amount=60000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
        _make_row(
            date="2023-09-01",
            amount=5000.0,
            transaction_type="EXPENSE",
            category_main="Investing",
        ),
        # 2024: income=90000, expense=70000, invested=8000 → saved=12000
        _make_row(
            date="2024-07-01",
            amount=90000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2024-08-01",
            amount=70000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
        _make_row(
            date="2024-09-01",
            amount=8000.0,
            transaction_type="EXPENSE",
            category_main="Investing",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        result = pd.read_sql("SELECT * FROM dash_balance_sheet ORDER BY year DESC", con)

    assert len(result) == 2

    row_2024 = result[result["year"] == 2024].iloc[0]
    assert row_2024["income"] == pytest.approx(90000.0)
    assert row_2024["expense"] == pytest.approx(70000.0)
    assert row_2024["invested"] == pytest.approx(8000.0)
    assert row_2024["saved"] == pytest.approx(12000.0)

    row_2023 = result[result["year"] == 2023].iloc[0]
    assert row_2023["income"] == pytest.approx(80000.0)
    assert row_2023["expense"] == pytest.approx(60000.0)
    assert row_2023["invested"] == pytest.approx(5000.0)
    assert row_2023["saved"] == pytest.approx(15000.0)


def test_build_total_plus_equals_saved_plus_invested_every_year(tmp_db: Path) -> None:
    """total_plus == saved + invested for every year written to dash_balance_sheet."""
    rows = [
        _make_row(
            date="2023-01-01",
            amount=80000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2023-02-01",
            amount=60000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
        _make_row(
            date="2023-03-01",
            amount=5000.0,
            transaction_type="EXPENSE",
            category_main="Investing",
        ),
        _make_row(
            date="2024-01-01",
            amount=90000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2024-02-01",
            amount=70000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
        _make_row(
            date="2024-03-01",
            amount=8000.0,
            transaction_type="EXPENSE",
            category_main="Investing",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        result = pd.read_sql("SELECT * FROM dash_balance_sheet", con)

    for _, row in result.iterrows():
        assert row["total_plus"] == pytest.approx(row["saved"] + row["invested"])


def test_build_invested_excluded_from_expense(tmp_db: Path) -> None:
    """Investing EXPENSE rows go into invested, not expense."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=50000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2024-02-01",
            amount=3000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
        _make_row(
            date="2024-03-01",
            amount=7000.0,
            transaction_type="EXPENSE",
            category_main="Investing",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        result = pd.read_sql("SELECT * FROM dash_balance_sheet", con)

    row = result.iloc[0]
    assert row["expense"] == pytest.approx(3000.0)
    assert row["invested"] == pytest.approx(7000.0)


def test_build_expense_excludes_net_balance_exclude_main(tmp_db: Path) -> None:
    """Salary EXPENSE rows are excluded from both expense and invested columns."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=90000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2024-02-01",
            amount=500.0,
            transaction_type="EXPENSE",
            category_main="Salary",
        ),
        _make_row(
            date="2024-03-01",
            amount=2000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        result = pd.read_sql("SELECT * FROM dash_balance_sheet", con)

    row = result.iloc[0]
    assert row["expense"] == pytest.approx(2000.0)
    assert row["invested"] == pytest.approx(0.0)


def test_build_in_plus_negative_year(tmp_db: Path) -> None:
    """in_plus == 0 when saved < 0 (deliberately negative year)."""
    rows = [
        _make_row(
            date="2022-01-01",
            amount=70000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2022-02-01",
            amount=78000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        result = pd.read_sql("SELECT * FROM dash_balance_sheet", con)

    row = result.iloc[0]
    assert row["saved"] == pytest.approx(-8000.0)
    assert int(row["in_plus"]) == 0


def test_build_in_plus_positive_year(tmp_db: Path) -> None:
    """in_plus == 1 when saved >= 0."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=80000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2024-02-01",
            amount=60000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        result = pd.read_sql("SELECT * FROM dash_balance_sheet", con)

    assert int(result.iloc[0]["in_plus"]) == 1


def test_build_zero_income_year_all_rates_zero(tmp_db: Path) -> None:
    """Zero-income year: all rate columns are 0.0, never NaN or inf."""
    rows = [
        _make_row(
            date="2020-06-01",
            amount=3000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        result = pd.read_sql("SELECT * FROM dash_balance_sheet", con)

    row = result.iloc[0]
    assert row["savings_rate_pct"] == pytest.approx(0.0)
    assert row["invested_rate_pct"] == pytest.approx(0.0)
    assert row["expense_rate_pct"] == pytest.approx(0.0)
    assert math.isfinite(float(row["savings_rate_pct"]))
    assert math.isfinite(float(row["invested_rate_pct"]))
    assert math.isfinite(float(row["expense_rate_pct"]))


def test_build_cumulative_saved_running_total_year_ascending(tmp_db: Path) -> None:
    """cumulative_saved is the running sum of total_plus in year-ascending order."""
    # 2022: saved=1500, invested=500 → total_plus=2000 → cumulative=2000
    # 2023: saved=4000, invested=1000 → total_plus=5000 → cumulative=7000
    rows = [
        _make_row(
            date="2022-06-01",
            amount=50000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2022-07-01",
            amount=48000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
        _make_row(
            date="2022-08-01",
            amount=500.0,
            transaction_type="EXPENSE",
            category_main="Investing",
        ),
        _make_row(
            date="2023-06-01",
            amount=60000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2023-07-01",
            amount=55000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
        _make_row(
            date="2023-08-01",
            amount=1000.0,
            transaction_type="EXPENSE",
            category_main="Investing",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        result = pd.read_sql("SELECT * FROM dash_balance_sheet ORDER BY year DESC", con)

    row_2023 = result[result["year"] == 2023].iloc[0]
    row_2022 = result[result["year"] == 2022].iloc[0]
    assert row_2022["cumulative_saved"] == pytest.approx(2000.0)
    assert row_2023["cumulative_saved"] == pytest.approx(7000.0)


def test_build_yoy_saved_pct_first_year_is_zero(tmp_db: Path) -> None:
    """yoy_saved_pct is 0.0 for the oldest year in the output table."""
    rows = [
        _make_row(
            date="2023-01-01",
            amount=80000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2023-02-01",
            amount=65000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
        _make_row(
            date="2024-01-01",
            amount=90000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2024-02-01",
            amount=73000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        result = pd.read_sql("SELECT * FROM dash_balance_sheet ORDER BY year ASC", con)

    oldest_row = result.iloc[0]
    assert oldest_row["year"] == 2023
    assert oldest_row["yoy_saved_pct"] == pytest.approx(0.0)


def test_build_output_sorted_year_descending(tmp_db: Path) -> None:
    """dash_balance_sheet rows are stored year DESC (newest first)."""
    rows = [
        _make_row(
            date="2022-01-01",
            amount=70000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2023-01-01",
            amount=80000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2024-01-01",
            amount=90000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        result = pd.read_sql("SELECT year FROM dash_balance_sheet", con)

    years = list(result["year"])
    assert years == sorted(years, reverse=True)


def test_build_categories_only_major_labels(tmp_db: Path) -> None:
    """dash_balance_sheet_categories contains only BALANCE_SHEET_MAJOR_CATEGORIES labels."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=500.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
        _make_row(
            date="2024-02-01",
            amount=200.0,
            transaction_type="EXPENSE",
            category_main="Entertainment",
        ),
        _make_row(
            date="2024-03-01",
            amount=300.0,
            transaction_type="EXPENSE",
            category_main="Sport",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        result = pd.read_sql(
            "SELECT DISTINCT category FROM dash_balance_sheet_categories", con
        )

    expected_labels = {label for label, _, _ in BALANCE_SHEET_MAJOR_CATEGORIES}
    actual_labels = set(result["category"].tolist())
    assert actual_labels.issubset(expected_labels)
    assert "Entertainment" not in actual_labels


def test_build_categories_entertainment_absent(tmp_db: Path) -> None:
    """Entertainment rows produce no entry in dash_balance_sheet_categories."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=100.0,
            transaction_type="EXPENSE",
            category_main="Entertainment",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        rows_cat = con.execute(
            "SELECT COUNT(*) FROM dash_balance_sheet_categories WHERE category='Entertainment'"
        ).fetchone()[0]

    assert rows_cat == 0


def test_build_categories_only_expense_rows(tmp_db: Path) -> None:
    """INCOME Groceries rows are not counted in dash_balance_sheet_categories."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=5000.0,
            transaction_type="INCOME",
            category_main="Groceries",
        ),
        _make_row(
            date="2024-02-01",
            amount=300.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        result = pd.read_sql(
            "SELECT * FROM dash_balance_sheet_categories WHERE category='Groceries'",
            con,
        )

    assert len(result) == 1
    assert result.iloc[0]["amount"] == pytest.approx(300.0)


def test_build_rent_mapping_housing_rent_rows_only(tmp_db: Path) -> None:
    """Housing/Rent row contributes to Rent; Housing/Utilities row does not."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=1800.0,
            transaction_type="EXPENSE",
            category_main="Housing",
            category_second="Rent",
        ),
        _make_row(
            date="2024-02-01",
            amount=1800.0,
            transaction_type="EXPENSE",
            category_main="Housing",
            category_second="Rent",
        ),
        _make_row(
            date="2024-03-01",
            amount=250.0,
            transaction_type="EXPENSE",
            category_main="Housing",
            category_second="Utilities",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        result = pd.read_sql(
            "SELECT * FROM dash_balance_sheet_categories WHERE category='Rent'", con
        )

    assert len(result) == 1
    assert result.iloc[0]["amount"] == pytest.approx(3600.0)


def test_build_rent_different_category_second_excluded(tmp_db: Path) -> None:
    """Housing rows with category_second != 'Rent' produce no Rent entry."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=500.0,
            transaction_type="EXPENSE",
            category_main="Housing",
            category_second="Utilities",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        rent_count = con.execute(
            "SELECT COUNT(*) FROM dash_balance_sheet_categories WHERE category='Rent'"
        ).fetchone()[0]

    assert rent_count == 0


def test_build_idempotency_no_duplicates(tmp_db: Path) -> None:
    """Second build() call replaces tables; row count stays the same (no duplicates)."""
    rows = [
        _make_row(
            date="2024-01-01",
            amount=90000.0,
            transaction_type="INCOME",
            category_main="Salary",
        ),
        _make_row(
            date="2024-02-01",
            amount=70000.0,
            transaction_type="EXPENSE",
            category_main="Groceries",
        ),
    ]
    df = _make_df(rows)
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)
        count_bs_first = con.execute(
            "SELECT COUNT(*) FROM dash_balance_sheet"
        ).fetchone()[0]

        build(df, con)  # second call
        count_bs_second = con.execute(
            "SELECT COUNT(*) FROM dash_balance_sheet"
        ).fetchone()[0]
        count_cat_second = con.execute(
            "SELECT COUNT(*) FROM dash_balance_sheet_categories"
        ).fetchone()[0]

    assert count_bs_second == count_bs_first == 1
    # Groceries category should appear exactly once for year 2024
    assert count_cat_second >= 1
