"""Tests for swiss_exp_tracker.pipeline_dash.tables.vacation.build()."""

from __future__ import annotations

import sqlite3

import pandas as pd
import pytest

from swiss_exp_tracker.pipeline_dash.tables.vacation import build

# ── factory helpers ───────────────────────────────────────────────────────────


def _make_row(**overrides: object) -> dict[str, object]:
    """Return a valid base dict for a vacation transaction row; each test varies only what it needs."""
    base: dict[str, object] = {
        "transaction_type": "EXPENSE",
        "category_main": "Travel",
        "category_second": "Accommodation",
        "amount": 200.0,
        "date": pd.Timestamp("2024-06-15"),
    }
    base.update(overrides)
    return base


def _make_df(rows: list[dict[str, object]]) -> pd.DataFrame:
    """Build a DataFrame from a list of row dicts produced by _make_row."""
    return pd.DataFrame(rows)


def _mem_con() -> sqlite3.Connection:
    """Return a fresh in-memory SQLite connection."""
    return sqlite3.connect(":memory:")


# ── happy path ────────────────────────────────────────────────────────────────


def test_build_vacation_creates_both_tables() -> None:
    """build() writes both dash_vacation and dash_vacation_transactions."""
    df = _make_df([_make_row()])
    con = _mem_con()
    build(df, con)
    tables = {
        row[0]
        for row in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "dash_vacation" in tables
    assert "dash_vacation_transactions" in tables


def test_build_vacation_grouped_columns() -> None:
    """dash_vacation must have columns Year, category_second, Total."""
    df = _make_df([_make_row()])
    con = _mem_con()
    build(df, con)
    cols = {
        row[1] for row in con.execute("PRAGMA table_info(dash_vacation)").fetchall()
    }
    assert {"Year", "category_second", "Total"} <= cols


def test_build_vacation_transaction_columns() -> None:
    """dash_vacation_transactions must have columns category_second and amount."""
    df = _make_df([_make_row()])
    con = _mem_con()
    build(df, con)
    cols = {
        row[1]
        for row in con.execute(
            "PRAGMA table_info(dash_vacation_transactions)"
        ).fetchall()
    }
    assert {"category_second", "amount"} <= cols


def test_build_vacation_happy_path_aggregation() -> None:
    """Three rows (2 categories, 2 years) produce correct grouped totals in dash_vacation."""
    rows = [
        _make_row(
            category_second="Accommodation",
            amount=200.0,
            date=pd.Timestamp("2024-06-15"),
        ),
        _make_row(
            category_second="Accommodation",
            amount=300.0,
            date=pd.Timestamp("2025-03-10"),
        ),
        _make_row(
            category_second="Transport", amount=150.0, date=pd.Timestamp("2024-08-01")
        ),
    ]
    df = _make_df(rows)
    con = _mem_con()
    build(df, con)

    result = pd.read_sql(
        "SELECT * FROM dash_vacation ORDER BY Year, category_second", con
    )
    assert len(result) == 3  # 2024/Accommodation, 2024/Transport, 2025/Accommodation

    acc_2024 = result[
        (result["Year"] == 2024) & (result["category_second"] == "Accommodation")
    ]
    assert len(acc_2024) == 1
    assert acc_2024.iloc[0]["Total"] == pytest.approx(200.0)

    trans_2024 = result[
        (result["Year"] == 2024) & (result["category_second"] == "Transport")
    ]
    assert len(trans_2024) == 1
    assert trans_2024.iloc[0]["Total"] == pytest.approx(150.0)

    acc_2025 = result[
        (result["Year"] == 2025) & (result["category_second"] == "Accommodation")
    ]
    assert len(acc_2025) == 1
    assert acc_2025.iloc[0]["Total"] == pytest.approx(300.0)


def test_build_vacation_transactions_row_count_matches_valid_input() -> None:
    """dash_vacation_transactions has the same row count as EXPENSE/Travel rows with non-null date."""
    rows = [
        _make_row(category_second="Accommodation", amount=200.0),
        _make_row(category_second="Transport", amount=150.0),
        _make_row(category_second="Food", amount=80.0),
    ]
    df = _make_df(rows)
    con = _mem_con()
    build(df, con)

    count = con.execute("SELECT COUNT(*) FROM dash_vacation_transactions").fetchone()[0]
    assert count == 3


def test_build_vacation_transactions_values() -> None:
    """dash_vacation_transactions preserves category_second and amount from input rows."""
    rows = [
        _make_row(category_second="Accommodation", amount=200.0),
        _make_row(category_second="Transport", amount=150.0),
    ]
    df = _make_df(rows)
    con = _mem_con()
    build(df, con)

    result = pd.read_sql(
        "SELECT * FROM dash_vacation_transactions ORDER BY amount", con
    )
    assert result.iloc[0]["category_second"] == "Transport"
    assert result.iloc[0]["amount"] == pytest.approx(150.0)
    assert result.iloc[1]["category_second"] == "Accommodation"
    assert result.iloc[1]["amount"] == pytest.approx(200.0)


# ── excluded rows ─────────────────────────────────────────────────────────────


def test_build_vacation_excludes_income_rows() -> None:
    """Rows with transaction_type='INCOME' are excluded from both output tables."""
    rows = [
        _make_row(amount=200.0),  # valid EXPENSE/Travel row
        _make_row(transaction_type="INCOME", amount=500.0),
    ]
    df = _make_df(rows)
    con = _mem_con()
    build(df, con)

    count = con.execute("SELECT COUNT(*) FROM dash_vacation_transactions").fetchone()[0]
    assert count == 1

    total = con.execute("SELECT SUM(Total) FROM dash_vacation").fetchone()[0]
    assert total == pytest.approx(200.0)


def test_build_vacation_excludes_non_travel_category_main() -> None:
    """Rows with category_main != 'Travel' are excluded from both output tables."""
    rows = [
        _make_row(amount=200.0),  # valid Travel row
        _make_row(category_main="Food", amount=50.0),
        _make_row(category_main="Transport", amount=100.0),
    ]
    df = _make_df(rows)
    con = _mem_con()
    build(df, con)

    count = con.execute("SELECT COUNT(*) FROM dash_vacation_transactions").fetchone()[0]
    assert count == 1

    total = con.execute("SELECT SUM(Total) FROM dash_vacation").fetchone()[0]
    assert total == pytest.approx(200.0)


def test_build_vacation_excludes_null_date_rows() -> None:
    """Rows with date=None are excluded from both output tables."""
    rows = [
        _make_row(amount=200.0),  # valid row with date
        _make_row(date=None, amount=999.0),
    ]
    df = _make_df(rows)
    con = _mem_con()
    build(df, con)

    count = con.execute("SELECT COUNT(*) FROM dash_vacation_transactions").fetchone()[0]
    assert count == 1

    total = con.execute("SELECT SUM(Total) FROM dash_vacation").fetchone()[0]
    assert total == pytest.approx(200.0)


def test_build_vacation_excludes_all_three_conditions_combined() -> None:
    """All excluded-row types together still yield only the one valid EXPENSE/Travel/non-null-date row."""
    rows = [
        _make_row(amount=100.0),  # only this row should pass
        _make_row(transaction_type="INCOME", amount=500.0),
        _make_row(category_main="Food", amount=50.0),
        _make_row(date=None, amount=999.0),
    ]
    df = _make_df(rows)
    con = _mem_con()
    build(df, con)

    count = con.execute("SELECT COUNT(*) FROM dash_vacation_transactions").fetchone()[0]
    assert count == 1

    agg_count = con.execute("SELECT COUNT(*) FROM dash_vacation").fetchone()[0]
    assert agg_count == 1


# ── idempotency ───────────────────────────────────────────────────────────────


def test_build_vacation_idempotent_row_count() -> None:
    """Calling build() twice produces the same row count (if_exists='replace')."""
    rows = [
        _make_row(category_second="Accommodation", amount=200.0),
        _make_row(category_second="Transport", amount=150.0),
    ]
    df = _make_df(rows)
    con = _mem_con()

    build(df, con)
    first_agg_count = con.execute("SELECT COUNT(*) FROM dash_vacation").fetchone()[0]
    first_txn_count = con.execute(
        "SELECT COUNT(*) FROM dash_vacation_transactions"
    ).fetchone()[0]

    build(df, con)
    second_agg_count = con.execute("SELECT COUNT(*) FROM dash_vacation").fetchone()[0]
    second_txn_count = con.execute(
        "SELECT COUNT(*) FROM dash_vacation_transactions"
    ).fetchone()[0]

    assert second_agg_count == first_agg_count
    assert second_txn_count == first_txn_count


def test_build_vacation_idempotent_totals() -> None:
    """Running build() twice on the same data yields the same totals (no duplication)."""
    rows = [
        _make_row(amount=200.0),
        _make_row(category_second="Transport", amount=150.0),
    ]
    df = _make_df(rows)
    con = _mem_con()

    build(df, con)
    build(df, con)

    total = con.execute("SELECT SUM(Total) FROM dash_vacation").fetchone()[0]
    assert total == pytest.approx(350.0)


# ── edge cases ────────────────────────────────────────────────────────────────


def test_build_vacation_empty_dataframe_produces_empty_tables() -> None:
    """An empty DataFrame must still create both tables (with zero rows).

    The date column must carry datetime64 dtype so .dt.year does not fail on empty input.
    """
    df = pd.DataFrame(
        {
            "transaction_type": pd.Series([], dtype="object"),
            "category_main": pd.Series([], dtype="object"),
            "category_second": pd.Series([], dtype="object"),
            "amount": pd.Series([], dtype="float64"),
            "date": pd.Series([], dtype="datetime64[ns]"),
        }
    )
    con = _mem_con()
    build(df, con)

    agg_count = con.execute("SELECT COUNT(*) FROM dash_vacation").fetchone()[0]
    txn_count = con.execute(
        "SELECT COUNT(*) FROM dash_vacation_transactions"
    ).fetchone()[0]
    assert agg_count == 0
    assert txn_count == 0


def test_build_vacation_year_column_is_integer() -> None:
    """The Year column in dash_vacation must store integer values, not floats."""
    df = _make_df([_make_row(date=pd.Timestamp("2023-11-05"), amount=100.0)])
    con = _mem_con()
    build(df, con)

    year_val = con.execute("SELECT Year FROM dash_vacation").fetchone()[0]
    assert year_val == 2023
    assert isinstance(year_val, int)


def test_build_vacation_multiple_years_multiple_categories() -> None:
    """Aggregation across 2 years x 2 categories yields exactly 4 rows in dash_vacation."""
    rows = [
        _make_row(
            category_second="Accommodation",
            amount=200.0,
            date=pd.Timestamp("2023-07-01"),
        ),
        _make_row(
            category_second="Transport", amount=100.0, date=pd.Timestamp("2023-08-01")
        ),
        _make_row(
            category_second="Accommodation",
            amount=300.0,
            date=pd.Timestamp("2024-06-15"),
        ),
        _make_row(
            category_second="Transport", amount=150.0, date=pd.Timestamp("2024-09-01")
        ),
    ]
    df = _make_df(rows)
    con = _mem_con()
    build(df, con)

    agg_count = con.execute("SELECT COUNT(*) FROM dash_vacation").fetchone()[0]
    assert agg_count == 4

    txn_count = con.execute(
        "SELECT COUNT(*) FROM dash_vacation_transactions"
    ).fetchone()[0]
    assert txn_count == 4
