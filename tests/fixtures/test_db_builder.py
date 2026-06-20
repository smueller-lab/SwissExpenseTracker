"""Tests for the fixture infrastructure (models, db_builder, seed_data).

Verifies that build_fixture_db creates the expected schema and row counts,
that UseTransactionFixture / GroceryUseFixture behave correctly, that the seed
dataset satisfies the invariants required by the dashboard pipeline, and that
running run_dashboard_pipeline on the fixture DB produces all non-empty dash
tables.
"""

from __future__ import annotations

import sqlite3

from pathlib import Path

import pytest

from pydantic import ValidationError

from swiss_exp_tracker.pipeline_dash.config import BALANCE_SHEET_MAJOR_CATEGORIES
from tests.fixtures.db_builder import build_fixture_db
from tests.fixtures.models import GroceryUseFixture
from tests.fixtures.models import UseTransactionFixture
from tests.fixtures.seed_data import make_seed_groceries
from tests.fixtures.seed_data import make_seed_transactions

# ---------------------------------------------------------------------------
# Module-level seed data (computed once)
# ---------------------------------------------------------------------------

_SEED_TRANSACTIONS = make_seed_transactions()
_SEED_GROCERIES = make_seed_groceries()

# Tables asserted to be non-empty after run_dashboard_pipeline (must match
# the set of tables that the seed data is designed to populate).
_DASH_TABLES = [
    "dash_balance",
    "dash_groceries",
    "dash_food",
    "dash_cat_main",
    "dash_stats",
    "dash_top_category",
    "dash_top_expenses",
    "dash_net_balance_month",
    "dash_vacation",
    "dash_transport",
    "dash_transport_heatmap",
    "dash_sport",
    "dash_sport_activities",
]

# Expected column names for each table created by build_fixture_db.
_TRANSACTIONS_USE_COLUMNS = {
    "id",
    "transaction_id",
    "source_type",
    "date",
    "amount",
    "transaction_type",
    "currency",
    "reference",
    "merchant",
    "category_main",
    "category_second",
    "city",
    "balance_chf",
}

_TRANSACTIONS_RFN_COLUMNS = {
    "id",
    "raw_id",
    "source_type",
    "date",
    "amount",
    "transaction_type",
    "booking_text",
    "merchant_normalized",
    "is_person",
    "currency",
    "reference",
    "enrichment_status",
    "created_at",
    "balance_chf",
}

_GROCERIES_USE_COLUMNS = {
    "id",
    "rfn_id",
    "date",
    "time",
    "location",
    "article",
    "unit",
    "quantity",
    "price_chf",
    "discount_chf",
    "category_main",
    "category_detail",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_valid_use_row(**overrides: object) -> dict[str, object]:
    """Return a valid UseTransactionFixture init dict with optional overrides."""
    base: dict[str, object] = {
        "transaction_id": 1,
        "source_type": "ZKB_DEBIT",
        "date": "2024-06-15",
        "amount": 100.0,
        "transaction_type": "EXPENSE",
        "currency": "CHF",
        "reference": "REF-00001",
        "merchant": "Migros",
        "category_main": "Groceries",
        "category_second": None,
        "city": "Zurich",
        "balance_chf": 5000.0,
    }
    base.update(overrides)
    return base


def _make_valid_grocery_row(**overrides: object) -> dict[str, object]:
    """Return a valid GroceryUseFixture init dict with optional overrides."""
    base: dict[str, object] = {
        "rfn_id": 1,
        "date": "2024-06-15",
        "time": "10:00:00",
        "location": "Coop Zürich",
        "article": "Apples",
        "unit": "kg",
        "quantity": 1.5,
        "price_chf": 2.50,
        "discount_chf": 0.0,
        "category_main": "Fresh Produce",
        "category_detail": "Fruit",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def fixture_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create a fixture DB with seed data and return its path (once per module)."""
    tmp = tmp_path_factory.mktemp("fixture_infra")
    dest = tmp / "transactions.db"
    return build_fixture_db(dest, _SEED_TRANSACTIONS, _SEED_GROCERIES)


@pytest.fixture(scope="module")
def pipeline_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create a fixture DB, run the dashboard pipeline, and return the path."""
    from swiss_exp_tracker.pipeline_dash.pipeline import run_dashboard_pipeline

    tmp = tmp_path_factory.mktemp("pipeline_infra")
    dest = tmp / "transactions.db"
    build_fixture_db(dest, _SEED_TRANSACTIONS, _SEED_GROCERIES)
    run_dashboard_pipeline(db_path=dest)
    return dest


# ---------------------------------------------------------------------------
# build_fixture_db — schema
# ---------------------------------------------------------------------------


def test_build_fixture_db_creates_transactions_use_table(fixture_db: Path) -> None:
    """build_fixture_db creates transactions_use with all expected columns."""
    with sqlite3.connect(fixture_db) as con:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert "transactions_use" in tables


def test_build_fixture_db_creates_transactions_rfn_table(fixture_db: Path) -> None:
    """build_fixture_db creates transactions_rfn (initially empty)."""
    with sqlite3.connect(fixture_db) as con:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert "transactions_rfn" in tables


def test_build_fixture_db_creates_groceries_use_table(fixture_db: Path) -> None:
    """build_fixture_db creates groceries_use with all expected columns."""
    with sqlite3.connect(fixture_db) as con:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert "groceries_use" in tables


def test_transactions_use_has_expected_columns(fixture_db: Path) -> None:
    """transactions_use table columns match the expected schema."""
    with sqlite3.connect(fixture_db) as con:
        actual_cols = {
            row[1]
            for row in con.execute("PRAGMA table_info(transactions_use)").fetchall()
        }
    missing = _TRANSACTIONS_USE_COLUMNS - actual_cols
    assert not missing, f"transactions_use missing columns: {missing}"


def test_transactions_rfn_has_expected_columns(fixture_db: Path) -> None:
    """transactions_rfn table columns match the expected schema."""
    with sqlite3.connect(fixture_db) as con:
        actual_cols = {
            row[1]
            for row in con.execute("PRAGMA table_info(transactions_rfn)").fetchall()
        }
    missing = _TRANSACTIONS_RFN_COLUMNS - actual_cols
    assert not missing, f"transactions_rfn missing columns: {missing}"


def test_groceries_use_has_expected_columns(fixture_db: Path) -> None:
    """groceries_use table columns match the expected schema."""
    with sqlite3.connect(fixture_db) as con:
        actual_cols = {
            row[1] for row in con.execute("PRAGMA table_info(groceries_use)").fetchall()
        }
    missing = _GROCERIES_USE_COLUMNS - actual_cols
    assert not missing, f"groceries_use missing columns: {missing}"


# ---------------------------------------------------------------------------
# build_fixture_db — row counts
# ---------------------------------------------------------------------------


def test_transactions_use_row_count_equals_seed(fixture_db: Path) -> None:
    """transactions_use row count equals len(make_seed_transactions())."""
    expected = len(_SEED_TRANSACTIONS)
    with sqlite3.connect(fixture_db) as con:
        actual = con.execute("SELECT COUNT(*) FROM transactions_use").fetchone()[0]
    assert actual == expected


def test_transactions_rfn_is_empty(fixture_db: Path) -> None:
    """transactions_rfn is empty (build_fixture_db does not insert rfn rows)."""
    with sqlite3.connect(fixture_db) as con:
        count = con.execute("SELECT COUNT(*) FROM transactions_rfn").fetchone()[0]
    assert count == 0


def test_groceries_use_row_count_equals_seed(fixture_db: Path) -> None:
    """groceries_use row count equals len(make_seed_groceries())."""
    expected = len(_SEED_GROCERIES)
    with sqlite3.connect(fixture_db) as con:
        actual = con.execute("SELECT COUNT(*) FROM groceries_use").fetchone()[0]
    assert actual == expected


# ---------------------------------------------------------------------------
# UseTransactionFixture — model behaviour
# ---------------------------------------------------------------------------


def test_use_transaction_fixture_valid_row_parses() -> None:
    """A valid UseTransactionFixture row parses without error."""
    row = UseTransactionFixture.model_validate(_make_valid_use_row())
    assert row.transaction_id == 1
    assert row.amount == pytest.approx(100.0)
    assert row.transaction_type == "EXPENSE"


def test_use_transaction_fixture_nullable_date_accepts_none() -> None:
    """date field accepts None without raising."""
    row = UseTransactionFixture.model_validate(_make_valid_use_row(date=None))
    assert row.date is None


def test_use_transaction_fixture_nullable_date_accepts_empty_string() -> None:
    """date field coerces empty string to None."""
    row = UseTransactionFixture.model_validate(_make_valid_use_row(date=""))
    assert row.date is None


def test_use_transaction_fixture_nullable_currency_accepts_none() -> None:
    """currency field accepts None without raising."""
    row = UseTransactionFixture.model_validate(_make_valid_use_row(currency=None))
    assert row.currency is None


def test_use_transaction_fixture_nullable_reference_accepts_none() -> None:
    """reference field accepts None without raising."""
    row = UseTransactionFixture.model_validate(_make_valid_use_row(reference=None))
    assert row.reference is None


def test_use_transaction_fixture_nullable_category_second_accepts_none() -> None:
    """category_second field accepts None without raising."""
    row = UseTransactionFixture.model_validate(
        _make_valid_use_row(category_second=None)
    )
    assert row.category_second is None


def test_use_transaction_fixture_nullable_city_accepts_none() -> None:
    """city field accepts None without raising."""
    row = UseTransactionFixture.model_validate(_make_valid_use_row(city=None))
    assert row.city is None


def test_use_transaction_fixture_nullable_balance_chf_accepts_none() -> None:
    """balance_chf field accepts None without raising."""
    row = UseTransactionFixture.model_validate(_make_valid_use_row(balance_chf=None))
    assert row.balance_chf is None


def test_use_transaction_fixture_nullable_balance_chf_accepts_empty_string() -> None:
    """balance_chf field coerces empty string to None."""
    row = UseTransactionFixture.model_validate(_make_valid_use_row(balance_chf=""))
    assert row.balance_chf is None


def test_use_transaction_fixture_to_insert_dict_keys_match_insert_params() -> None:
    """to_insert_dict() keys match the named params of insert_transactions_use."""
    expected_keys = {
        "transaction_id",
        "source_type",
        "date",
        "amount",
        "transaction_type",
        "currency",
        "reference",
        "merchant",
        "category_main",
        "category_second",
        "city",
        "balance_chf",
    }
    row = UseTransactionFixture.model_validate(_make_valid_use_row())
    assert set(row.to_insert_dict().keys()) == expected_keys


# ---------------------------------------------------------------------------
# GroceryUseFixture — model behaviour
# ---------------------------------------------------------------------------


def test_grocery_use_fixture_valid_row_parses() -> None:
    """A valid GroceryUseFixture row parses without error."""
    row = GroceryUseFixture.model_validate(_make_valid_grocery_row())
    assert row.rfn_id == 1
    assert row.price_chf == pytest.approx(2.50)


def test_grocery_use_fixture_empty_date_raises() -> None:
    """GroceryUseFixture raises ValidationError when date is empty string."""
    with pytest.raises(ValidationError):
        GroceryUseFixture.model_validate(_make_valid_grocery_row(date=""))


def test_grocery_use_fixture_none_date_raises() -> None:
    """GroceryUseFixture raises ValidationError when date is None."""
    with pytest.raises(ValidationError):
        GroceryUseFixture.model_validate(_make_valid_grocery_row(date=None))


def test_grocery_use_fixture_to_insert_dict_keys_match_insert_params() -> None:
    """to_insert_dict() keys match the named params of insert_groceries_use."""
    expected_keys = {
        "rfn_id",
        "date",
        "time",
        "location",
        "article",
        "unit",
        "quantity",
        "price_chf",
        "discount_chf",
        "category_main",
        "category_detail",
    }
    row = GroceryUseFixture.model_validate(_make_valid_grocery_row())
    assert set(row.to_insert_dict().keys()) == expected_keys


# ---------------------------------------------------------------------------
# Seed dataset invariants
# ---------------------------------------------------------------------------


def test_seed_transactions_spans_at_least_three_years() -> None:
    """make_seed_transactions() covers at least 3 distinct calendar years."""
    years = {int(r.date[:4]) for r in _SEED_TRANSACTIONS if r.date}
    assert len(years) >= 3, f"Seed data only spans years: {sorted(years)}"


def test_seed_transactions_all_references_unique() -> None:
    """Every reference value in the seed dataset is unique (no duplicates)."""
    refs = [r.reference for r in _SEED_TRANSACTIONS if r.reference is not None]
    assert len(refs) == len(set(refs)), "Duplicate reference values found in seed data"


def test_seed_transactions_zkb_debit_rows_have_balance_chf() -> None:
    """All ZKB_DEBIT rows in the seed dataset have a non-null balance_chf."""
    zkb_rows = [r for r in _SEED_TRANSACTIONS if r.source_type == "ZKB_DEBIT"]
    assert zkb_rows, "No ZKB_DEBIT rows found in seed data"
    for row in zkb_rows:
        assert (
            row.balance_chf is not None
        ), f"ZKB_DEBIT row {row.transaction_id} has null balance_chf"


def test_seed_transactions_has_salary_income_rows() -> None:
    """Seed data contains Salary/INCOME rows (required for the salary gate)."""
    salary_rows = [
        r
        for r in _SEED_TRANSACTIONS
        if r.category_main == "Salary" and r.transaction_type == "INCOME"
    ]
    assert len(salary_rows) >= 1, "No Salary INCOME rows found in seed data"


def test_seed_transactions_salary_row_per_month_in_range() -> None:
    """Every month covered by the seed data has at least one Salary INCOME row."""
    all_months = {r.date[:7] for r in _SEED_TRANSACTIONS if r.date}
    salary_months = {
        r.date[:7]
        for r in _SEED_TRANSACTIONS
        if r.date and r.category_main == "Salary" and r.transaction_type == "INCOME"
    }
    missing = all_months - salary_months
    assert not missing, f"Months without a Salary row: {sorted(missing)}"


def test_seed_transactions_covers_major_expense_categories() -> None:
    """Seed data covers the key BALANCE_SHEET_MAJOR_CATEGORIES that are explicitly seeded."""
    # Healthcare is intentionally absent from the seed; all others must be present.
    seeded_category_mains = {r.category_main for r in _SEED_TRANSACTIONS}
    expected_present = {
        "Groceries",
        "Housing",
        "Car",
        "Transport",
        "Travel",
        "Sport",
        "Restaurant",
        "Retail",
    }
    missing = expected_present - seeded_category_mains
    assert not missing, f"Expected categories absent from seed data: {missing}"


# ---------------------------------------------------------------------------
# After run_dashboard_pipeline — all dash tables non-empty
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("table", _DASH_TABLES)
def test_dash_table_non_empty_after_pipeline(table: str, pipeline_db: Path) -> None:
    """Every DASH_TABLES entry is non-empty after running run_dashboard_pipeline."""
    with sqlite3.connect(pipeline_db) as con:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    assert count > 0, f"{table} is empty after run_dashboard_pipeline on fixture DB"


def test_all_major_category_labels_appear_in_pivot_after_pipeline(
    pipeline_db: Path,
) -> None:
    """Every BALANCE_SHEET_MAJOR_CATEGORIES label appears as a column in pdf_CategorySpendPivot."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    loader = DataLoader(db_path=pipeline_db)
    pivot_cols = set(loader.pdf_CategorySpendPivot.columns)
    expected_labels = [label for label, _, _ in BALANCE_SHEET_MAJOR_CATEGORIES]
    missing = set(expected_labels) - pivot_cols
    assert not missing, f"Labels missing from pdf_CategorySpendPivot columns: {missing}"
