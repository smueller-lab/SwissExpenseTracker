"""Tests for dash_budget SQL queries: create_dash_budget_table, upsert_dash_budget, get_dash_budget."""

from __future__ import annotations

import sqlite3

from pathlib import Path

import pandas as pd
import pytest

from swiss_exp_tracker.db.sql import transactions

# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _make_budget_row(**overrides: object) -> dict[str, object]:
    """Return a valid dash_budget insert dict; each test states only what it varies."""
    base: dict[str, object] = {
        "category": "Groceries",
        "year": 2024,
        "budget_chf": 500.0,
        "updated_at": "2024-01-01T00:00:00",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def budget_db(tmp_path: Path) -> Path:
    """Temp SQLite DB with only the dash_budget table created."""
    db_path = tmp_path / "budget_test.db"
    with sqlite3.connect(db_path) as db:
        transactions.create_dash_budget_table(db)
        db.commit()
    return db_path


@pytest.fixture()
def pipeline_db(tmp_path: Path) -> Path:
    """Full fixture DB (transactions_use populated) ready for run_dashboard_pipeline."""
    from tests.fixtures.db_builder import build_fixture_db
    from tests.fixtures.seed_data import make_seed_groceries
    from tests.fixtures.seed_data import make_seed_transactions

    db_path = tmp_path / "transactions.db"
    return build_fixture_db(db_path, make_seed_transactions(), make_seed_groceries())


# ---------------------------------------------------------------------------
# create_dash_budget_table
# ---------------------------------------------------------------------------


def test_create_dash_budget_table_idempotent(tmp_path: Path) -> None:
    """create_dash_budget_table can be called twice without error (IF NOT EXISTS)."""
    db_path = tmp_path / "idempotent.db"
    with sqlite3.connect(db_path) as db:
        transactions.create_dash_budget_table(db)
        transactions.create_dash_budget_table(db)
        db.commit()
        rows = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='dash_budget'"
        ).fetchall()
    assert len(rows) == 1


def test_create_dash_budget_table_schema(budget_db: Path) -> None:
    """dash_budget has exactly the four expected columns."""
    with sqlite3.connect(budget_db) as db:
        cols = {
            row[1] for row in db.execute("PRAGMA table_info(dash_budget)").fetchall()
        }
    assert cols == {"category", "year", "budget_chf", "updated_at"}


# ---------------------------------------------------------------------------
# upsert_dash_budget — insert
# ---------------------------------------------------------------------------


def test_upsert_dash_budget_inserts_new_row(budget_db: Path) -> None:
    """upsert_dash_budget creates one row for a new (year, category) combination."""
    with sqlite3.connect(budget_db) as db:
        transactions.upsert_dash_budget(db, **_make_budget_row())
        db.commit()
        count = db.execute("SELECT COUNT(*) FROM dash_budget").fetchone()[0]
    assert count == 1


def test_upsert_dash_budget_distinct_categories_same_year(budget_db: Path) -> None:
    """Two rows with the same year but different categories are stored independently."""
    with sqlite3.connect(budget_db) as db:
        transactions.upsert_dash_budget(
            db, **_make_budget_row(category="Groceries", budget_chf=500.0)
        )
        transactions.upsert_dash_budget(
            db, **_make_budget_row(category="Housing", budget_chf=1800.0)
        )
        db.commit()
        count = db.execute("SELECT COUNT(*) FROM dash_budget").fetchone()[0]
    assert count == 2


# ---------------------------------------------------------------------------
# upsert_dash_budget — update (ON CONFLICT)
# ---------------------------------------------------------------------------


def test_upsert_dash_budget_second_call_updates_no_duplicate(budget_db: Path) -> None:
    """Second upsert with the same (year, category) updates budget_chf; row count stays 1."""
    with sqlite3.connect(budget_db) as db:
        transactions.upsert_dash_budget(
            db, **_make_budget_row(budget_chf=500.0, updated_at="2024-01-01T00:00:00")
        )
        transactions.upsert_dash_budget(
            db, **_make_budget_row(budget_chf=600.0, updated_at="2024-01-02T00:00:00")
        )
        db.commit()
        count = db.execute("SELECT COUNT(*) FROM dash_budget").fetchone()[0]
        stored_budget = db.execute("SELECT budget_chf FROM dash_budget").fetchone()[0]
    assert count == 1
    assert stored_budget == pytest.approx(600.0)


def test_upsert_dash_budget_updates_updated_at(budget_db: Path) -> None:
    """Second upsert with the same key overwrites updated_at with the newer timestamp."""
    with sqlite3.connect(budget_db) as db:
        transactions.upsert_dash_budget(
            db, **_make_budget_row(updated_at="2024-01-01T00:00:00")
        )
        transactions.upsert_dash_budget(
            db, **_make_budget_row(updated_at="2024-01-02T00:00:00")
        )
        db.commit()
        stored_at = db.execute("SELECT updated_at FROM dash_budget").fetchone()[0]
    assert stored_at == "2024-01-02T00:00:00"


# ---------------------------------------------------------------------------
# get_dash_budget
# ---------------------------------------------------------------------------


def test_get_dash_budget_empty_when_no_rows(budget_db: Path) -> None:
    """get_dash_budget returns an empty DataFrame when no rows have been inserted."""
    with sqlite3.connect(budget_db) as db:
        df = pd.read_sql(transactions.get_dash_budget.sql, db)
    assert df.empty


def test_get_dash_budget_returns_expected_columns(budget_db: Path) -> None:
    """Result has exactly the four expected column names."""
    with sqlite3.connect(budget_db) as db:
        df = pd.read_sql(transactions.get_dash_budget.sql, db)
    assert set(df.columns) == {"category", "year", "budget_chf", "updated_at"}


def test_get_dash_budget_returns_all_rows(budget_db: Path) -> None:
    """get_dash_budget returns every inserted row."""
    with sqlite3.connect(budget_db) as db:
        transactions.upsert_dash_budget(
            db, **_make_budget_row(category="Groceries", budget_chf=500.0)
        )
        transactions.upsert_dash_budget(
            db, **_make_budget_row(category="Housing", budget_chf=1800.0)
        )
        db.commit()
        df = pd.read_sql(transactions.get_dash_budget.sql, db)
    assert len(df) == 2
    assert set(df["category"]) == {"Groceries", "Housing"}


def test_get_dash_budget_values_match_upserted(budget_db: Path) -> None:
    """Values retrieved match the values passed to upsert_dash_budget."""
    row = _make_budget_row(category="Transport", year=2023, budget_chf=150.0)
    with sqlite3.connect(budget_db) as db:
        transactions.upsert_dash_budget(db, **row)
        db.commit()
        df = pd.read_sql(transactions.get_dash_budget.sql, db)
    assert df.iloc[0]["category"] == "Transport"
    assert int(df.iloc[0]["year"]) == 2023
    assert df.iloc[0]["budget_chf"] == pytest.approx(150.0)


# ---------------------------------------------------------------------------
# Pipeline survival
# ---------------------------------------------------------------------------


def test_dash_budget_survives_run_dashboard_pipeline(pipeline_db: Path) -> None:
    """A dash_budget row inserted before run_dashboard_pipeline is still present after."""
    from swiss_exp_tracker.pipeline_dash.pipeline import run_dashboard_pipeline

    row = _make_budget_row(category="Groceries", year=2024, budget_chf=500.0)
    with sqlite3.connect(pipeline_db) as db:
        transactions.create_dash_budget_table(db)
        transactions.upsert_dash_budget(db, **row)
        db.commit()

    run_dashboard_pipeline(db_path=pipeline_db)

    with sqlite3.connect(pipeline_db) as db:
        count = db.execute("SELECT COUNT(*) FROM dash_budget").fetchone()[0]
        stored = db.execute(
            "SELECT budget_chf FROM dash_budget WHERE category='Groceries' AND year=2024"
        ).fetchone()

    assert count == 1
    assert stored is not None
    assert stored[0] == pytest.approx(500.0)
