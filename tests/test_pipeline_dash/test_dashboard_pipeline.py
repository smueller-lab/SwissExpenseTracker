from __future__ import annotations

import shutil
import sqlite3

from pathlib import Path

import pytest

from swiss_exp_tracker.pipeline_dash.pipeline import run_dashboard_pipeline

DASH_TABLES = [
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

EXPECTED_COLUMNS: dict[str, set[str]] = {
    "dash_balance": {"date", "balance_chf"},
    "dash_groceries": {
        "Period",
        "merchant",
        "total_CHF",
        "totalPeriod_CHF",
        "pct",
        "Freq",
    },
    "dash_food": {
        "Period",
        "category_second",
        "total_CHF",
        "totalPeriod_CHF",
        "pct",
        "Freq",
    },
    "dash_cat_main": {"category_main", "amount", "perc", "Year"},
    "dash_stats": {
        "Balance_current",
        "Balance_net_currentYear",
        "Expense_avg_12months",
    },
    "dash_top_category": {"category_main", "amount_MonthLast", "MonthLast"},
    "dash_top_expenses": {"date", "amount", "merchant", "category_main"},
    "dash_net_balance_month": {"Month", "expense", "income", "NetBalance"},
    "dash_vacation": {"Year", "category_second", "Total"},
    "dash_transport": {"Year", "category_transport", "amount"},
    "dash_transport_heatmap": {"Year", "Month_num", "amount", "Month_name"},
    "dash_sport": {"category_sport", "Total", "Freq", "Period"},
    "dash_sport_activities": {"year", "sport", "activity_count"},
}


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    from swiss_exp_tracker.pipeline_ingestion.config import INGESTION_DB_PATH

    dest = tmp_path / "transactions.db"
    shutil.copy(INGESTION_DB_PATH, dest)
    return dest


def test_run_dashboard_pipeline_produces_all_tables(tmp_db: Path) -> None:
    run_dashboard_pipeline(db_path=tmp_db)
    with sqlite3.connect(tmp_db) as con:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    for table in DASH_TABLES:
        assert table in tables, f"Missing table: {table}"


def test_dash_tables_have_expected_columns(tmp_db: Path) -> None:
    run_dashboard_pipeline(db_path=tmp_db)
    with sqlite3.connect(tmp_db) as con:
        for table, expected_cols in EXPECTED_COLUMNS.items():
            actual_cols = {
                row[1] for row in con.execute(f"PRAGMA table_info({table})").fetchall()
            }
            missing = expected_cols - actual_cols
            assert not missing, f"{table} is missing columns: {missing}"


def test_dash_tables_are_non_empty(tmp_db: Path) -> None:
    run_dashboard_pipeline(db_path=tmp_db)
    with sqlite3.connect(tmp_db) as con:
        for table in DASH_TABLES:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert count > 0, f"{table} is empty"


def test_dataloader_initialises_without_error(tmp_db: Path) -> None:
    run_dashboard_pipeline(db_path=tmp_db)
    from swiss_exp_tracker.app.data.loader import DataLoader

    loader = DataLoader(db_path=tmp_db)
    assert loader.pdf_Balance is not None
    assert loader.pdf_Grocery is not None
    assert loader.pdf_Food is not None
    assert loader.pdf_CatMain is not None
    assert loader.pdf_Sport is not None
    assert loader.pdf_Transport is not None
    assert loader.pdf_NetBalanceMonth is not None
    assert loader.pdf_TopExpenses is not None
