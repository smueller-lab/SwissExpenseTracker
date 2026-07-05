"""Tests for pipeline_dash/tables/stats.py — focus on Balance_current derivation.

Balance_current must come from any balance-carrying debit source (UBS, ZKB, …),
not only ZKB_DEBIT, and must be None (not a crash) when no such source exists.
All values are synthetic.
"""

from __future__ import annotations

import sqlite3

from pathlib import Path

import pandas as pd
import pytest

from swiss_exp_tracker.pipeline_dash.tables.stats import build


def _make_row(**overrides: object) -> dict[str, object]:
    """Return a synthetic transactions_use-like row; tests vary only what they need."""
    base: dict[str, object] = {
        "date": pd.Timestamp("2026-01-15"),
        "amount": 50.0,
        "transaction_type": "EXPENSE",
        "category_main": "Groceries",
        "source_type": "UBS_DEBIT",
        "balance_chf": 1000.0,
    }
    base.update(overrides)
    return base


@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    """Empty SQLite DB; build() writes dash_stats via to_sql."""
    db_path = tmp_path / "test.db"
    sqlite3.connect(str(db_path)).close()
    return db_path


def _read_balance_current(db_path: Path) -> object:
    """Return the single Balance_current value written to dash_stats."""
    with sqlite3.connect(str(db_path)) as con:
        return pd.read_sql("SELECT * FROM dash_stats", con)["Balance_current"].iloc[0]


def test_balance_current_from_ubs_debit(tmp_db: Path) -> None:
    """The latest UBS_DEBIT balance becomes Balance_current (was None before the fix)."""
    df = pd.DataFrame(
        [
            _make_row(date=pd.Timestamp("2026-01-10"), balance_chf=900.0),
            _make_row(date=pd.Timestamp("2026-02-10"), balance_chf=1234.50),
        ]
    )
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)

    assert _read_balance_current(tmp_db) == pytest.approx(1234.50)


def test_balance_current_sums_latest_per_source(tmp_db: Path) -> None:
    """With two debit accounts, Balance_current is the sum of each one's latest balance."""
    df = pd.DataFrame(
        [
            _make_row(source_type="UBS_DEBIT", balance_chf=1000.0),
            _make_row(source_type="ZKB_DEBIT", balance_chf=250.0),
        ]
    )
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)

    assert _read_balance_current(tmp_db) == pytest.approx(1250.0)


def test_balance_current_none_when_no_balance_source(tmp_db: Path) -> None:
    """A credit-card-only dataset (no running balance) yields NULL, not a crash."""
    df = pd.DataFrame([_make_row(source_type="UBS_CREDIT", balance_chf=None)])
    with sqlite3.connect(str(tmp_db)) as con:
        build(df, con)

    assert _read_balance_current(tmp_db) is None
