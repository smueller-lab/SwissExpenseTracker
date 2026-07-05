"""Tests for pipeline_dash/tables/groceries_detail.py.

Focus: build() must not fail when there is no item-level Cumulus data, since
that data is optional and absent for many accounts. All values are synthetic.
"""

from __future__ import annotations

import sqlite3

from pathlib import Path

import pandas as pd
import pytest

from swiss_exp_tracker.pipeline_dash.tables.groceries_detail import build

_OUTPUT_TABLES = (
    "dash_groceries_cat",
    "dash_groceries_health",
    "dash_groceries_top_articles",
)

_GROCERIES_USE_COLUMNS = (
    "rfn_id",
    "date",
    "article",
    "category_main",
    "price_chf",
)


def _make_item(**overrides: object) -> dict[str, object]:
    """Return a synthetic groceries_use row; each test only states what it varies."""
    base: dict[str, object] = {
        "rfn_id": 1,
        "date": "2026-01-05",
        "article": "Test Article",
        "category_main": "Fresh Produce",
        "price_chf": 3.5,
    }
    base.update(overrides)
    return base


@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    """Empty SQLite DB; build() writes the dash_groceries_* tables via to_sql."""
    db_path = tmp_path / "test.db"
    sqlite3.connect(str(db_path)).close()
    return db_path


def _create_groceries_use(
    con: sqlite3.Connection, rows: list[dict[str, object]]
) -> None:
    """Create groceries_use and insert the given synthetic rows."""
    pd.DataFrame(rows, columns=list(_GROCERIES_USE_COLUMNS)).to_sql(
        "groceries_use", con, if_exists="replace", index=False
    )


def _read_outputs(con: sqlite3.Connection) -> dict[str, pd.DataFrame]:
    """Read all three output tables into a name → DataFrame mapping."""
    return {t: pd.read_sql(f"SELECT * FROM {t}", con) for t in _OUTPUT_TABLES}


def test_build_no_groceries_use_table_writes_empty_tables(tmp_db: Path) -> None:
    """Missing groceries_use table → empty output tables, no exception."""
    with sqlite3.connect(str(tmp_db)) as con:
        build(pd.DataFrame(), con)
        outputs = _read_outputs(con)

    for table, frame in outputs.items():
        assert frame.empty, table


def test_build_empty_groceries_use_writes_empty_tables(tmp_db: Path) -> None:
    """Existing-but-empty groceries_use → empty output tables, no exception."""
    with sqlite3.connect(str(tmp_db)) as con:
        _create_groceries_use(con, [])
        build(pd.DataFrame(), con)
        outputs = _read_outputs(con)

    for table, frame in outputs.items():
        assert frame.empty, table


def test_build_empty_outputs_have_expected_columns(tmp_db: Path) -> None:
    """Empty output tables still expose the columns the loader reads."""
    with sqlite3.connect(str(tmp_db)) as con:
        build(pd.DataFrame(), con)
        outputs = _read_outputs(con)

    assert list(outputs["dash_groceries_health"].columns) == ["Period", "score"]
    assert "category_main" in outputs["dash_groceries_cat"].columns
    assert "article" in outputs["dash_groceries_top_articles"].columns


def test_build_with_items_populates_tables(tmp_db: Path) -> None:
    """With synthetic items present, the normal path still produces rows."""
    rows = [
        _make_item(rfn_id=1, date="2026-01-05", category_main="Fresh Produce"),
        _make_item(rfn_id=2, date="2026-01-06", category_main="Snacks & Sweets"),
    ]
    with sqlite3.connect(str(tmp_db)) as con:
        _create_groceries_use(con, rows)
        build(pd.DataFrame(), con)
        outputs = _read_outputs(con)

    assert not outputs["dash_groceries_cat"].empty
    assert not outputs["dash_groceries_health"].empty
    assert not outputs["dash_groceries_top_articles"].empty
