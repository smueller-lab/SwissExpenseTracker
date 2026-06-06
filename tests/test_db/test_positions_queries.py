"""Tests for named aiosql queries in positions.sql."""

from __future__ import annotations

import json
import sqlite3

from pathlib import Path

from swiss_exp_tracker.db.sql import positions as pos_sql

# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _make_file_row(**overrides: object) -> dict[str, object]:
    """Return a valid ingested_files_pos insert dict."""
    base: dict[str, object] = {
        "filename": "positions_2026.xlsx",
        "file_hash": "def456",
        "ingested_at": "2026-01-01T10:00:00",
        "record_count": 3,
        "status": "done",
    }
    base.update(overrides)
    return base


def _make_lnd_row(**overrides: object) -> dict[str, object]:
    """Return a valid positions_lnd insert dict."""
    base: dict[str, object] = {
        "file_id": 1,
        "raw_json": json.dumps({"symbol": "AAPL", "price": "180.00"}),
        "created_at": "2026-01-01T10:00:00",
    }
    base.update(overrides)
    return base


def _make_rfn_row(**overrides: object) -> dict[str, object]:
    """Return a valid positions_rfn insert dict."""
    base: dict[str, object] = {
        "snapshot_date": "2026-01-15",
        "account_id": "ACC-001",
        "asset_class": "Equity",
        "symbol": "AAPL",
        "name": "Apple Inc.",
        "quantity": 10.0,
        "unit_cost": 150.0,
        "price": 180.0,
        "currency": "USD",
        "total_value": 1800.0,
        "total_value_chf": 1620.0,
        "pnl_nominal_chf": 270.0,
        "pnl_pct_chf": 20.0,
        "position_weight_pct": 25.0,
        "source_file": "positions_2026.xlsx",
        "created_at": "2026-01-01T10:00:00",
    }
    base.update(overrides)
    return base


def _insert_file(db: sqlite3.Connection, **overrides: object) -> None:
    """Insert one ingested_files_pos row."""
    pos_sql.insert_ingested_file_pos(db, **_make_file_row(**overrides))  # type: ignore[arg-type]
    db.commit()


def _insert_lnd(db: sqlite3.Connection, **overrides: object) -> int:
    """Insert one positions_lnd row and return its id."""
    pos_sql.insert_positions_lnd(db, [_make_lnd_row(**overrides)])
    db.commit()
    return db.execute(
        "SELECT id FROM positions_lnd ORDER BY id DESC LIMIT 1"
    ).fetchone()[0]


def _insert_rfn(db: sqlite3.Connection, **overrides: object) -> None:
    """Insert one positions_rfn row (OR IGNORE)."""
    pos_sql.insert_positions_rfn_ignore(db, **_make_rfn_row(**overrides))  # type: ignore[arg-type]
    db.commit()


# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------


def test_create_positions_schema_creates_all_tables(tmp_positions_db: Path) -> None:
    """DDL functions create all expected positions tables."""
    with sqlite3.connect(tmp_positions_db) as db:
        tables = {
            r[0]
            for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

    expected = {
        "ingested_files_pos",
        "positions_lnd",
        "positions_raw",
        "positions_rfn",
        "positions_use",
    }
    assert expected <= tables


# ---------------------------------------------------------------------------
# insert_ingested_file_pos / get_latest_positions_file_id
# ---------------------------------------------------------------------------


def test_insert_and_fetch_ingested_file_pos_idempotent(tmp_positions_db: Path) -> None:
    """OR IGNORE means inserting the same positions file twice leaves exactly one row."""
    with sqlite3.connect(tmp_positions_db) as db:
        _insert_file(db)
        _insert_file(db)
        count = db.execute("SELECT COUNT(*) FROM ingested_files_pos").fetchone()[0]

    assert count == 1


def test_get_latest_positions_file_id_correct(tmp_positions_db: Path) -> None:
    """get_latest_positions_file_id returns a plain integer id."""
    with sqlite3.connect(tmp_positions_db) as db:
        _insert_file(db)
        result = pos_sql.get_latest_positions_file_id(
            db, filename="positions_2026.xlsx"
        )

    assert isinstance(result, int)
    assert result >= 1


# ---------------------------------------------------------------------------
# insert_positions_lnd
# ---------------------------------------------------------------------------


def test_insert_positions_lnd_batch(tmp_positions_db: Path) -> None:
    """Batch-inserting 2 positions_lnd rows yields COUNT = 2."""
    with sqlite3.connect(tmp_positions_db) as db:
        _insert_file(db)
        file_id = pos_sql.get_latest_positions_file_id(
            db, filename="positions_2026.xlsx"
        )
        rows = [_make_lnd_row(file_id=file_id) for _ in range(2)]
        pos_sql.insert_positions_lnd(db, rows)
        db.commit()

        count = db.execute("SELECT COUNT(*) FROM positions_lnd").fetchone()[0]

    assert count == 2


# ---------------------------------------------------------------------------
# mark_positions_lnd_processed
# ---------------------------------------------------------------------------


def test_insert_positions_lnd_and_mark_processed(tmp_positions_db: Path) -> None:
    """mark_positions_lnd_processed sets processed = 1."""
    with sqlite3.connect(tmp_positions_db) as db:
        _insert_file(db)
        file_id = pos_sql.get_latest_positions_file_id(
            db, filename="positions_2026.xlsx"
        )
        lnd_id = _insert_lnd(db, file_id=file_id)
        pos_sql.mark_positions_lnd_processed(db, landing_id=lnd_id)
        db.commit()

        processed = db.execute(
            "SELECT processed FROM positions_lnd WHERE id = ?", (lnd_id,)
        ).fetchone()[0]

    assert processed == 1


# ---------------------------------------------------------------------------
# insert_positions_rfn_ignore (deduplication)
# ---------------------------------------------------------------------------


def test_insert_positions_rfn_ignore_deduplication(tmp_positions_db: Path) -> None:
    """INSERT OR IGNORE on same unique key (snapshot_date, symbol, account_id) leaves one row."""
    with sqlite3.connect(tmp_positions_db) as db:
        _insert_rfn(db)
        _insert_rfn(db)  # same unique key → ignored

        count = db.execute("SELECT COUNT(*) FROM positions_rfn").fetchone()[0]

    assert count == 1


# ---------------------------------------------------------------------------
# delete_positions_use / populate_positions_use
# ---------------------------------------------------------------------------


def test_delete_and_populate_positions_use(tmp_positions_db: Path) -> None:
    """populate_positions_use fills positions_use from rfn; delete empties it."""
    with sqlite3.connect(tmp_positions_db) as db:
        _insert_rfn(db)

        pos_sql.populate_positions_use(db)
        db.commit()

        count_after_populate = db.execute(
            "SELECT COUNT(*) FROM positions_use"
        ).fetchone()[0]
        assert count_after_populate > 0

        pos_sql.delete_positions_use(db)
        db.commit()

        count_after_delete = db.execute(
            "SELECT COUNT(*) FROM positions_use"
        ).fetchone()[0]

    assert count_after_delete == 0
