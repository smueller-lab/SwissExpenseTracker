"""Shared fixtures for tests/test_db/.

DB isolation pattern mirrors tests/test_pipeline_groceries/conftest.py exactly.
"""

from __future__ import annotations

import sqlite3

from pathlib import Path

import pytest

from swiss_exp_tracker.db.sql import positions as positions_sql
from swiss_exp_tracker.db.sql import transactions


def _create_transactions_schema(db: sqlite3.Connection) -> None:
    """Create the full transactions pipeline schema in db."""
    transactions.create_ingested_files_table(db)
    transactions.create_transactions_lnd_table(db)
    transactions.create_transactions_raw_table(db)
    transactions.create_transactions_rfn_table(db)
    transactions.create_api_usage_table(db)
    transactions.create_idx_ingested_files_source(db)
    transactions.create_idx_landing_file_processed(db)
    transactions.create_idx_raw_source_processed(db)
    transactions.create_idx_refined_enrichment_status(db)
    db.commit()


def _create_positions_schema(db: sqlite3.Connection) -> None:
    """Create the full positions pipeline schema in db."""
    positions_sql.create_ingested_files_pos_table(db)
    positions_sql.create_positions_lnd_table(db)
    positions_sql.create_positions_raw_table(db)
    positions_sql.create_positions_rfn_table(db)
    positions_sql.create_positions_use_table(db)
    positions_sql.create_idx_pos_rfn_date(db)
    positions_sql.create_idx_pos_rfn_symbol(db)
    positions_sql.create_idx_pos_lnd_processed(db)
    positions_sql.create_idx_pos_raw_processed(db)
    db.commit()


@pytest.fixture()
def tmp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Temp SQLite DB with full transactions schema; patches pipeline DB path."""
    db_path = tmp_path / "test.db"
    with sqlite3.connect(db_path) as db:
        _create_transactions_schema(db)

    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_ingestion.db.INGESTION_DB_PATH", db_path
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_ingestion.config.INGESTION_DB_PATH", db_path
    )
    return db_path


@pytest.fixture()
def tmp_positions_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Temp SQLite DB with full positions schema; patches pipeline positions DB path."""
    db_path = tmp_path / "positions_test.db"
    with sqlite3.connect(db_path) as db:
        _create_positions_schema(db)

    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_ingestion.config.POSITIONS_DB_PATH", db_path
    )
    return db_path
