"""Shared fixtures and helpers for the agentic pipeline test suite."""

from __future__ import annotations

import sqlite3

from pathlib import Path

import pytest

from swiss_exp_tracker.db.sql import agentic
from swiss_exp_tracker.db.sql import transactions

# ---------------------------------------------------------------------------
# Schema helper
# ---------------------------------------------------------------------------


def _create_agentic_schema(db: sqlite3.Connection) -> None:
    """Create all tables needed for agentic pipeline tests."""
    # Transaction pipeline tables
    transactions.create_ingested_files_table(db)
    transactions.create_transactions_lnd_table(db)
    transactions.create_transactions_raw_table(db)
    transactions.create_transactions_rfn_table(db)
    transactions.create_api_usage_table(db)
    # Agentic tables
    agentic.create_merchant_metadata_raw_table(db)
    agentic.create_merchant_metadata_rfn_table(db)
    agentic.create_transactions_use_table(db)
    db.commit()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Temp SQLite DB with full agentic schema; patches pipeline DB path."""
    db_path = tmp_path / "test.db"
    with sqlite3.connect(db_path) as db:
        _create_agentic_schema(db)

    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_ingestion.db.INGESTION_DB_PATH", db_path
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_ingestion.config.INGESTION_DB_PATH", db_path
    )
    return db_path


@pytest.fixture()
def tmp_chroma(tmp_path: Path) -> str:
    """Return a temp directory path string for use as a MerchantStore chroma DB."""
    chroma_dir = tmp_path / "chroma"
    chroma_dir.mkdir(parents=True, exist_ok=True)
    return str(chroma_dir)
