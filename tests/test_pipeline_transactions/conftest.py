from __future__ import annotations

import sqlite3

from pathlib import Path

import pytest


def _create_schema(db: sqlite3.Connection) -> None:
    """Create the full transaction pipeline schema in the given DB connection."""
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS ingested_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            file_hash TEXT NOT NULL,
            source_type TEXT NOT NULL,
            ingested_at TEXT NOT NULL,
            record_count INTEGER NOT NULL,
            status TEXT NOT NULL,
            UNIQUE(filename, file_hash, source_type)
        );
        CREATE TABLE IF NOT EXISTS transactions_lnd (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            source_type TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            processed INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (file_id) REFERENCES ingested_files(id)
        );
        CREATE TABLE IF NOT EXISTS transactions_raw (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            landing_id INTEGER NOT NULL,
            source_type TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            source_file TEXT NOT NULL,
            created_at TEXT NOT NULL,
            processed INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (landing_id) REFERENCES transactions_lnd(id)
        );
        CREATE TABLE IF NOT EXISTS transactions_rfn (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_id INTEGER NOT NULL,
            source_type TEXT NOT NULL,
            date TEXT,
            amount REAL NOT NULL,
            transaction_type TEXT NOT NULL,
            booking_text TEXT,
            merchant_normalized TEXT,
            is_person INTEGER NOT NULL DEFAULT 0,
            currency TEXT,
            reference TEXT,
            enrichment_status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            balance_chf REAL,
            FOREIGN KEY (raw_id) REFERENCES transactions_raw(id)
        );
        CREATE TABLE IF NOT EXISTS api_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL,
            period TEXT NOT NULL,
            used INTEGER NOT NULL DEFAULT 0,
            credit_limit INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            UNIQUE(provider, period)
        );
        CREATE INDEX IF NOT EXISTS idx_ingested_files_source
            ON ingested_files(source_type);
        CREATE INDEX IF NOT EXISTS idx_landing_file_processed
            ON transactions_lnd(file_id, processed);
        CREATE INDEX IF NOT EXISTS idx_raw_source_processed
            ON transactions_raw(source_type, processed);
        CREATE INDEX IF NOT EXISTS idx_refined_enrichment_status
            ON transactions_rfn(enrichment_status);
        """
    )
    db.commit()


@pytest.fixture()
def tmp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Temp SQLite DB with transaction pipeline schema; patches pipeline DB path."""
    db_path = tmp_path / "test.db"
    with sqlite3.connect(db_path) as db:
        _create_schema(db)

    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_ingestion.db.INGESTION_DB_PATH", db_path
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_ingestion.config.INGESTION_DB_PATH", db_path
    )
    return db_path
