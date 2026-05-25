from __future__ import annotations

import sqlite3

from pathlib import Path

import pytest


def _create_schema(db: sqlite3.Connection) -> None:
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
        CREATE TABLE IF NOT EXISTS groceries_lnd (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id      INTEGER NOT NULL,
            source_type  TEXT NOT NULL,
            raw_json     TEXT NOT NULL,
            is_bonus_row INTEGER NOT NULL DEFAULT 0,
            created_at   TEXT NOT NULL,
            processed    INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (file_id) REFERENCES ingested_files(id)
        );
        CREATE TABLE IF NOT EXISTS groceries_raw (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            landing_id   INTEGER NOT NULL,
            source_type  TEXT NOT NULL,
            raw_json     TEXT NOT NULL,
            source_file  TEXT NOT NULL,
            created_at   TEXT NOT NULL,
            processed    INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (landing_id) REFERENCES groceries_lnd(id)
        );
        CREATE TABLE IF NOT EXISTS groceries_rfn (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_id              INTEGER,
            source_type         TEXT NOT NULL,
            date                TEXT NOT NULL,
            time                TEXT NOT NULL,
            location            TEXT NOT NULL,
            article             TEXT NOT NULL,
            article_normalized  TEXT NOT NULL,
            unit                TEXT NOT NULL,
            quantity            REAL NOT NULL,
            price_chf           REAL NOT NULL,
            discount_chf        REAL NOT NULL,
            enrichment_status   TEXT NOT NULL DEFAULT 'pending',
            created_at          TEXT NOT NULL,
            FOREIGN KEY (raw_id) REFERENCES groceries_raw(id)
        );
        CREATE TABLE IF NOT EXISTS groceries_use (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            rfn_id          INTEGER NOT NULL,
            date            TEXT NOT NULL,
            time            TEXT NOT NULL,
            location        TEXT NOT NULL,
            article         TEXT NOT NULL,
            unit            TEXT NOT NULL,
            quantity        REAL NOT NULL,
            price_chf       REAL NOT NULL,
            discount_chf    REAL NOT NULL,
            category_main   TEXT NOT NULL,
            category_detail TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_groceries_rfn_dedup
            ON groceries_rfn(date, time, location, article, quantity);
        """
    )
    db.commit()


@pytest.fixture()
def tmp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Temp SQLite DB with grocery schema; patches pipeline DB path."""
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
