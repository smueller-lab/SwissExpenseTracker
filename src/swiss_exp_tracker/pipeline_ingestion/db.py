from __future__ import annotations

import json
import sqlite3

from collections.abc import Generator
from contextlib import contextmanager

from swiss_exp_tracker.pipeline_ingestion.config import INGESTION_DB_PATH


@contextmanager
def get_connection() -> Generator[sqlite3.Connection, None, None]:
    db = sqlite3.connect(INGESTION_DB_PATH)
    try:
        db.execute("PRAGMA foreign_keys = ON")
        yield db
        db.commit()
    finally:
        db.close()


def _backfill_balance_chf_rfn(db: sqlite3.Connection) -> None:
    """Populate balance_chf for existing ZKB rows from the raw JSON stored in transactions_raw."""
    rows = db.execute(
        """
        SELECT rfn.id, raw.raw_json
        FROM transactions_rfn rfn
        JOIN transactions_raw raw ON raw.id = rfn.raw_id
        WHERE rfn.source_type = 'ZKB_DEBIT'
          AND rfn.balance_chf IS NULL
        """
    ).fetchall()
    for row_id, raw_json_str in rows:
        data = json.loads(raw_json_str)
        balance = data.get("Balance CHF")
        if balance is not None:
            db.execute(
                "UPDATE transactions_rfn SET balance_chf = ? WHERE id = ?",
                (float(balance), row_id),
            )


def create_all_tables() -> None:
    with get_connection() as db:
        db.execute(
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
			)
			"""
        )

        db.execute(
            """
			CREATE TABLE IF NOT EXISTS transactions_lnd (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				file_id INTEGER NOT NULL,
				source_type TEXT NOT NULL,
				raw_json TEXT NOT NULL,
				created_at TEXT NOT NULL,
				processed INTEGER NOT NULL DEFAULT 0,
				FOREIGN KEY (file_id) REFERENCES ingested_files(id)
			)
			"""
        )

        db.execute(
            """
			CREATE TABLE IF NOT EXISTS transactions_raw (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				landing_id INTEGER NOT NULL,
				source_type TEXT NOT NULL,
				raw_json TEXT NOT NULL,
				source_file TEXT NOT NULL,
				created_at TEXT NOT NULL,
				processed INTEGER NOT NULL DEFAULT 0,
				FOREIGN KEY (landing_id) REFERENCES transactions_lnd(id)
			)
			"""
        )

        db.execute(
            """
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
			)
			"""
        )

        refined_columns = {
            str(column[1])
            for column in db.execute("PRAGMA table_info(transactions_rfn)").fetchall()
        }
        if "is_person" not in refined_columns:
            db.execute(
                "ALTER TABLE transactions_rfn ADD COLUMN is_person INTEGER NOT NULL DEFAULT 0"
            )
        if "balance_chf" not in refined_columns:
            db.execute("ALTER TABLE transactions_rfn ADD COLUMN balance_chf REAL")
            _backfill_balance_chf_rfn(db)

        db.execute(
            """
			CREATE INDEX IF NOT EXISTS idx_ingested_files_source
			ON ingested_files(source_type)
			"""
        )
        db.execute(
            """
			CREATE INDEX IF NOT EXISTS idx_landing_file_processed
			ON transactions_lnd(file_id, processed)
			"""
        )
        db.execute(
            """
			CREATE INDEX IF NOT EXISTS idx_raw_source_processed
			ON transactions_raw(source_type, processed)
			"""
        )
        db.execute(
            """
			CREATE INDEX IF NOT EXISTS idx_refined_enrichment_status
			ON transactions_rfn(enrichment_status)
			"""
        )

        db.execute(
            """
			CREATE TABLE IF NOT EXISTS api_usage (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				provider TEXT NOT NULL,
				period TEXT NOT NULL,
				used INTEGER NOT NULL DEFAULT 0,
				credit_limit INTEGER NOT NULL DEFAULT 0,
				updated_at TEXT NOT NULL,
				UNIQUE(provider, period)
			)
			"""
        )
