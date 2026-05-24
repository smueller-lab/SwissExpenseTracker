from __future__ import annotations

import sqlite3

from collections.abc import Generator
from contextlib import contextmanager

from swiss_exp_tracker.pipeline_ingestion.config import POSITIONS_DB_PATH


@contextmanager
def get_positions_connection() -> Generator[sqlite3.Connection, None, None]:
    db = sqlite3.connect(POSITIONS_DB_PATH)
    try:
        db.execute("PRAGMA foreign_keys = ON")
        yield db
        db.commit()
    finally:
        db.close()


def create_positions_tables() -> None:
    with get_positions_connection() as db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS ingested_files_pos (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                filename     TEXT    NOT NULL,
                file_hash    TEXT    NOT NULL,
                ingested_at  TEXT    NOT NULL,
                record_count INTEGER NOT NULL,
                status       TEXT    NOT NULL,
                UNIQUE(filename, file_hash)
            )
            """
        )

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS positions_lnd (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id    INTEGER NOT NULL,
                raw_json   TEXT    NOT NULL,
                created_at TEXT    NOT NULL,
                processed  INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (file_id) REFERENCES ingested_files_pos(id)
            )
            """
        )

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS positions_raw (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                landing_id  INTEGER NOT NULL,
                raw_json    TEXT    NOT NULL,
                source_file TEXT    NOT NULL,
                created_at  TEXT    NOT NULL,
                processed   INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (landing_id) REFERENCES positions_lnd(id)
            )
            """
        )

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS positions_rfn (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date       TEXT    NOT NULL,
                account_id          TEXT    NOT NULL,
                asset_class         TEXT    NOT NULL,
                -- in CCY (original currency) --
                symbol              TEXT    NOT NULL,
                name                TEXT,
                quantity            REAL    NOT NULL,
                unit_cost           REAL,
                price               REAL    NOT NULL,
                currency            TEXT    NOT NULL,
                total_value         REAL,
                -- in CHF --
                total_value_chf     REAL    NOT NULL,
                pnl_nominal_chf     REAL,
                pnl_pct_chf         REAL,
                position_weight_pct REAL,
                -- metadata --
                source_file         TEXT    NOT NULL,
                created_at          TEXT    NOT NULL,
                UNIQUE(snapshot_date, symbol, account_id)
            )
            """
        )

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS positions_use (
                date        TEXT NOT NULL,
                account     TEXT NOT NULL,
                asset_class TEXT NOT NULL,
                symbol      TEXT NOT NULL,
                name        TEXT,
                quantity    REAL NOT NULL,
                unit_cost   REAL,
                price       REAL NOT NULL,
                currency    TEXT NOT NULL,
                value       REAL,
                value_chf   REAL NOT NULL,
                pnl_chf     REAL,
                pnl_pct     REAL,
                weight_pct  REAL
            )
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pos_rfn_date
            ON positions_rfn(snapshot_date)
            """
        )
        db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pos_rfn_symbol
            ON positions_rfn(symbol)
            """
        )
        db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pos_lnd_processed
            ON positions_lnd(file_id, processed)
            """
        )
        db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pos_raw_processed
            ON positions_raw(processed)
            """
        )
