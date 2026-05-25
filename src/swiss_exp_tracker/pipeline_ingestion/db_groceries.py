from __future__ import annotations

from swiss_exp_tracker.pipeline_ingestion.db import get_connection


def create_grocery_tables() -> None:
    """Create all grocery pipeline tables in transactions.db if they don't exist."""
    with get_connection() as db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS groceries_lnd (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id      INTEGER NOT NULL,
                source_type  TEXT NOT NULL,
                raw_json     TEXT NOT NULL,
                is_bonus_row INTEGER NOT NULL DEFAULT 0,
                created_at   TEXT NOT NULL,
                processed    INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (file_id) REFERENCES ingested_files(id)
            )
            """
        )

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS groceries_raw (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                landing_id   INTEGER NOT NULL,
                source_type  TEXT NOT NULL,
                raw_json     TEXT NOT NULL,
                source_file  TEXT NOT NULL,
                created_at   TEXT NOT NULL,
                processed    INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (landing_id) REFERENCES groceries_lnd(id)
            )
            """
        )

        db.execute(
            """
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
            )
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_groceries_lnd_processed
            ON groceries_lnd(processed)
            """
        )
        db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_groceries_raw_processed
            ON groceries_raw(processed)
            """
        )
        db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_groceries_rfn_enrichment
            ON groceries_rfn(enrichment_status)
            """
        )
        # Composite index to accelerate the duplicate check in stage_03_rfn.
        db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_groceries_rfn_dedup
            ON groceries_rfn(date, time, location, article, quantity)
            """
        )
