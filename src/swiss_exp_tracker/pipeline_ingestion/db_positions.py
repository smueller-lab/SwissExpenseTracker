from __future__ import annotations

import sqlite3

from collections.abc import Generator
from contextlib import contextmanager

from swiss_exp_tracker.db.sql import positions
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
    """Create all positions pipeline tables and indexes in positions.db if they do not exist."""
    with get_positions_connection() as db:
        positions.create_ingested_files_pos_table(db)
        positions.create_positions_lnd_table(db)
        positions.create_positions_raw_table(db)
        positions.create_positions_rfn_table(db)
        positions.create_positions_use_table(db)
        positions.create_idx_pos_rfn_date(db)
        positions.create_idx_pos_rfn_symbol(db)
        positions.create_idx_pos_lnd_processed(db)
        positions.create_idx_pos_raw_processed(db)
