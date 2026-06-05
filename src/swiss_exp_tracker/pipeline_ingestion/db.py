from __future__ import annotations

import json
import sqlite3

from collections.abc import Generator
from contextlib import contextmanager

from swiss_exp_tracker.db.sql import transactions
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
    rows = transactions.get_rfn_rows_for_balance_backfill(db)
    for row_id, raw_json_str in rows:
        data = json.loads(raw_json_str)
        balance = data.get("Balance CHF")
        if balance is not None:
            transactions.set_rfn_balance_chf(
                db, balance_chf=float(balance), rfn_id=row_id
            )


def create_all_tables() -> None:
    """Create all pipeline tables and indexes in transactions.db if they do not exist."""
    with get_connection() as db:
        transactions.create_ingested_files_table(db)
        transactions.create_transactions_lnd_table(db)
        transactions.create_transactions_raw_table(db)
        transactions.create_transactions_rfn_table(db)
        transactions.create_api_usage_table(db)

        refined_columns = {str(col[1]) for col in transactions.get_rfn_column_names(db)}
        if "is_person" not in refined_columns:
            db.execute(transactions.alter_rfn_add_is_person.sql)
        if "balance_chf" not in refined_columns:
            db.execute(transactions.alter_rfn_add_balance_chf.sql)
            _backfill_balance_chf_rfn(db)

        transactions.create_idx_ingested_files_source(db)
        transactions.create_idx_landing_file_processed(db)
        transactions.create_idx_raw_source_processed(db)
        transactions.create_idx_refined_enrichment_status(db)
