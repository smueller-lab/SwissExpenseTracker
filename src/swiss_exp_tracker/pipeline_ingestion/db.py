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


# Per-source raw-JSON key holding the running account balance.
_BALANCE_JSON_KEY: dict[str, str] = {
    "ZKB_DEBIT": "Balance CHF",
    "UBS_DEBIT": "Solde",
}


def _table_exists(db: sqlite3.Connection, name: str) -> bool:
    """Return True if a table named `name` exists in the database."""
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def backfill_balance_chf_rfn(db: sqlite3.Connection) -> int:
    """Fill NULL balance_chf in transactions_rfn from raw JSON; returns rows updated."""
    if not _table_exists(db, "transactions_rfn") or not _table_exists(
        db, "transactions_raw"
    ):
        return 0
    rows = transactions.get_rfn_rows_for_balance_backfill(db)
    updated = 0
    for row_id, source_type, raw_json_str in rows:
        key = _BALANCE_JSON_KEY.get(str(source_type))
        if key is None:
            continue
        balance = json.loads(raw_json_str).get(key)
        if balance is not None and str(balance).strip() != "":
            transactions.set_rfn_balance_chf(
                db, balance_chf=float(str(balance).strip()), rfn_id=row_id
            )
            updated += 1
    return updated


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
            backfill_balance_chf_rfn(db)

        transactions.create_idx_ingested_files_source(db)
        transactions.create_idx_landing_file_processed(db)
        transactions.create_idx_raw_source_processed(db)
        transactions.create_idx_refined_enrichment_status(db)
