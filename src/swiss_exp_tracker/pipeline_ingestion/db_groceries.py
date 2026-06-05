from __future__ import annotations

from swiss_exp_tracker.db.sql import groceries
from swiss_exp_tracker.pipeline_ingestion.db import get_connection


def create_grocery_tables() -> None:
    """Create all grocery pipeline tables in transactions.db if they don't exist."""
    with get_connection() as db:
        groceries.create_groceries_lnd_table(db)
        groceries.create_groceries_raw_table(db)
        groceries.create_groceries_rfn_table(db)
        groceries.create_idx_groceries_lnd_processed(db)
        groceries.create_idx_groceries_raw_processed(db)
        groceries.create_idx_groceries_rfn_enrichment(db)
        groceries.create_idx_groceries_rfn_dedup(db)
