from __future__ import annotations

from swiss_exp_tracker.db.sql import positions
from swiss_exp_tracker.pipeline_ingestion.db_positions import create_positions_tables
from swiss_exp_tracker.pipeline_ingestion.db_positions import get_positions_connection


def run_positions_use() -> dict[str, int]:
    """Rebuild positions_use from positions_rfn."""
    create_positions_tables()

    with get_positions_connection() as db:
        positions.delete_positions_use(db)
        count = positions.populate_positions_use(db)

    return {"rows_written": count}
