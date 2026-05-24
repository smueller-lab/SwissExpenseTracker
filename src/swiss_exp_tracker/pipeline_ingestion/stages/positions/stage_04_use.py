from __future__ import annotations

from swiss_exp_tracker.pipeline_ingestion.db_positions import create_positions_tables
from swiss_exp_tracker.pipeline_ingestion.db_positions import get_positions_connection


def run_positions_use() -> dict[str, int]:
    create_positions_tables()

    with get_positions_connection() as db:
        db.execute("DELETE FROM positions_use")
        cursor = db.execute(
            """
            INSERT INTO positions_use (
                date, account, asset_class,
                symbol, name, quantity, unit_cost, price, currency,
                value, value_chf, pnl_chf, pnl_pct, weight_pct
            )
            SELECT
                snapshot_date, account_id, asset_class,
                symbol, name, quantity, unit_cost, price, currency,
                total_value, total_value_chf, pnl_nominal_chf, pnl_pct_chf,
                position_weight_pct
            FROM positions_rfn
            ORDER BY snapshot_date, asset_class, symbol
            """
        )
        count = cursor.rowcount

    return {"rows_written": count}
