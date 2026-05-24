from __future__ import annotations

import json

from datetime import datetime

from swiss_exp_tracker.pipeline_ingestion.data_models.position import SwissquotePosition
from swiss_exp_tracker.pipeline_ingestion.db_positions import create_positions_tables
from swiss_exp_tracker.pipeline_ingestion.db_positions import get_positions_connection


def run_positions_rfn() -> dict[str, int]:
    create_positions_tables()

    with get_positions_connection() as db:
        raw_rows = db.execute(
            """
            SELECT pr.id, pl.file_id, pr.raw_json, pr.source_file
            FROM positions_raw pr
            JOIN positions_lnd pl ON pl.id = pr.landing_id
            WHERE pr.processed = 0
            ORDER BY pr.id
            """
        ).fetchall()

    rows_processed = 0
    records_inserted = 0
    duplicates_ignored = 0
    processed_file_ids: set[int] = set()

    with get_positions_connection() as db:
        for raw_id, file_id, raw_json_str, source_file in raw_rows:
            payload = json.loads(str(raw_json_str))
            position = SwissquotePosition.model_validate(payload)

            cursor = db.execute(
                """
                INSERT OR IGNORE INTO positions_rfn (
                    snapshot_date, account_id, asset_class,
                    symbol, name, quantity, unit_cost, price, currency,
                    total_value, total_value_chf, pnl_nominal_chf, pnl_pct_chf,
                    position_weight_pct, source_file, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position.snapshot_date.isoformat(),
                    position.account_id,
                    position.asset_class,
                    position.symbol,
                    position.name,
                    position.quantity,
                    position.unit_cost,
                    position.price,
                    position.currency,
                    position.total_value,
                    position.total_value_chf,
                    position.pnl_nominal_chf,
                    position.pnl_pct_chf,
                    position.position_weight_pct,
                    source_file,
                    datetime.now().isoformat(),
                ),
            )

            if cursor.rowcount > 0:
                records_inserted += 1
            else:
                duplicates_ignored += 1

            db.execute(
                "UPDATE positions_raw SET processed = 1 WHERE id = ?",
                (raw_id,),
            )
            processed_file_ids.add(int(file_id))
            rows_processed += 1

        for file_id in processed_file_ids:
            unprocessed = db.execute(
                """
                SELECT COUNT(*) FROM positions_raw pr
                JOIN positions_lnd pl ON pl.id = pr.landing_id
                WHERE pl.file_id = ? AND pr.processed = 0
                """,
                (file_id,),
            ).fetchone()
            if unprocessed is None or int(unprocessed[0]) > 0:
                continue
            db.execute(
                "UPDATE ingested_files_pos SET status = 'refined' WHERE id = ?",
                (file_id,),
            )

    return {
        "rows_found": len(raw_rows),
        "rows_processed": rows_processed,
        "records_inserted": records_inserted,
        "duplicates_ignored": duplicates_ignored,
    }
