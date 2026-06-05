from __future__ import annotations

import json

from datetime import datetime

from swiss_exp_tracker.db.sql import positions
from swiss_exp_tracker.pipeline_ingestion.data_models.position import SwissquotePosition
from swiss_exp_tracker.pipeline_ingestion.db_positions import create_positions_tables
from swiss_exp_tracker.pipeline_ingestion.db_positions import get_positions_connection


def run_positions_rfn() -> dict[str, int]:
    """Promote unprocessed positions_raw rows to positions_rfn, skipping duplicates."""
    create_positions_tables()

    with get_positions_connection() as db:
        raw_rows = list(positions.get_unprocessed_positions_raw_rows(db))

    rows_processed = 0
    records_inserted = 0
    duplicates_ignored = 0
    processed_file_ids: set[int] = set()

    with get_positions_connection() as db:
        for raw_id, file_id, raw_json_str, source_file in raw_rows:
            payload = json.loads(str(raw_json_str))
            position = SwissquotePosition.model_validate(payload)

            rowcount = positions.insert_positions_rfn_ignore(
                db,
                snapshot_date=position.snapshot_date.isoformat(),
                account_id=position.account_id,
                asset_class=position.asset_class,
                symbol=position.symbol,
                name=position.name,
                quantity=position.quantity,
                unit_cost=position.unit_cost,
                price=position.price,
                currency=position.currency,
                total_value=position.total_value,
                total_value_chf=position.total_value_chf,
                pnl_nominal_chf=position.pnl_nominal_chf,
                pnl_pct_chf=position.pnl_pct_chf,
                position_weight_pct=position.position_weight_pct,
                source_file=source_file,
                created_at=datetime.now().isoformat(),
            )

            if rowcount > 0:
                records_inserted += 1
            else:
                duplicates_ignored += 1

            positions.mark_positions_raw_processed(db, raw_id=raw_id)
            processed_file_ids.add(int(file_id))
            rows_processed += 1

        for file_id in processed_file_ids:
            count = positions.count_unprocessed_positions_raw_for_file(
                db, file_id=file_id
            )
            if count:
                continue
            positions.set_positions_file_status(db, status="refined", file_id=file_id)

    return {
        "rows_found": len(raw_rows),
        "rows_processed": rows_processed,
        "records_inserted": records_inserted,
        "duplicates_ignored": duplicates_ignored,
    }
