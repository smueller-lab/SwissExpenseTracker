from __future__ import annotations

import json

from datetime import datetime

from swiss_exp_tracker.db.sql import positions
from swiss_exp_tracker.pipeline_ingestion.data_models.position import (
    SwissquotePositionRaw,
)
from swiss_exp_tracker.pipeline_ingestion.db_positions import create_positions_tables
from swiss_exp_tracker.pipeline_ingestion.db_positions import get_positions_connection


def run_positions_raw() -> dict[str, int]:
    """Promote unprocessed positions_lnd rows to positions_raw."""
    create_positions_tables()

    with get_positions_connection() as db:
        landing_rows = list(positions.get_unprocessed_positions_lnd_rows(db))

    rows_processed = 0
    records_inserted = 0
    processed_file_ids: set[int] = set()

    with get_positions_connection() as db:
        for landing_id, file_id, raw_json_str, source_file in landing_rows:
            payload = json.loads(str(raw_json_str))
            model = SwissquotePositionRaw.model_validate(payload)
            validated_json = json.dumps(model.model_dump(mode="json"))

            positions.insert_positions_raw(
                db,
                landing_id=landing_id,
                raw_json=validated_json,
                source_file=source_file,
                created_at=datetime.now().isoformat(),
            )
            positions.mark_positions_lnd_processed(db, landing_id=landing_id)

            processed_file_ids.add(int(file_id))
            rows_processed += 1
            records_inserted += 1

        for file_id in processed_file_ids:
            count = positions.count_unprocessed_positions_lnd_for_file(
                db, file_id=file_id
            )
            if count:
                continue
            positions.set_positions_file_status(db, status="raw", file_id=file_id)

    return {
        "rows_found": len(landing_rows),
        "rows_processed": rows_processed,
        "records_inserted": records_inserted,
    }
