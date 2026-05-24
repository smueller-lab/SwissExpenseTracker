from __future__ import annotations

import json

from datetime import datetime

from swiss_exp_tracker.pipeline_ingestion.data_models.position import (
    SwissquotePositionRaw,
)
from swiss_exp_tracker.pipeline_ingestion.db_positions import create_positions_tables
from swiss_exp_tracker.pipeline_ingestion.db_positions import get_positions_connection


def run_positions_raw() -> dict[str, int]:
    create_positions_tables()

    with get_positions_connection() as db:
        landing_rows = db.execute(
            """
            SELECT pl.id, pl.file_id, pl.raw_json, f.filename
            FROM positions_lnd pl
            JOIN ingested_files_pos f ON f.id = pl.file_id
            WHERE pl.processed = 0
            ORDER BY pl.id
            """
        ).fetchall()

    rows_processed = 0
    records_inserted = 0
    processed_file_ids: set[int] = set()

    with get_positions_connection() as db:
        for landing_id, file_id, raw_json_str, source_file in landing_rows:
            payload = json.loads(str(raw_json_str))
            model = SwissquotePositionRaw.model_validate(payload)
            validated_json = json.dumps(model.model_dump(mode="json"))

            db.execute(
                """
                INSERT INTO positions_raw (
                    landing_id, raw_json, source_file, created_at, processed
                )
                VALUES (?, ?, ?, ?, 0)
                """,
                (landing_id, validated_json, source_file, datetime.now().isoformat()),
            )
            db.execute(
                "UPDATE positions_lnd SET processed = 1 WHERE id = ?",
                (landing_id,),
            )

            processed_file_ids.add(int(file_id))
            rows_processed += 1
            records_inserted += 1

        for file_id in processed_file_ids:
            unprocessed = db.execute(
                "SELECT COUNT(*) FROM positions_lnd WHERE file_id = ? AND processed = 0",
                (file_id,),
            ).fetchone()
            if unprocessed is None or int(unprocessed[0]) > 0:
                continue
            db.execute(
                "UPDATE ingested_files_pos SET status = 'raw' WHERE id = ?",
                (file_id,),
            )

    return {
        "rows_found": len(landing_rows),
        "rows_processed": rows_processed,
        "records_inserted": records_inserted,
    }
