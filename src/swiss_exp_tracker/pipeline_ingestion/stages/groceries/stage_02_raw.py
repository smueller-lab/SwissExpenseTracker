from __future__ import annotations

import json

from datetime import datetime

from swiss_exp_tracker.pipeline_ingestion.data_models.grocery import GroceryItem
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.db import get_connection
from swiss_exp_tracker.pipeline_ingestion.db_groceries import create_grocery_tables


_SOURCE_TYPE = SourceType.MIGROS_GROCERY


def run_groceries_raw() -> dict[str, int]:
    create_grocery_tables()

    with get_connection() as db:
        rows = db.execute(
            """
            SELECT gl.id, gl.file_id, gl.raw_json, gl.is_bonus_row, f.filename
            FROM groceries_lnd gl
            JOIN ingested_files f ON f.id = gl.file_id
            WHERE gl.processed = 0
              AND gl.source_type = ?
            ORDER BY gl.id
            """,
            (_SOURCE_TYPE.value,),
        ).fetchall()

    if not rows:
        return {"rows_found": 0, "rows_processed": 0, "records_inserted": 0}

    rows_processed = 0
    records_inserted = 0
    processed_file_ids: set[int] = set()

    with get_connection() as db:
        for landing_id, file_id, raw_json, _is_bonus, source_file in rows:
            # Re-validate to produce canonical JSON with English field names.
            payload = json.loads(raw_json)
            item = GroceryItem.model_validate(payload)
            canonical_json = json.dumps(
                item.model_dump(mode="json", by_alias=False),
                ensure_ascii=False,
            )

            db.execute(
                """
                INSERT INTO groceries_raw
                    (landing_id, source_type, raw_json, source_file, created_at, processed)
                VALUES (?, ?, ?, ?, ?, 0)
                """,
                (
                    landing_id,
                    _SOURCE_TYPE.value,
                    canonical_json,
                    source_file,
                    datetime.now().isoformat(),
                ),
            )
            db.execute(
                "UPDATE groceries_lnd SET processed = 1 WHERE id = ?",
                (landing_id,),
            )

            processed_file_ids.add(file_id)
            rows_processed += 1
            records_inserted += 1

        for file_id in processed_file_ids:
            unprocessed = db.execute(
                "SELECT COUNT(*) FROM groceries_lnd WHERE file_id = ? AND processed = 0",
                (file_id,),
            ).fetchone()
            if unprocessed and int(unprocessed[0]) == 0:
                db.execute(
                    "UPDATE ingested_files SET status = 'raw' WHERE id = ?",
                    (file_id,),
                )

    return {
        "rows_found": len(rows),
        "rows_processed": rows_processed,
        "records_inserted": records_inserted,
    }
