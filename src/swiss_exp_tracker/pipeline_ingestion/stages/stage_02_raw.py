from __future__ import annotations

import json

from datetime import datetime

from swiss_exp_tracker.pipeline_ingestion.data_models.data_sources import (
    SOURCE_MODEL_MAP,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.data_models.tables import LandingRow
from swiss_exp_tracker.pipeline_ingestion.db import create_all_tables
from swiss_exp_tracker.pipeline_ingestion.db import get_connection


def _load_unprocessed_landing_rows(source_type: SourceType) -> list[LandingRow]:
    query = """
        SELECT tl.id, tl.file_id, tl.source_type, tl.raw_json, f.filename
        FROM transactions_lnd tl
        JOIN ingested_files f ON f.id = tl.file_id
        WHERE tl.processed = 0
          AND tl.source_type = f.source_type
          AND tl.source_type = ?
        ORDER BY tl.id
    """

    with get_connection() as db:
        rows = db.execute(query, (source_type.value,)).fetchall()

    return [
        LandingRow(
            landing_id=int(row[0]),
            file_id=int(row[1]),
            source_type=SourceType(row[2]),
            raw_json=str(row[3]),
            source_file=str(row[4]),
        )
        for row in rows
    ]


def process_raw_source(source_type: SourceType) -> dict[str, int]:
    create_all_tables()
    rows = _load_unprocessed_landing_rows(source_type)

    rows_processed = 0
    records_inserted = 0
    processed_file_ids: set[int] = set()

    with get_connection() as db:
        for row in rows:
            current_source = SourceType(row.source_type)
            model_class = SOURCE_MODEL_MAP[current_source]

            payload = json.loads(row.raw_json)
            source_model = model_class.model_validate(payload)
            validated_raw_json = json.dumps(
                source_model.model_dump(mode="json", by_alias=True),
                ensure_ascii=False,
            )

            db.execute(
                """
                INSERT INTO transactions_raw (
                    landing_id,
                    source_type,
                    raw_json,
                    source_file,
                    created_at,
                    processed
                )
                VALUES (?, ?, ?, ?, ?, 0)
                """,
                (
                    row.landing_id,
                    SourceType(row.source_type).value,
                    validated_raw_json,
                    row.source_file,
                    datetime.now().isoformat(),
                ),
            )

            db.execute(
                "UPDATE transactions_lnd SET processed = 1 WHERE id = ?",
                (row.landing_id,),
            )

            processed_file_ids.add(row.file_id)
            rows_processed += 1
            records_inserted += 1

        for file_id in processed_file_ids:
            unprocessed_count = db.execute(
                "SELECT COUNT(*) FROM transactions_lnd WHERE file_id = ? AND processed = 0",
                (file_id,),
            ).fetchone()
            if unprocessed_count is None or int(unprocessed_count[0]) > 0:
                continue

            db.execute(
                "UPDATE ingested_files SET status = 'raw' WHERE id = ?",
                (file_id,),
            )

    return {
        "rows_found": len(rows),
        "rows_processed": rows_processed,
        "records_inserted": records_inserted,
    }


def run_raw() -> dict[str, dict[str, int]]:
    results: dict[str, dict[str, int]] = {}

    for source_type in SOURCE_MODEL_MAP:
        result = process_raw_source(source_type)
        results[source_type.value] = result

    return results
