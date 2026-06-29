from __future__ import annotations

import json

from datetime import datetime

from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_ingestion.adapters.generic_adapter import to_unified
from swiss_exp_tracker.pipeline_ingestion.data_models.data_sources import (
    SUPPORTED_SOURCES,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.data_sources import get_profile
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.data_models.tables import LandingRow
from swiss_exp_tracker.pipeline_ingestion.db import create_all_tables
from swiss_exp_tracker.pipeline_ingestion.db import get_connection


def _load_unprocessed_landing_rows(source_type: SourceType) -> list[LandingRow]:
    """Return all unprocessed landing rows for source_type, ordered by id."""
    with get_connection() as db:
        rows = list(
            transactions.get_unprocessed_landing_rows(db, source_type=source_type.value)
        )

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
    """Promote unprocessed landing rows to transactions_raw for source_type.

    Validates each row via the generic adapter (raises on malformed required data)
    and writes the unchanged canonical row dict JSON to transactions_raw.
    """
    create_all_tables()
    rows = _load_unprocessed_landing_rows(source_type)

    rows_processed = 0
    records_inserted = 0
    processed_file_ids: set[int] = set()

    profile = get_profile(source_type)

    with get_connection() as db:
        for row in rows:
            payload = json.loads(row.raw_json)
            to_unified(payload, profile, row.source_file)

            transactions.insert_transactions_raw(
                db,
                landing_id=row.landing_id,
                source_type=source_type.value,
                raw_json=json.dumps(payload, ensure_ascii=False),
                source_file=row.source_file,
                created_at=datetime.now().isoformat(),
            )

            transactions.mark_lnd_processed(db, landing_id=row.landing_id)

            processed_file_ids.add(row.file_id)
            rows_processed += 1
            records_inserted += 1

        for file_id in processed_file_ids:
            count = transactions.count_unprocessed_lnd_for_file(db, file_id=file_id)
            if count:
                continue
            transactions.set_ingested_file_status(db, status="raw", file_id=file_id)

    return {
        "rows_found": len(rows),
        "rows_processed": rows_processed,
        "records_inserted": records_inserted,
    }


def run_raw() -> dict[str, dict[str, int]]:
    """Run raw stage for all known source types."""
    results: dict[str, dict[str, int]] = {}

    for source_type in SUPPORTED_SOURCES:
        result = process_raw_source(source_type)
        results[source_type.value] = result

    return results
