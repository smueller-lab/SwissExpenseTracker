from __future__ import annotations

import json

from datetime import datetime

from swiss_exp_tracker.db.sql import groceries
from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_ingestion.data_models.grocery import GroceryItem
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.db import get_connection
from swiss_exp_tracker.pipeline_ingestion.db_groceries import create_grocery_tables

_SOURCE_TYPE = SourceType.MIGROS_GROCERY


def run_groceries_raw() -> dict[str, int]:
    """Promote unprocessed groceries_lnd rows to groceries_raw."""
    create_grocery_tables()

    with get_connection() as db:
        rows = list(
            groceries.get_unprocessed_groceries_lnd_rows(
                db, source_type=_SOURCE_TYPE.value
            )
        )

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

            groceries.insert_groceries_raw(
                db,
                landing_id=landing_id,
                source_type=_SOURCE_TYPE.value,
                raw_json=canonical_json,
                source_file=source_file,
                created_at=datetime.now().isoformat(),
            )
            groceries.mark_groceries_lnd_processed(db, landing_id=landing_id)

            processed_file_ids.add(file_id)
            rows_processed += 1
            records_inserted += 1

        for file_id in processed_file_ids:
            count = groceries.count_unprocessed_groceries_lnd_for_file(
                db, file_id=file_id
            )
            if not count:
                transactions.set_ingested_file_status(db, status="raw", file_id=file_id)

    return {
        "rows_found": len(rows),
        "rows_processed": rows_processed,
        "records_inserted": records_inserted,
    }
