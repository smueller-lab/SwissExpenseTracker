from __future__ import annotations

import json
import logging

from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_ingestion.adapters.generic_adapter import to_unified
from swiss_exp_tracker.pipeline_ingestion.data_models.profile_loader import (
    load_profiles,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.db import get_connection
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_03_refined import (
    _extract_merchant_normalized,
)

logger = logging.getLogger(__name__)


def run_booking_text_backfill() -> dict[str, int]:
    """Re-derive booking_text for refined rows that have none, using current source profiles.

    Reads each affected row's raw JSON, re-runs the adapter (which now applies the
    Details fallback for Viseca), and updates booking_text + merchant when a value is
    recovered. Returns counts of rows scanned and rows updated.
    """
    profiles = load_profiles()
    rows_scanned = 0
    rows_updated = 0

    with get_connection() as db:
        rows = list(transactions.get_rfn_rows_missing_booking_text(db))
        for rfn_id, source_type_str, raw_json_str in rows:
            rows_scanned += 1
            try:
                source_type = SourceType(str(source_type_str))
            except ValueError:
                continue
            profile = profiles.get(source_type)
            if profile is None:
                continue

            payload = json.loads(raw_json_str)
            booking_text = to_unified(payload, profile, "backfill").booking_text
            if not booking_text:
                continue

            merchant_normalized, is_person = _extract_merchant_normalized(booking_text)
            transactions.set_rfn_booking_text(
                db,
                booking_text=booking_text,
                merchant_normalized=merchant_normalized,
                is_person=int(is_person),
                rfn_id=int(rfn_id),
            )
            rows_updated += 1

    return {"rows_scanned": rows_scanned, "rows_updated": rows_updated}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = run_booking_text_backfill()
    logger.info(
        "booking_text backfill: scanned %d row(s), updated %d.",
        result["rows_scanned"],
        result["rows_updated"],
    )
