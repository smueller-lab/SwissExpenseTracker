from __future__ import annotations

import csv
import json

from datetime import datetime
from pathlib import Path
from typing import Any

from swiss_exp_tracker.pipeline_ingestion.config import LANDING_ZONE_DIR
from swiss_exp_tracker.pipeline_ingestion.data_models.grocery import GroceryItem
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.db import get_connection
from swiss_exp_tracker.pipeline_ingestion.db_groceries import create_grocery_tables
from swiss_exp_tracker.pipeline_ingestion.file_tracker import get_new_files
from swiss_exp_tracker.pipeline_ingestion.file_tracker import mark_file_processed


_SOURCE_TYPE = SourceType.MIGROS_GROCERY


def _read_csv_rows(file_path: Path) -> list[dict[str, Any]]:
    with file_path.open(encoding="utf-8-sig", newline="") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;")
            reader = csv.DictReader(fh, dialect=dialect)
        except csv.Error:
            reader = csv.DictReader(fh)
        return list(reader)


def _get_file_id(file_name: str) -> int:
    with get_connection() as db:
        row = db.execute(
            """
            SELECT id FROM ingested_files
            WHERE filename = ? AND source_type = ?
            ORDER BY id DESC LIMIT 1
            """,
            (file_name, _SOURCE_TYPE.value),
        ).fetchone()
    if row is None:
        msg = f"No ingested_files row found for {file_name}"
        raise RuntimeError(msg)
    return int(row[0])


def run_groceries_landing() -> dict[str, int]:
    create_grocery_tables()

    folder = LANDING_ZONE_DIR / _SOURCE_TYPE.value.lower()
    new_files = get_new_files(folder, _SOURCE_TYPE)

    files_processed = 0
    records_inserted = 0

    for file_path in new_files:
        rows = _read_csv_rows(file_path)
        if not rows:
            continue

        validated: list[tuple[str, int]] = []
        for row in rows:
            item = GroceryItem.model_validate(row)
            raw_json = json.dumps(
                item.model_dump(mode="json", by_alias=False),
                ensure_ascii=False,
            )
            is_bonus = 1 if (item.price == 0 and item.discount == 0) else 0
            validated.append((raw_json, is_bonus))

        mark_file_processed(
            filename=file_path,
            source=_SOURCE_TYPE,
            record_count=len(validated),
            status="landing",
        )
        file_id = _get_file_id(file_path.name)

        with get_connection() as db:
            for raw_json, is_bonus in validated:
                db.execute(
                    """
                    INSERT INTO groceries_lnd
                        (file_id, source_type, raw_json, is_bonus_row, created_at, processed)
                    VALUES (?, ?, ?, ?, ?, 0)
                    """,
                    (
                        file_id,
                        _SOURCE_TYPE.value,
                        raw_json,
                        is_bonus,
                        datetime.now().isoformat(),
                    ),
                )
            records_inserted += len(validated)

        files_processed += 1

    return {
        "files_found": len(new_files),
        "files_processed": files_processed,
        "records_inserted": records_inserted,
    }
