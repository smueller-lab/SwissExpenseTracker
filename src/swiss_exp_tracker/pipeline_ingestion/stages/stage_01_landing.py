from __future__ import annotations

import csv
import json

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from swiss_exp_tracker.pipeline_ingestion.config import LANDING_ZONE_DIR
from swiss_exp_tracker.pipeline_ingestion.data_models.data_sources import (
    SOURCE_MODEL_MAP,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.db import create_all_tables
from swiss_exp_tracker.pipeline_ingestion.db import get_connection
from swiss_exp_tracker.pipeline_ingestion.file_tracker import get_new_files
from swiss_exp_tracker.pipeline_ingestion.file_tracker import mark_file_processed


def _read_rows(file_path: Path) -> list[dict[str, Any]]:
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        with file_path.open(encoding="utf-8-sig", newline="") as file:
            # detect delimiter
            sample = file.read(4096)
            file.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;")
                reader = csv.DictReader(file, dialect=dialect)
            except csv.Error:
                reader = csv.DictReader(file)
            return list(reader)

    if suffix in {".xlsx", ".xls"}:
        frame = pd.read_excel(file_path)
        records = frame.to_dict(orient="records")
        return [{str(key): value for key, value in row.items()} for row in records]

    return []


def _insert_landing_row(file_id: int, source_type: SourceType, raw_json: str) -> None:
    with get_connection() as db:
        db.execute(
            """
			INSERT INTO transactions_lnd (
				file_id,
				source_type,
				raw_json,
				created_at,
				processed
			)
			VALUES (?, ?, ?, ?, 0)
			""",
            (
                file_id,
                source_type.value,
                raw_json,
                datetime.now().isoformat(),
            ),
        )


def _get_latest_file_id(file_name: str, source_type: SourceType) -> int:
    with get_connection() as db:
        row = db.execute(
            """
			SELECT id
			FROM ingested_files
			WHERE filename = ? AND source_type = ?
			ORDER BY id DESC
			LIMIT 1
			""",
            (file_name, source_type.value),
        ).fetchone()

    if row is None:
        msg = f"No ingested_files row found for {file_name} ({source_type.value})"
        raise RuntimeError(msg)

    return int(row[0])


def process_landing_source(
    folder: str | Path, source_type: SourceType
) -> dict[str, int]:
    create_all_tables()

    if source_type not in SOURCE_MODEL_MAP:
        msg = f"Source type {source_type.value} is not supported in landing stage yet"
        raise NotImplementedError(msg)

    source_model = SOURCE_MODEL_MAP[source_type]
    new_files = get_new_files(folder, source_type)

    files_processed = 0
    records_inserted = 0

    for file_path in new_files:
        # read rows
        rows = _read_rows(file_path)
        if not rows:
            continue

        # model validate and convert to JSON
        valid_rows_json: list[str] = []
        for row in rows:
            parsed = source_model.model_validate(row)
            dumped = parsed.model_dump(mode="json", by_alias=True)
            valid_rows_json.append(json.dumps(dumped, ensure_ascii=False))

        # mark file as landing processed
        mark_file_processed(
            filename=file_path,
            source=source_type,
            record_count=len(valid_rows_json),
            status="landing",
        )
        file_id = _get_latest_file_id(file_path.name, source_type)

        for raw_json in valid_rows_json:
            _insert_landing_row(file_id, source_type, raw_json)
            records_inserted += 1

        files_processed += 1

    return {
        "files_found": len(new_files),
        "files_processed": files_processed,
        "records_inserted": records_inserted,
    }


def run_landing() -> dict[str, dict[str, int]]:
    results: dict[str, dict[str, int]] = {}

    for source_type in SOURCE_MODEL_MAP:
        source_folder = LANDING_ZONE_DIR / source_type.value.lower()
        results[source_type.value] = process_landing_source(source_folder, source_type)

    return results
