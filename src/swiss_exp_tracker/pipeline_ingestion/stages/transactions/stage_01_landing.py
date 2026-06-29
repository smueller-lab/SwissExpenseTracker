from __future__ import annotations

import csv
import io
import json
import unicodedata

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_ingestion.adapters.generic_adapter import to_unified
from swiss_exp_tracker.pipeline_ingestion.config import LANDING_ZONE_DIR
from swiss_exp_tracker.pipeline_ingestion.data_models.data_sources import (
    SUPPORTED_SOURCES,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.data_sources import get_profile
from swiss_exp_tracker.pipeline_ingestion.data_models.source_profile import (
    SourceProfile,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.db import create_all_tables
from swiss_exp_tracker.pipeline_ingestion.db import get_connection
from swiss_exp_tracker.pipeline_ingestion.file_tracker import get_new_files
from swiss_exp_tracker.pipeline_ingestion.file_tracker import mark_file_processed


def _canonical_header(text: str) -> str:
    """Collapse a header to an accent/encoding-insensitive token.

    Drops accented letters and the U+FFFD replacement char entirely, so a clean
    "Débit" and a mojibake'd "D�bit" both reduce to "dbit".
    """
    cleaned = text.replace("�", "")
    decomposed = unicodedata.normalize("NFKD", cleaned)
    kept: list[str] = []
    for char in decomposed:
        if unicodedata.combining(char):
            if kept:
                kept.pop()  # drop the base letter the accent sat on
            continue
        kept.append(char)
    return "".join(char for char in "".join(kept).lower() if char.isalnum())


def _strip_sep_prefix(cell: str) -> str:
    """Remove an Excel 'sep=;' directive glued onto the first header cell."""
    if cell.lower().startswith("sep="):
        return cell.split(";", 1)[-1] if ";" in cell else cell[len("sep=") :]
    return cell


def _decode_ubs_text(file_path: Path) -> str:
    """Decode a UBS export, trying UTF-8 then cp1252 (UBS exports are Windows Latin-1)."""
    raw = file_path.read_bytes()
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8-sig", errors="replace")


def _read_ubs_rows(
    file_path: Path, profile: SourceProfile, header_signature: str
) -> list[dict[str, Any]]:
    """Read a CSV with a metadata preamble, skipping to header_signature and renaming via profile aliases."""
    alias_by_canonical = {
        _canonical_header(french): alias
        for french, alias in profile.column_aliases.items()
    }
    signature = _canonical_header(header_signature)

    text = _decode_ubs_text(file_path)
    all_rows = list(csv.reader(io.StringIO(text), delimiter=";"))

    header_index = next(
        (
            i
            for i, row in enumerate(all_rows)
            if any(_canonical_header(cell) == signature for cell in row)
        ),
        None,
    )
    if header_index is None:
        return []

    raw_header = [_strip_sep_prefix(cell) for cell in all_rows[header_index]]
    header = [
        alias_by_canonical.get(_canonical_header(cell), cell.strip())
        for cell in raw_header
    ]

    records: list[dict[str, Any]] = []
    for row in all_rows[header_index + 1 :]:
        if not any(cell.strip() for cell in row):
            continue
        records.append({header[i]: row[i] for i in range(min(len(header), len(row)))})
    return records


def _read_rows(file_path: Path, profile: SourceProfile) -> list[dict[str, Any]]:
    """Read CSV or Excel file and return rows as a list of dicts with canonical column keys."""
    if profile.header_signature is not None:
        return _read_ubs_rows(file_path, profile, profile.header_signature)

    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        with file_path.open(encoding="utf-8-sig", newline="") as file:
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


def process_landing_source(
    folder: str | Path, source_type: SourceType
) -> dict[str, int]:
    """Process new files for source_type: validate, register, and batch-insert to landing."""
    create_all_tables()

    if source_type not in SUPPORTED_SOURCES:
        msg = f"Source type {source_type.value} is not supported in landing stage yet"
        raise NotImplementedError(msg)

    profile = get_profile(source_type)
    new_files = get_new_files(folder, source_type)

    files_processed = 0
    records_inserted = 0

    for file_path in new_files:
        rows = _read_rows(file_path, profile)
        if not rows:
            continue

        valid_rows_json: list[str] = []
        for row in rows:
            to_unified(
                row, profile, str(file_path)
            )  # raises on malformed required data
            valid_rows_json.append(json.dumps(row, ensure_ascii=False, default=str))

        mark_file_processed(
            filename=file_path,
            source=source_type,
            record_count=len(valid_rows_json),
            status="landing",
        )

        with get_connection() as db:
            file_id = transactions.get_latest_ingested_file_id(
                db, filename=file_path.name, source_type=source_type.value
            )

        if file_id is None:
            msg = f"No ingested_files row found for {file_path.name} ({source_type.value})"
            raise RuntimeError(msg)

        now = datetime.now().isoformat()
        lnd_rows = [
            {
                "file_id": file_id,
                "source_type": source_type.value,
                "raw_json": j,
                "created_at": now,
            }
            for j in valid_rows_json
        ]
        with get_connection() as db:
            transactions.insert_transactions_lnd(db, lnd_rows)

        records_inserted += len(lnd_rows)
        files_processed += 1

    return {
        "files_found": len(new_files),
        "files_processed": files_processed,
        "records_inserted": records_inserted,
    }


def run_landing() -> dict[str, dict[str, int]]:
    """Run landing stage for all known source types."""
    results: dict[str, dict[str, int]] = {}

    for source_type in SUPPORTED_SOURCES:
        source_folder = LANDING_ZONE_DIR / source_type.value.lower()
        results[source_type.value] = process_landing_source(source_folder, source_type)

    return results
