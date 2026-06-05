from __future__ import annotations

import hashlib

from datetime import datetime
from pathlib import Path

from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_ingestion.config import LANDING_ZONE_DIR
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.db import create_all_tables
from swiss_exp_tracker.pipeline_ingestion.db import get_connection


TRACKABLE_EXTENSIONS = {".csv", ".xlsx", ".xls"}


def _resolve_file_path(filename: str | Path) -> Path:
    """
    Get full path from given filename.
    """
    path = Path(filename)
    if path.is_absolute():
        return path
    if path.exists():
        return path
    return LANDING_ZONE_DIR / path


def _md5_hash(file_path: Path) -> str:
    """
    Calculate the MD5 hash of a file.
    """
    digest = hashlib.md5(usedforsecurity=False)
    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _list_candidate_files(folder: str | Path) -> list[Path]:
    """
    List all candidate files in the given folder.
    """
    folder_path = Path(folder)
    if not folder_path.is_absolute() and not folder_path.exists():
        folder_path = LANDING_ZONE_DIR / folder_path

    if not folder_path.exists():
        return []

    return sorted(
        [
            path
            for path in folder_path.iterdir()
            if path.is_file() and path.suffix.lower() in TRACKABLE_EXTENSIONS
        ]
    )


def get_new_files(folder: str | Path, source_type: SourceType) -> list[Path]:
    """
    Get a list of new files in the given folder for the specified source type.
    """
    create_all_tables()
    candidate_files = _list_candidate_files(folder)
    if not candidate_files:
        return []

    with get_connection() as db:
        rows = transactions.get_known_files_for_source(
            db, source_type=source_type.value
        )
        known_entries = {(row[0], row[1]) for row in rows}
    new_files: list[Path] = []

    for file_path in candidate_files:
        file_hash = _md5_hash(file_path)
        key = (file_path.name, file_hash)
        if key not in known_entries:
            new_files.append(file_path)

    return new_files


def mark_file_processed(
    filename: str | Path,
    source: SourceType,
    record_count: int,
    status: str,
) -> None:
    """
    Mark a file as processed in the database with the given metadata.
    """
    create_all_tables()
    file_path = _resolve_file_path(filename)
    file_hash = _md5_hash(file_path)

    with get_connection() as db:
        transactions.insert_ingested_file(
            db,
            filename=file_path.name,
            file_hash=file_hash,
            source_type=source.value,
            ingested_at=datetime.now().isoformat(),
            record_count=record_count,
            status=status,
        )
