from __future__ import annotations

import hashlib

from datetime import datetime
from pathlib import Path

from swiss_exp_tracker.db.sql import positions
from swiss_exp_tracker.pipeline_ingestion.db_positions import create_positions_tables
from swiss_exp_tracker.pipeline_ingestion.db_positions import get_positions_connection


_TRACKABLE_EXTENSIONS = {".csv", ".xlsx", ".xls"}


def _md5_hash(file_path: Path) -> str:
    digest = hashlib.md5(usedforsecurity=False)
    with file_path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _list_candidate_files(folder: Path) -> list[Path]:
    if not folder.exists():
        return []
    return sorted(
        p
        for p in folder.iterdir()
        if p.is_file() and p.suffix.lower() in _TRACKABLE_EXTENSIONS
    )


def get_new_positions_files(folder: Path) -> list[Path]:
    create_positions_tables()
    candidates = _list_candidate_files(folder)
    if not candidates:
        return []

    with get_positions_connection() as db:
        rows = positions.get_known_positions_files(db)
        known = {(str(r[0]), str(r[1])) for r in rows}
    return [f for f in candidates if (f.name, _md5_hash(f)) not in known]


def mark_positions_file_processed(
    file_path: Path,
    record_count: int,
    status: str,
) -> None:
    create_positions_tables()
    file_hash = _md5_hash(file_path)

    with get_positions_connection() as db:
        positions.insert_ingested_file_pos(
            db,
            filename=file_path.name,
            file_hash=file_hash,
            ingested_at=datetime.now().isoformat(),
            record_count=record_count,
            status=status,
        )


def get_latest_positions_file_id(filename: str) -> int:
    """Return the most recent ingested_files_pos id for filename; raises RuntimeError if absent."""
    with get_positions_connection() as db:
        result = positions.get_latest_positions_file_id(db, filename=filename)

    if result is None:
        msg = f"No ingested_files_pos row found for {filename!r}"
        raise RuntimeError(msg)

    return int(result)
