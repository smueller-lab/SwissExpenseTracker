from __future__ import annotations

import hashlib

from datetime import datetime
from pathlib import Path

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
        rows = db.execute(
            "SELECT filename, file_hash FROM ingested_files_pos"
        ).fetchall()

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
        db.execute(
            """
            INSERT OR IGNORE INTO ingested_files_pos (
                filename, file_hash, ingested_at, record_count, status
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                file_path.name,
                file_hash,
                datetime.now().isoformat(),
                record_count,
                status,
            ),
        )


def get_latest_positions_file_id(filename: str) -> int:
    with get_positions_connection() as db:
        row = db.execute(
            """
            SELECT id FROM ingested_files_pos
            WHERE filename = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (filename,),
        ).fetchone()

    if row is None:
        msg = f"No ingested_files_pos row found for {filename!r}"
        raise RuntimeError(msg)

    return int(row[0])
