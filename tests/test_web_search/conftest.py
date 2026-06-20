from __future__ import annotations

import sqlite3

from pathlib import Path

# vcrpy 8.1.1 imports aiohttp.streams.AsyncStreamReaderMixin, which was removed
# in aiohttp 3.12+.  Since these tests play back pre-recorded cassettes and never
# issue real aiohttp requests, adding a no-op shim is enough for the import to
# succeed without affecting test behaviour.
import aiohttp.streams
import pytest

from swiss_exp_tracker.db.sql import transactions

if not hasattr(aiohttp.streams, "AsyncStreamReaderMixin"):
    aiohttp.streams.AsyncStreamReaderMixin = object  # type: ignore[attr-defined]


@pytest.fixture(autouse=True)
def tmp_api_usage_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Temp SQLite DB with the api_usage table; patches the pipeline DB path."""
    db_path = tmp_path / "test.db"
    with sqlite3.connect(db_path) as db:
        transactions.create_api_usage_table(db)
        db.commit()

    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_ingestion.db.INGESTION_DB_PATH", db_path
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_ingestion.config.INGESTION_DB_PATH", db_path
    )
    return db_path
