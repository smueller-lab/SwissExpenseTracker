from __future__ import annotations

import datetime
import sqlite3

from pathlib import Path

# vcrpy 8.1.1 imports aiohttp.streams.AsyncStreamReaderMixin, which was removed
# in aiohttp 3.12+.  Since these tests play back pre-recorded cassettes and never
# issue real aiohttp requests, adding a no-op shim is enough for the import to
# succeed without affecting test behaviour.
import aiohttp.streams  # pyright: ignore[reportMissingImports]
import pytest

from pydantic import BaseModel

from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_ingestion.db import get_connection

if not hasattr(aiohttp.streams, "AsyncStreamReaderMixin"):
    aiohttp.streams.AsyncStreamReaderMixin = object  # type: ignore[attr-defined]

_PROVIDERS = ("tavily", "exa", "brave_search", "scrape_do")


class ApiUsageRow(BaseModel):
    """Pydantic model representing a single row in the api_usage table."""

    provider: str
    period: str
    used: int
    credit_limit: int
    updated_at: str


def _make_api_usage_row(**overrides: object) -> ApiUsageRow:
    """Return a valid ApiUsageRow with sensible defaults, updated by overrides."""
    base: dict[str, object] = {
        "provider": "tavily",
        "period": datetime.date.today().strftime("%Y-%m"),
        "used": 0,
        "credit_limit": 1000,
        "updated_at": datetime.datetime.now(datetime.UTC).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
    }
    base.update(overrides)
    return ApiUsageRow.model_validate(base)


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


@pytest.fixture()
def seed_api_usage(tmp_api_usage_db: Path) -> dict[str, ApiUsageRow]:
    """Seed one ApiUsageRow per provider into the temp DB; return rows keyed by provider."""
    rows = {provider: _make_api_usage_row(provider=provider) for provider in _PROVIDERS}
    with get_connection() as db:
        for row in rows.values():
            data = row.model_dump()
            transactions.upsert_api_usage(db, **data)
    return rows
