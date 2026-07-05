from __future__ import annotations

import csv
import json
import sqlite3

from datetime import datetime
from pathlib import Path

from swiss_exp_tracker.pipeline_ingestion.adapters.generic_adapter import to_unified
from swiss_exp_tracker.pipeline_ingestion.data_models.data_sources import get_profile
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    UnifiedTransaction,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_02_raw import (
    process_raw_source,
)

TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "test_data"

# ---------------------------------------------------------------------------
# DB seeding helpers
# ---------------------------------------------------------------------------


def _insert_ingested_file(
    db_path: Path,
    filename: str = "test.csv",
    source_type: str = "ZKB_DEBIT",
) -> int:
    """Insert a minimal ingested_files row and return its id."""
    with sqlite3.connect(db_path) as db:
        db.execute(
            "INSERT INTO ingested_files "
            "(filename, file_hash, source_type, ingested_at, record_count, status) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (filename, "abc123", source_type, datetime.now().isoformat(), 1, "landing"),
        )
        db.commit()
        row = db.execute("SELECT last_insert_rowid()").fetchone()
        return int(row[0])


def _insert_landing_row(
    db_path: Path,
    raw_json: str,
    file_id: int,
    source_type: str = "ZKB_DEBIT",
) -> int:
    """Insert an unprocessed landing row and return its id."""
    with sqlite3.connect(db_path) as db:
        db.execute(
            "INSERT INTO transactions_lnd "
            "(file_id, source_type, raw_json, created_at, processed) "
            "VALUES (?, ?, ?, ?, 0)",
            (file_id, source_type, raw_json, datetime.now().isoformat()),
        )
        db.commit()
        row = db.execute("SELECT last_insert_rowid()").fetchone()
        return int(row[0])


def _load_zkb_row_json() -> str:
    """Load first ZKB row from test CSV as a raw canonical-keyed JSON string."""
    with (TEST_DATA_DIR / "zkb_test.csv").open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        first_row = dict(next(reader))
    return json.dumps(first_row, ensure_ascii=False)


def _load_viseca_row_json() -> str:
    """Load first Viseca row from test CSV as a raw canonical-keyed JSON string."""
    with (TEST_DATA_DIR / "viseca_test.csv").open(
        encoding="utf-8-sig", newline=""
    ) as fh:
        reader = csv.DictReader(fh)
        first_row = dict(next(reader))
    return json.dumps(first_row, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_happy_path_zkb(tmp_db: Path) -> None:
    """Two ZKB landing rows → rows_found=2, records_inserted=2, all lnd processed=1."""
    zkb_json = _load_zkb_row_json()
    file_id = _insert_ingested_file(tmp_db, "test.csv", "ZKB_DEBIT")
    _insert_landing_row(tmp_db, zkb_json, file_id, "ZKB_DEBIT")
    _insert_landing_row(tmp_db, zkb_json, file_id, "ZKB_DEBIT")

    result = process_raw_source(SourceType.ZKB_DEBIT)

    assert result["rows_found"] == 2
    assert result["records_inserted"] == 2

    with sqlite3.connect(tmp_db) as db:
        raw_count = db.execute("SELECT COUNT(*) FROM transactions_raw").fetchone()[0]
        unprocessed = db.execute(
            "SELECT COUNT(*) FROM transactions_lnd WHERE processed=0"
        ).fetchone()[0]
        file_status = db.execute(
            "SELECT status FROM ingested_files WHERE id=?", (file_id,)
        ).fetchone()[0]

    assert raw_count == 2
    assert unprocessed == 0
    assert file_status == "raw"


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_idempotency(tmp_db: Path) -> None:
    """Second run on already-processed rows returns rows_found=0, raw count unchanged."""
    zkb_json = _load_zkb_row_json()
    file_id = _insert_ingested_file(tmp_db, "test.csv", "ZKB_DEBIT")
    _insert_landing_row(tmp_db, zkb_json, file_id, "ZKB_DEBIT")

    process_raw_source(SourceType.ZKB_DEBIT)
    result2 = process_raw_source(SourceType.ZKB_DEBIT)

    assert result2["rows_found"] == 0

    with sqlite3.connect(tmp_db) as db:
        raw_count = db.execute("SELECT COUNT(*) FROM transactions_raw").fetchone()[0]
    assert raw_count == 1


# ---------------------------------------------------------------------------
# File status promotion
# ---------------------------------------------------------------------------


def test_file_status_promoted_only_when_all_lnd_rows_done(tmp_db: Path) -> None:
    """File status advances to 'raw' after all landing rows for that file are processed."""
    zkb_json = _load_zkb_row_json()
    file_id = _insert_ingested_file(tmp_db, "test.csv", "ZKB_DEBIT")
    _insert_landing_row(tmp_db, zkb_json, file_id, "ZKB_DEBIT")
    _insert_landing_row(tmp_db, zkb_json, file_id, "ZKB_DEBIT")

    process_raw_source(SourceType.ZKB_DEBIT)

    with sqlite3.connect(tmp_db) as db:
        status = db.execute(
            "SELECT status FROM ingested_files WHERE id=?", (file_id,)
        ).fetchone()[0]
    assert status == "raw"


# ---------------------------------------------------------------------------
# Raw JSON re-validation
# ---------------------------------------------------------------------------


def test_raw_json_re_validated(tmp_db: Path) -> None:
    """raw_json stored in transactions_raw is parseable by the generic adapter."""
    zkb_json = _load_zkb_row_json()
    file_id = _insert_ingested_file(tmp_db, "test.csv", "ZKB_DEBIT")
    _insert_landing_row(tmp_db, zkb_json, file_id, "ZKB_DEBIT")

    process_raw_source(SourceType.ZKB_DEBIT)

    with sqlite3.connect(tmp_db) as db:
        raw_json_stored = db.execute(
            "SELECT raw_json FROM transactions_raw LIMIT 1"
        ).fetchone()[0]

    parsed = json.loads(raw_json_stored)
    profile = get_profile(SourceType.ZKB_DEBIT)
    unified = to_unified(parsed, profile, "test.csv")
    assert isinstance(unified, UnifiedTransaction)


# ---------------------------------------------------------------------------
# Unprocessed-row filtering
# ---------------------------------------------------------------------------


def test_only_unprocessed_lnd_rows_promoted(tmp_db: Path) -> None:
    """Pre-marked landing rows are skipped; only unprocessed rows are promoted."""
    zkb_json = _load_zkb_row_json()
    file_id = _insert_ingested_file(tmp_db, "test.csv", "ZKB_DEBIT")
    lnd1 = _insert_landing_row(tmp_db, zkb_json, file_id, "ZKB_DEBIT")
    _insert_landing_row(tmp_db, zkb_json, file_id, "ZKB_DEBIT")
    _insert_landing_row(tmp_db, zkb_json, file_id, "ZKB_DEBIT")

    # Pre-mark first row as already processed
    with sqlite3.connect(tmp_db) as db:
        db.execute("UPDATE transactions_lnd SET processed=1 WHERE id=?", (lnd1,))
        db.commit()

    result = process_raw_source(SourceType.ZKB_DEBIT)

    assert result["rows_found"] == 2

    with sqlite3.connect(tmp_db) as db:
        raw_count = db.execute("SELECT COUNT(*) FROM transactions_raw").fetchone()[0]
    assert raw_count == 2


# ---------------------------------------------------------------------------
# Source-type isolation
# ---------------------------------------------------------------------------


def test_source_type_isolation_stage02(tmp_db: Path) -> None:
    """process_raw_source(ZKB_DEBIT) leaves VISECA landing rows unprocessed."""
    zkb_json = _load_zkb_row_json()
    vis_json = _load_viseca_row_json()

    zkb_file_id = _insert_ingested_file(tmp_db, "zkb.csv", "ZKB_DEBIT")
    vis_file_id = _insert_ingested_file(tmp_db, "viseca.csv", "VISECA")

    _insert_landing_row(tmp_db, zkb_json, zkb_file_id, "ZKB_DEBIT")
    vis_lnd_id = _insert_landing_row(tmp_db, vis_json, vis_file_id, "VISECA")

    process_raw_source(SourceType.ZKB_DEBIT)

    with sqlite3.connect(tmp_db) as db:
        zkb_processed = db.execute(
            "SELECT processed FROM transactions_lnd WHERE source_type='ZKB_DEBIT'"
        ).fetchone()[0]
        vis_processed = db.execute(
            "SELECT processed FROM transactions_lnd WHERE id=?", (vis_lnd_id,)
        ).fetchone()[0]

    assert zkb_processed == 1
    assert vis_processed == 0
