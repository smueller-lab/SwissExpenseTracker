from __future__ import annotations

import json
import sqlite3

from pathlib import Path

import pytest

from swiss_exp_tracker.pipeline_ingestion.stages.groceries.stage_03_rfn import (
    run_groceries_rfn,
)


def _insert_file(db: sqlite3.Connection, filename: str = "test.csv") -> int:
    db.execute(
        """
        INSERT INTO ingested_files (filename, file_hash, source_type, ingested_at, record_count, status)
        VALUES (?, 'abc', 'MIGROS_GROCERY', datetime('now'), 1, 'raw')
        """,
        (filename,),
    )
    db.commit()
    return int(db.execute("SELECT last_insert_rowid()").fetchone()[0])


def _insert_landing(
    db: sqlite3.Connection, file_id: int, raw_json: str, is_bonus: int = 0
) -> int:
    db.execute(
        """
        INSERT INTO groceries_lnd (file_id, source_type, raw_json, is_bonus_row, created_at, processed)
        VALUES (?, 'MIGROS_GROCERY', ?, ?, datetime('now'), 0)
        """,
        (file_id, raw_json, is_bonus),
    )
    db.commit()
    return int(db.execute("SELECT last_insert_rowid()").fetchone()[0])


def _insert_raw(
    db: sqlite3.Connection,
    landing_id: int,
    raw_json: str,
    source_file: str = "test.csv",
) -> int:
    db.execute(
        """
        INSERT INTO groceries_raw (landing_id, source_type, raw_json, source_file, created_at, processed)
        VALUES (?, 'MIGROS_GROCERY', ?, ?, datetime('now'), 0)
        """,
        (landing_id, raw_json, source_file),
    )
    db.commit()
    return int(db.execute("SELECT last_insert_rowid()").fetchone()[0])


def _make_raw_json(
    article: str = "Erdbeeren",
    quantity: float = 1.0,
    price: float = 3.00,
    discount: float = 0.0,
    date: str = "2026-05-17",
    time_str: str = "09:04:29",
    location: str = "location_1",
) -> str:
    return json.dumps(
        {
            "date": date,
            "time": time_str,
            "location": location,
            "article": article,
            "quantity": quantity,
            "discount": discount,
            "price": price,
        }
    )


def test_rfn_inserts_single_row(tmp_db: Path) -> None:
    with sqlite3.connect(tmp_db) as db:
        fid = _insert_file(db)
        lid = _insert_landing(db, fid, _make_raw_json())
        _insert_raw(db, lid, _make_raw_json())

    result = run_groceries_rfn()

    assert result["records_inserted"] == 1
    assert result["duplicates_skipped"] == 0

    with sqlite3.connect(tmp_db) as db:
        rows = db.execute(
            "SELECT article, quantity, price_chf FROM groceries_rfn"
        ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "Erdbeeren"


def test_rfn_nets_partial_return(tmp_db: Path) -> None:
    with sqlite3.connect(tmp_db) as db:
        fid = _insert_file(db)
        lid = _insert_landing(db, fid, _make_raw_json(quantity=2.0, price=2.50))
        _insert_raw(db, lid, _make_raw_json(quantity=2.0, price=2.50))

        lid2 = _insert_landing(db, fid, _make_raw_json(quantity=-1.0, price=-1.25))
        _insert_raw(db, lid2, _make_raw_json(quantity=-1.0, price=-1.25))

    result = run_groceries_rfn()

    assert result["records_inserted"] == 1
    with sqlite3.connect(tmp_db) as db:
        row = db.execute("SELECT quantity, price_chf FROM groceries_rfn").fetchone()
    assert row[0] == pytest.approx(1.0)
    assert row[1] == pytest.approx(1.25)


def test_rfn_discards_full_return(tmp_db: Path) -> None:
    with sqlite3.connect(tmp_db) as db:
        fid = _insert_file(db)
        lid = _insert_landing(db, fid, _make_raw_json(quantity=1.0, price=1.50))
        _insert_raw(db, lid, _make_raw_json(quantity=1.0, price=1.50))
        lid2 = _insert_landing(db, fid, _make_raw_json(quantity=-1.0, price=-1.50))
        _insert_raw(db, lid2, _make_raw_json(quantity=-1.0, price=-1.50))

    result = run_groceries_rfn()

    assert result["records_inserted"] == 0
    with sqlite3.connect(tmp_db) as db:
        count = db.execute("SELECT COUNT(*) FROM groceries_rfn").fetchone()[0]
    assert count == 0


def test_rfn_skips_duplicate(tmp_db: Path) -> None:
    with sqlite3.connect(tmp_db) as db:
        fid = _insert_file(db, "file1.csv")
        lid = _insert_landing(db, fid, _make_raw_json())
        _insert_raw(db, lid, _make_raw_json())

    run_groceries_rfn()

    with sqlite3.connect(tmp_db) as db:
        fid2 = _insert_file(db, "file2.csv")
        lid2 = _insert_landing(db, fid2, _make_raw_json())
        _insert_raw(db, lid2, _make_raw_json())
        db.execute("UPDATE groceries_raw SET processed = 0 WHERE id = ?", (2,))
        db.commit()

    result = run_groceries_rfn()

    assert result["duplicates_skipped"] == 1
    with sqlite3.connect(tmp_db) as db:
        count = db.execute("SELECT COUNT(*) FROM groceries_rfn").fetchone()[0]
    assert count == 1


def test_rfn_marks_raw_rows_processed(tmp_db: Path) -> None:
    with sqlite3.connect(tmp_db) as db:
        fid = _insert_file(db)
        lid = _insert_landing(db, fid, _make_raw_json())
        _insert_raw(db, lid, _make_raw_json())

    run_groceries_rfn()

    with sqlite3.connect(tmp_db) as db:
        unprocessed = db.execute(
            "SELECT COUNT(*) FROM groceries_raw WHERE processed = 0"
        ).fetchone()[0]
    assert unprocessed == 0


def test_rfn_returns_zero_when_no_rows(tmp_db: Path) -> None:
    result = run_groceries_rfn()
    assert result["rows_found"] == 0
    assert result["records_inserted"] == 0


def test_rfn_bonus_rows_not_inserted(tmp_db: Path) -> None:
    bonus_json = json.dumps(
        {
            "date": "2026-05-17",
            "time": "09:04:29",
            "location": "location_1",
            "article": "Cumulus Bonus",
            "quantity": 1.0,
            "discount": 0.0,
            "price": 0.0,
        }
    )
    with sqlite3.connect(tmp_db) as db:
        fid = _insert_file(db)
        lid = _insert_landing(db, fid, bonus_json, is_bonus=1)
        _insert_raw(db, lid, bonus_json)

    result = run_groceries_rfn()

    assert result["records_inserted"] == 0


def test_rfn_sets_enrichment_status_pending(tmp_db: Path) -> None:
    with sqlite3.connect(tmp_db) as db:
        fid = _insert_file(db)
        lid = _insert_landing(db, fid, _make_raw_json())
        _insert_raw(db, lid, _make_raw_json())

    run_groceries_rfn()

    with sqlite3.connect(tmp_db) as db:
        status = db.execute("SELECT enrichment_status FROM groceries_rfn").fetchone()[0]
    assert status == "pending"


def test_rfn_kg_unit_for_decimal_quantity(tmp_db: Path) -> None:
    with sqlite3.connect(tmp_db) as db:
        fid = _insert_file(db)
        raw = _make_raw_json(article="Butter", quantity=0.25, price=2.10)
        lid = _insert_landing(db, fid, raw)
        _insert_raw(db, lid, raw)

    run_groceries_rfn()

    with sqlite3.connect(tmp_db) as db:
        unit = db.execute("SELECT unit FROM groceries_rfn").fetchone()[0]
    assert unit == "kg"
