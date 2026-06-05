"""Tests for named aiosql queries in groceries.sql."""

from __future__ import annotations

import json
import sqlite3

from pathlib import Path

from swiss_exp_tracker.db.sql import groceries as gq
from swiss_exp_tracker.db.sql import transactions


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _make_file_row(**overrides: object) -> dict[str, object]:
    """Return a valid ingested_files insert dict."""
    base: dict[str, object] = {
        "filename": "coop_2026.csv",
        "file_hash": "hash001",
        "source_type": "COOP",
        "ingested_at": "2026-01-01T10:00:00",
        "record_count": 5,
        "status": "done",
    }
    base.update(overrides)
    return base


def _make_lnd_row(**overrides: object) -> dict[str, object]:
    """Return a valid groceries_lnd insert dict."""
    base: dict[str, object] = {
        "file_id": 1,
        "source_type": "COOP",
        "raw_json": json.dumps({"article": "Banana", "price": "0.99"}),
        "is_bonus_row": 0,
        "created_at": "2026-01-01T10:00:00",
    }
    base.update(overrides)
    return base


def _make_raw_row(**overrides: object) -> dict[str, object]:
    """Return a valid groceries_raw insert dict."""
    base: dict[str, object] = {
        "landing_id": 1,
        "source_type": "COOP",
        "raw_json": json.dumps({"article": "Banana"}),
        "source_file": "coop_2026.csv",
        "created_at": "2026-01-01T10:00:00",
    }
    base.update(overrides)
    return base


def _make_rfn_row(**overrides: object) -> dict[str, object]:
    """Return a valid groceries_rfn insert dict."""
    base: dict[str, object] = {
        "raw_id": 1,
        "source_type": "COOP",
        "date": "2026-01-15",
        "time": "10:30:00",
        "location": "Coop Basel",
        "article": "Banana",
        "article_normalized": "banana",
        "unit": "kg",
        "quantity": 1.0,
        "price_chf": 0.99,
        "discount_chf": 0.0,
        "created_at": "2026-01-01T10:00:00",
    }
    base.update(overrides)
    return base


def _insert_file(db: sqlite3.Connection, **overrides: object) -> None:
    """Insert one ingested_files row via transactions namespace."""
    transactions.insert_ingested_file(db, **_make_file_row(**overrides))
    db.commit()


def _insert_lnd(db: sqlite3.Connection, **overrides: object) -> int:
    """Insert one groceries_lnd row and return its id."""
    gq.insert_groceries_lnd(db, [_make_lnd_row(**overrides)])
    db.commit()
    return int(
        db.execute("SELECT id FROM groceries_lnd ORDER BY id DESC LIMIT 1").fetchone()[
            0
        ]
    )


def _insert_raw(db: sqlite3.Connection, **overrides: object) -> int:
    """Insert one groceries_raw row and return its id."""
    gq.insert_groceries_raw(db, **_make_raw_row(**overrides))
    db.commit()
    return int(
        db.execute("SELECT id FROM groceries_raw ORDER BY id DESC LIMIT 1").fetchone()[
            0
        ]
    )


def _insert_rfn(db: sqlite3.Connection, **overrides: object) -> int:
    """Insert one groceries_rfn row and return its id."""
    gq.insert_groceries_rfn(db, **_make_rfn_row(**overrides))
    db.commit()
    return int(
        db.execute("SELECT id FROM groceries_rfn ORDER BY id DESC LIMIT 1").fetchone()[
            0
        ]
    )


def _insert_categorization(
    db: sqlite3.Connection,
    rfn_id: int,
    category_main: str = "Food",
    category_detail: str = "Fruit",
) -> None:
    """Insert one grocery_categorization_raw row for rfn_id."""
    db.execute(
        """INSERT INTO grocery_categorization_raw
               (created_at, rfn_id, article, matched_article, cache_hit,
                similarity, category_main, category_detail)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "2026-01-01T10:00:00",
            rfn_id,
            "Banana",
            "banana",
            1,
            1.0,
            category_main,
            category_detail,
        ),
    )
    db.commit()


# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------


def test_create_groceries_schema_creates_all_tables(tmp_db: Path) -> None:
    """DDL functions create all groceries tables and the categorization view."""
    with sqlite3.connect(tmp_db) as db:
        gq.create_groceries_lnd_table(db)
        gq.create_groceries_raw_table(db)
        gq.create_groceries_rfn_table(db)
        gq.create_groceries_use_table(db)
        gq.create_grocery_categorization_raw_table(db)
        gq.create_grocery_categorization_rfn_view(db)
        db.commit()

        tables = {
            r[0]
            for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
            ).fetchall()
        }

    expected = {
        "groceries_lnd",
        "groceries_raw",
        "groceries_rfn",
        "groceries_use",
        "grocery_categorization_raw",
        "grocery_categorization_rfn",
    }
    assert expected <= tables


# ---------------------------------------------------------------------------
# insert_groceries_lnd
# ---------------------------------------------------------------------------


def test_insert_groceries_lnd_batch(tmp_db: Path) -> None:
    """Batch-inserting 2 rows into groceries_lnd yields COUNT = 2."""
    with sqlite3.connect(tmp_db) as db:
        gq.create_groceries_lnd_table(db)
        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="coop_2026.csv", source_type="COOP"
        )
        rows = [_make_lnd_row(file_id=file_id) for _ in range(2)]
        gq.insert_groceries_lnd(db, rows)
        db.commit()

        count = db.execute("SELECT COUNT(*) FROM groceries_lnd").fetchone()[0]

    assert count == 2


# ---------------------------------------------------------------------------
# get_unprocessed_groceries_lnd_rows
# ---------------------------------------------------------------------------


def test_get_unprocessed_groceries_lnd_rows_respects_source_type(
    tmp_db: Path,
) -> None:
    """Only rows matching the requested source_type are returned."""
    with sqlite3.connect(tmp_db) as db:
        gq.create_groceries_lnd_table(db)

        # COOP file
        _insert_file(db, source_type="COOP", filename="coop_2026.csv", file_hash="h1")
        coop_id = transactions.get_latest_ingested_file_id(
            db, filename="coop_2026.csv", source_type="COOP"
        )

        # MIGROS file
        _insert_file(
            db, source_type="MIGROS", filename="migros_2026.csv", file_hash="h2"
        )
        mig_id = transactions.get_latest_ingested_file_id(
            db, filename="migros_2026.csv", source_type="MIGROS"
        )

        gq.insert_groceries_lnd(
            db, [_make_lnd_row(file_id=coop_id, source_type="COOP")]
        )
        gq.insert_groceries_lnd(
            db, [_make_lnd_row(file_id=mig_id, source_type="MIGROS")]
        )
        db.commit()

        rows = list(gq.get_unprocessed_groceries_lnd_rows(db, source_type="COOP"))

    assert len(rows) == 1
    # The 4th column (index 3) is the filename from the join
    assert "coop" in rows[0][4].lower()


# ---------------------------------------------------------------------------
# mark_groceries_lnd_processed
# ---------------------------------------------------------------------------


def test_mark_groceries_lnd_processed_works(tmp_db: Path) -> None:
    """mark_groceries_lnd_processed sets processed = 1 for the given id."""
    with sqlite3.connect(tmp_db) as db:
        gq.create_groceries_lnd_table(db)
        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="coop_2026.csv", source_type="COOP"
        )
        lnd_id = _insert_lnd(db, file_id=file_id)
        gq.mark_groceries_lnd_processed(db, landing_id=lnd_id)
        db.commit()

        processed = db.execute(
            "SELECT processed FROM groceries_lnd WHERE id = ?", (lnd_id,)
        ).fetchone()[0]

    assert processed == 1


# ---------------------------------------------------------------------------
# check_duplicate_groceries_rfn
# ---------------------------------------------------------------------------


def test_check_duplicate_groceries_rfn_finds_existing(tmp_db: Path) -> None:
    """check_duplicate_groceries_rfn returns 1 when a matching row exists."""
    with sqlite3.connect(tmp_db) as db:
        gq.create_groceries_lnd_table(db)
        gq.create_groceries_raw_table(db)
        gq.create_groceries_rfn_table(db)

        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="coop_2026.csv", source_type="COOP"
        )
        lnd_id = _insert_lnd(db, file_id=file_id)
        raw_id = _insert_raw(db, landing_id=lnd_id)
        _insert_rfn(
            db,
            raw_id=raw_id,
            date="2026-01-15",
            time="10:30:00",
            location="Coop Basel",
            article="Banana",
            quantity=1.0,
        )

        result = gq.check_duplicate_groceries_rfn(
            db,
            date="2026-01-15",
            time="10:30:00",
            location="Coop Basel",
            article="Banana",
            quantity=1.0,
        )

    assert result == 1


def test_check_duplicate_groceries_rfn_no_false_positive(tmp_db: Path) -> None:
    """check_duplicate_groceries_rfn returns None when no matching row exists."""
    with sqlite3.connect(tmp_db) as db:
        gq.create_groceries_rfn_table(db)

        result = gq.check_duplicate_groceries_rfn(
            db,
            date="2099-12-31",
            time="23:59:59",
            location="NOWHERE",
            article="NOTHING",
            quantity=0.0,
        )

    assert result is None


# ---------------------------------------------------------------------------
# insert_groceries_rfn
# ---------------------------------------------------------------------------


def test_insert_groceries_rfn_happy_path(tmp_db: Path) -> None:
    """insert_groceries_rfn stores the row in groceries_rfn."""
    with sqlite3.connect(tmp_db) as db:
        gq.create_groceries_lnd_table(db)
        gq.create_groceries_raw_table(db)
        gq.create_groceries_rfn_table(db)

        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="coop_2026.csv", source_type="COOP"
        )
        lnd_id = _insert_lnd(db, file_id=file_id)
        raw_id = _insert_raw(db, landing_id=lnd_id)
        rfn_id = _insert_rfn(db, raw_id=raw_id)

        row = db.execute(
            "SELECT article FROM groceries_rfn WHERE id = ?", (rfn_id,)
        ).fetchone()

    assert row is not None
    assert row[0] == "Banana"


# ---------------------------------------------------------------------------
# get_file_id_for_groceries_raw_row
# ---------------------------------------------------------------------------


def test_get_file_id_for_groceries_raw_row_correct(tmp_db: Path) -> None:
    """get_file_id_for_groceries_raw_row returns the scalar file_id."""
    with sqlite3.connect(tmp_db) as db:
        gq.create_groceries_lnd_table(db)
        gq.create_groceries_raw_table(db)

        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="coop_2026.csv", source_type="COOP"
        )
        lnd_id = _insert_lnd(db, file_id=file_id)
        raw_id = _insert_raw(db, landing_id=lnd_id)

        result = gq.get_file_id_for_groceries_raw_row(db, raw_id=raw_id)

    assert result == file_id


# ---------------------------------------------------------------------------
# get_groceries_for_use
# ---------------------------------------------------------------------------


def test_get_groceries_for_use_skips_already_inserted(tmp_db: Path) -> None:
    """get_groceries_for_use excludes rfn rows whose id is already in groceries_use."""
    with sqlite3.connect(tmp_db) as db:
        gq.create_groceries_lnd_table(db)
        gq.create_groceries_raw_table(db)
        gq.create_groceries_rfn_table(db)
        gq.create_groceries_use_table(db)
        gq.create_grocery_categorization_raw_table(db)
        gq.create_grocery_categorization_rfn_view(db)

        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="coop_2026.csv", source_type="COOP"
        )
        lnd_id = _insert_lnd(db, file_id=file_id)
        raw_id = _insert_raw(db, landing_id=lnd_id)
        rfn_id = _insert_rfn(db, raw_id=raw_id)

        # Enrich the rfn row
        gq.mark_grocery_enriched(db, rfn_id=rfn_id)
        _insert_categorization(db, rfn_id=rfn_id)

        # Insert into groceries_use
        gq.insert_groceries_use(
            db,
            rfn_id=rfn_id,
            date="2026-01-15",
            time="10:30:00",
            location="Coop Basel",
            article="Banana",
            unit="kg",
            quantity=1.0,
            price_chf=0.99,
            discount_chf=0.0,
            category_main="Food",
            category_detail="Fruit",
        )
        db.commit()

        rows = gq.get_groceries_for_use(db)

    # Already in use, should not reappear
    assert all(r[0] != rfn_id for r in rows)


# ---------------------------------------------------------------------------
# apply_groceries_use_category_correction
# ---------------------------------------------------------------------------


def test_apply_groceries_use_category_correction(tmp_db: Path) -> None:
    """apply_groceries_use_category_correction updates matching articles."""
    with sqlite3.connect(tmp_db) as db:
        gq.create_groceries_lnd_table(db)
        gq.create_groceries_raw_table(db)
        gq.create_groceries_rfn_table(db)
        gq.create_groceries_use_table(db)
        gq.create_grocery_categorization_raw_table(db)
        gq.create_grocery_categorization_rfn_view(db)

        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="coop_2026.csv", source_type="COOP"
        )
        lnd_id = _insert_lnd(db, file_id=file_id)
        raw_id = _insert_raw(db, landing_id=lnd_id)
        rfn_id = _insert_rfn(db, raw_id=raw_id)
        gq.mark_grocery_enriched(db, rfn_id=rfn_id)
        _insert_categorization(
            db, rfn_id=rfn_id, category_main="WrongCat", category_detail="WrongDetail"
        )

        gq.insert_groceries_use(
            db,
            rfn_id=rfn_id,
            date="2026-01-15",
            time="10:30:00",
            location="Coop Basel",
            article="Banana",
            unit="kg",
            quantity=1.0,
            price_chf=0.99,
            discount_chf=0.0,
            category_main="WrongCat",
            category_detail="WrongDetail",
        )
        db.commit()

        gq.apply_groceries_use_category_correction(
            db,
            category_main="Food",
            category_detail="Fruit",
            pattern="Bana%",
        )
        db.commit()

        row = db.execute(
            "SELECT category_main, category_detail FROM groceries_use WHERE rfn_id = ?",
            (rfn_id,),
        ).fetchone()

    assert row[0] == "Food"
    assert row[1] == "Fruit"
