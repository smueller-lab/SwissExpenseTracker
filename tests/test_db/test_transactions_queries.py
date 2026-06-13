"""Tests for named aiosql queries in transactions.sql."""

from __future__ import annotations

import json
import sqlite3

from pathlib import Path

import pytest

from swiss_exp_tracker.db.sql import transactions

# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _make_file_row(**overrides: object) -> dict[str, object]:
    """Return a valid ingested_files insert dict."""
    base: dict[str, object] = {
        "filename": "export_2026.csv",
        "file_hash": "abc123",
        "source_type": "ZKB_DEBIT",
        "ingested_at": "2026-01-01T10:00:00",
        "record_count": 10,
        "status": "done",
    }
    base.update(overrides)
    return base


def _make_lnd_row(**overrides: object) -> dict[str, object]:
    """Return a valid transactions_lnd insert dict."""
    base: dict[str, object] = {
        "file_id": 1,
        "source_type": "ZKB_DEBIT",
        "raw_json": json.dumps({"Amount": "10.00", "Date": "2026-01-15"}),
        "created_at": "2026-01-01T10:00:00",
    }
    base.update(overrides)
    return base


def _make_raw_row(**overrides: object) -> dict[str, object]:
    """Return a valid transactions_raw insert dict."""
    base: dict[str, object] = {
        "landing_id": 1,
        "source_type": "ZKB_DEBIT",
        "raw_json": json.dumps({"Amount": "10.00"}),
        "source_file": "export_2026.csv",
        "created_at": "2026-01-01T10:00:00",
    }
    base.update(overrides)
    return base


def _make_rfn_row(**overrides: object) -> dict[str, object]:
    """Return a valid transactions_rfn insert dict."""
    base: dict[str, object] = {
        "raw_id": 1,
        "source_type": "ZKB_DEBIT",
        "date": "2026-01-15",
        "amount": 42.50,
        "transaction_type": "EXPENSE",
        "booking_text": "Freshmart",
        "merchant_normalized": "Freshmart",
        "is_person": 0,
        "currency": "CHF",
        "reference": "REF-001",
        "enrichment_status": "pending",
        "created_at": "2026-01-01T10:00:00",
        "balance_chf": 1000.0,
    }
    base.update(overrides)
    return base


def _insert_file(db: sqlite3.Connection, **overrides: object) -> None:
    """Insert one ingested_files row."""
    transactions.insert_ingested_file(db, **_make_file_row(**overrides))
    db.commit()


def _insert_lnd(db: sqlite3.Connection, **overrides: object) -> None:
    """Insert one transactions_lnd row."""
    transactions.insert_transactions_lnd(db, [_make_lnd_row(**overrides)])
    db.commit()


def _insert_raw(db: sqlite3.Connection, **overrides: object) -> None:
    """Insert one transactions_raw row."""
    transactions.insert_transactions_raw(db, **_make_raw_row(**overrides))
    db.commit()


def _insert_rfn(db: sqlite3.Connection, **overrides: object) -> None:
    """Insert one transactions_rfn row."""
    transactions.insert_transactions_rfn(db, **_make_rfn_row(**overrides))
    db.commit()


# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------


def test_create_schema_creates_all_tables(tmp_db: Path) -> None:
    """DDL queries create all expected tables in the database."""
    with sqlite3.connect(tmp_db) as db:
        rows = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = {r[0] for r in rows}

    expected = {
        "ingested_files",
        "transactions_lnd",
        "transactions_raw",
        "transactions_rfn",
        "api_usage",
    }
    assert expected <= table_names


# ---------------------------------------------------------------------------
# insert_ingested_file / get_latest_ingested_file_id
# ---------------------------------------------------------------------------


def test_insert_and_fetch_ingested_file(tmp_db: Path) -> None:
    """insert_ingested_file persists a row; get_latest_ingested_file_id returns its int id."""
    with sqlite3.connect(tmp_db) as db:
        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="export_2026.csv", source_type="ZKB_DEBIT"
        )

    assert isinstance(file_id, int)
    assert file_id >= 1


def test_insert_ingested_file_is_idempotent(tmp_db: Path) -> None:
    """OR IGNORE means inserting the same row twice leaves exactly one row."""
    with sqlite3.connect(tmp_db) as db:
        _insert_file(db)
        _insert_file(db)
        count = db.execute("SELECT COUNT(*) FROM ingested_files").fetchone()[0]

    assert count == 1


# ---------------------------------------------------------------------------
# insert_transactions_lnd / get_unprocessed_landing_rows
# ---------------------------------------------------------------------------


def test_insert_transactions_lnd_batch(tmp_db: Path) -> None:
    """Batch-inserting 3 rows into transactions_lnd yields COUNT = 3."""
    with sqlite3.connect(tmp_db) as db:
        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="export_2026.csv", source_type="ZKB_DEBIT"
        )
        rows = [
            _make_lnd_row(file_id=file_id, raw_json=json.dumps({"n": i}))
            for i in range(3)
        ]
        transactions.insert_transactions_lnd(db, rows)
        db.commit()

        count = db.execute("SELECT COUNT(*) FROM transactions_lnd").fetchone()[0]

    assert count == 3


def test_get_unprocessed_landing_rows_empty_when_all_processed(tmp_db: Path) -> None:
    """get_unprocessed_landing_rows returns [] after marking all rows processed."""
    with sqlite3.connect(tmp_db) as db:
        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="export_2026.csv", source_type="ZKB_DEBIT"
        )
        _insert_lnd(db, file_id=file_id)
        lnd_id = db.execute(
            "SELECT id FROM transactions_lnd ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
        transactions.mark_lnd_processed(db, landing_id=lnd_id)
        db.commit()

        result = list(
            transactions.get_unprocessed_landing_rows(db, source_type="ZKB_DEBIT")
        )

    assert result == []


# ---------------------------------------------------------------------------
# insert_transactions_raw
# ---------------------------------------------------------------------------


def test_insert_transactions_raw(tmp_db: Path) -> None:
    """insert_transactions_raw stores the row and it can be retrieved."""
    with sqlite3.connect(tmp_db) as db:
        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="export_2026.csv", source_type="ZKB_DEBIT"
        )
        _insert_lnd(db, file_id=file_id)
        lnd_id = db.execute("SELECT id FROM transactions_lnd LIMIT 1").fetchone()[0]
        _insert_raw(db, landing_id=lnd_id)

        count = db.execute("SELECT COUNT(*) FROM transactions_raw").fetchone()[0]

    assert count == 1


# ---------------------------------------------------------------------------
# count_unprocessed_lnd_for_file
# ---------------------------------------------------------------------------


def test_count_unprocessed_lnd_for_file_correct(tmp_db: Path) -> None:
    """count_unprocessed_lnd_for_file returns a plain int equal to unprocessed rows."""
    with sqlite3.connect(tmp_db) as db:
        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="export_2026.csv", source_type="ZKB_DEBIT"
        )
        # 2 unprocessed + 1 processed
        for _ in range(3):
            transactions.insert_transactions_lnd(db, [_make_lnd_row(file_id=file_id)])
        # mark the last one processed
        last_id = db.execute(
            "SELECT id FROM transactions_lnd ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
        transactions.mark_lnd_processed(db, landing_id=last_id)
        db.commit()

        result = transactions.count_unprocessed_lnd_for_file(db, file_id=file_id)

    assert result == 2


# ---------------------------------------------------------------------------
# check_duplicate_rfn
# ---------------------------------------------------------------------------


def test_check_duplicate_rfn_with_date_finds_existing(tmp_db: Path) -> None:
    """check_duplicate_rfn returns 1 when a matching rfn row exists."""
    with sqlite3.connect(tmp_db) as db:
        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="export_2026.csv", source_type="ZKB_DEBIT"
        )
        _insert_lnd(db, file_id=file_id)
        lnd_id = db.execute("SELECT id FROM transactions_lnd LIMIT 1").fetchone()[0]
        _insert_raw(db, landing_id=lnd_id)
        raw_id = db.execute("SELECT id FROM transactions_raw LIMIT 1").fetchone()[0]
        _insert_rfn(
            db, raw_id=raw_id, date="2026-01-15", amount=42.50, reference="REF-001"
        )

        result = transactions.check_duplicate_rfn(
            db, reference="REF-001", date="2026-01-15", amount=42.50
        )

    assert result == 1


def test_check_duplicate_rfn_null_date_finds_existing(tmp_db: Path) -> None:
    """check_duplicate_rfn COALESCE logic handles None date correctly."""
    with sqlite3.connect(tmp_db) as db:
        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="export_2026.csv", source_type="ZKB_DEBIT"
        )
        _insert_lnd(db, file_id=file_id)
        lnd_id = db.execute("SELECT id FROM transactions_lnd LIMIT 1").fetchone()[0]
        _insert_raw(db, landing_id=lnd_id)
        raw_id = db.execute("SELECT id FROM transactions_raw LIMIT 1").fetchone()[0]
        _insert_rfn(db, raw_id=raw_id, date=None, amount=99.99, reference="REF-NULL")

        result = transactions.check_duplicate_rfn(
            db, reference="REF-NULL", date=None, amount=99.99
        )

    assert result == 1


def test_check_duplicate_rfn_no_false_positive(tmp_db: Path) -> None:
    """check_duplicate_rfn returns None when no matching row exists."""
    with sqlite3.connect(tmp_db) as db:
        result = transactions.check_duplicate_rfn(
            db, reference="NONEXISTENT", date="2026-01-15", amount=10.00
        )

    assert result is None


# ---------------------------------------------------------------------------
# upsert_api_usage / get_api_usage
# ---------------------------------------------------------------------------


def test_upsert_api_usage_creates_and_updates(tmp_db: Path) -> None:
    """upsert_api_usage creates a row and subsequent upsert updates used."""
    with sqlite3.connect(tmp_db) as db:
        transactions.upsert_api_usage(
            db,
            provider="openai",
            period="2026-01",
            used=5,
            credit_limit=100,
            updated_at="2026-01-01T10:00:00",
        )
        db.commit()

        transactions.upsert_api_usage(
            db,
            provider="openai",
            period="2026-01",
            used=12,
            credit_limit=100,
            updated_at="2026-01-01T11:00:00",
        )
        db.commit()

        result = transactions.get_api_usage(db, provider="openai", period="2026-01")

    assert result == 12


def test_get_api_usage_returns_none_when_absent(tmp_db: Path) -> None:
    """get_api_usage returns None (not a tuple) when no matching row exists."""
    with sqlite3.connect(tmp_db) as db:
        result = transactions.get_api_usage(db, provider="missing", period="2026-01")

    assert result is None


# ---------------------------------------------------------------------------
# get_zkb_credit_card_payment_rows
# ---------------------------------------------------------------------------


def test_get_zkb_credit_card_payment_rows_filters_correctly(tmp_db: Path) -> None:
    """Only rows with 'Viseca Payment' in booking_text are returned."""
    with sqlite3.connect(tmp_db) as db:
        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="export_2026.csv", source_type="ZKB_DEBIT"
        )
        _insert_lnd(db, file_id=file_id)
        lnd_id = db.execute("SELECT id FROM transactions_lnd LIMIT 1").fetchone()[0]
        _insert_raw(db, landing_id=lnd_id)
        raw_id = db.execute("SELECT id FROM transactions_raw LIMIT 1").fetchone()[0]

        # Matching row
        _insert_rfn(
            db,
            raw_id=raw_id,
            booking_text="Viseca Payment 2026-01",
            transaction_type="EXPENSE",
            reference="REF-VIS",
            amount=200.00,
        )
        # Non-matching row
        _insert_rfn(
            db,
            raw_id=raw_id,
            booking_text='"Freshmart" purchase',
            transaction_type="EXPENSE",
            reference="REF-MIG",
            amount=25.00,
        )
        db.commit()

        rows = list(
            transactions.get_zkb_credit_card_payment_rows(
                db, source_type="ZKB_DEBIT", transaction_type="EXPENSE"
            )
        )

    assert len(rows) == 1
    assert rows[0][1] == pytest.approx(200.00)


# ---------------------------------------------------------------------------
# fill_viseca_fee_text
# ---------------------------------------------------------------------------


def test_fill_viseca_fee_text_updates_correct_rows(tmp_db: Path) -> None:
    """fill_viseca_fee_text only updates rows with NULL/empty booking_text."""
    with sqlite3.connect(tmp_db) as db:
        _insert_file(db)
        file_id = transactions.get_latest_ingested_file_id(
            db, filename="export_2026.csv", source_type="ZKB_DEBIT"
        )
        _insert_lnd(db, file_id=file_id)
        lnd_id = db.execute("SELECT id FROM transactions_lnd LIMIT 1").fetchone()[0]
        _insert_raw(db, landing_id=lnd_id)
        raw_id = db.execute("SELECT id FROM transactions_raw LIMIT 1").fetchone()[0]

        # Qualifying row: NULL booking_text, NULL merchant_normalized
        _insert_rfn(
            db,
            raw_id=raw_id,
            booking_text=None,
            merchant_normalized=None,
            transaction_type="INCOME",
            source_type="VISECA",
            reference="REF-FEE",
            amount=5.00,
        )
        # Non-qualifying row: already has booking_text
        _insert_rfn(
            db,
            raw_id=raw_id,
            booking_text="Existing text",
            merchant_normalized="SomeMerchant",
            transaction_type="INCOME",
            source_type="VISECA",
            reference="REF-OK",
            amount=5.00,
        )
        db.commit()

        transactions.fill_viseca_fee_text(
            db,
            booking_text="Viseca Annual Fee",
            merchant_normalized="Viseca",
            source_type="VISECA",
            transaction_type="INCOME",
            amount=5.00,
        )
        db.commit()

        updated = db.execute(
            "SELECT booking_text FROM transactions_rfn WHERE reference='REF-FEE'"
        ).fetchone()[0]
        untouched = db.execute(
            "SELECT booking_text FROM transactions_rfn WHERE reference='REF-OK'"
        ).fetchone()[0]

    assert updated == "Viseca Annual Fee"
    assert untouched == "Existing text"
