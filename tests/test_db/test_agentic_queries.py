"""Tests for named aiosql queries in agentic.sql (and agentic-related queries in transactions/groceries)."""

from __future__ import annotations

import sqlite3

from pathlib import Path

import pytest

from swiss_exp_tracker.db.sql import agentic
from swiss_exp_tracker.db.sql import groceries as gq
from swiss_exp_tracker.db.sql import transactions

# ---------------------------------------------------------------------------
# Fixture — standalone SQLite DB with all required tables
# ---------------------------------------------------------------------------


def _create_agentic_schema(db: sqlite3.Connection) -> None:
    """Create every table needed for agentic tests."""
    # transactions pipeline tables
    transactions.create_ingested_files_table(db)
    transactions.create_transactions_lnd_table(db)
    transactions.create_transactions_raw_table(db)
    transactions.create_transactions_rfn_table(db)
    transactions.create_api_usage_table(db)
    # agentic tables
    agentic.create_merchant_metadata_raw_table(db)
    agentic.create_merchant_metadata_rfn_table(db)
    agentic.create_transactions_use_table(db)
    # groceries table (for groceries enrichment tests)
    gq.create_groceries_lnd_table(db)
    gq.create_groceries_raw_table(db)
    gq.create_groceries_rfn_table(db)
    db.commit()


@pytest.fixture()
def tmp_agentic_db(tmp_path: Path) -> Path:
    """Return path to a fresh SQLite DB with full agentic schema."""
    db_path = tmp_path / "agentic_test.db"
    with sqlite3.connect(db_path) as db:
        _create_agentic_schema(db)
    return db_path


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _make_rfn_row(**overrides: object) -> dict[str, object]:
    """Return a valid transactions_rfn insert dict with sensible defaults."""
    base: dict[str, object] = {
        "raw_id": 1,
        "source_type": "ZKB_DEBIT",
        "date": "2026-01-15",
        "amount": 42.50,
        "transaction_type": "EXPENSE",
        "booking_text": "Test booking",
        "merchant_normalized": "test merchant",
        "is_person": 0,
        "currency": "CHF",
        "reference": "REF-001",
        "enrichment_status": "pending",
        "created_at": "2026-01-15T10:00:00",
        "balance_chf": None,
    }
    base.update(overrides)
    return base


def _make_use_row(**overrides: object) -> dict[str, object]:
    """Return a valid transactions_use insert dict with sensible defaults."""
    base: dict[str, object] = {
        "transaction_id": 1,
        "source_type": "ZKB_DEBIT",
        "date": "2026-01-15",
        "amount": 42.50,
        "transaction_type": "EXPENSE",
        "currency": "CHF",
        "reference": "REF-001",
        "merchant": "test merchant",
        "category_main": "Food",
        "category_second": "Restaurant",
        "city": "Zurich",
        "balance_chf": None,
    }
    base.update(overrides)
    return base


def _make_merchant_raw_row(**overrides: object) -> dict[str, object]:
    """Return a valid merchant_metadata_raw insert dict."""
    base: dict[str, object] = {
        "created_at": "2026-01-15T10:00:00",
        "reference_id": "REF-001",
        "matched_merchant": "Test Merchant",
        "cache_hit": 0,
        "similarity": 0.95,
        "search_tool": "web",
        "category_main": "Food",
        "category_second": "Restaurant",
        "city": "Zurich",
    }
    base.update(overrides)
    return base


def _make_merchant_rfn_row(**overrides: object) -> dict[str, object]:
    """Return a valid merchant_metadata_rfn insert dict."""
    base: dict[str, object] = {
        "created_at": "2026-01-15T10:00:00",
        "reference_id": "REF-001",
        "matched_merchant": "Test Merchant",
        "cache_hit": 0,
        "similarity": 0.95,
        "search_tool": "web",
        "category_main": "Food",
        "category_second": "Restaurant",
        "city": "Zurich",
    }
    base.update(overrides)
    return base


def _make_groceries_rfn_row(**overrides: object) -> dict[str, object]:
    """Return a valid groceries_rfn insert dict."""
    base: dict[str, object] = {
        "raw_id": None,
        "source_type": "COOP",
        "date": "2026-01-15",
        "time": "10:30:00",
        "location": "DailyStore Basel",
        "article": "Banana",
        "article_normalized": "banana",
        "unit": "kg",
        "quantity": 1.0,
        "price_chf": 0.99,
        "discount_chf": 0.0,
        "created_at": "2026-01-15T10:00:00",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Helper to insert a transactions_rfn row directly
# ---------------------------------------------------------------------------


def _insert_rfn(db: sqlite3.Connection, **overrides: object) -> int:
    """Insert one transactions_rfn row and return its id."""
    transactions.insert_transactions_rfn(db, **_make_rfn_row(**overrides))
    db.commit()
    return int(
        db.execute(
            "SELECT id FROM transactions_rfn ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
    )


def _insert_groceries_rfn(db: sqlite3.Connection, **overrides: object) -> int:
    """Insert one groceries_rfn row and return its id."""
    gq.insert_groceries_rfn(db, **_make_groceries_rfn_row(**overrides))
    db.commit()
    return int(
        db.execute("SELECT id FROM groceries_rfn ORDER BY id DESC LIMIT 1").fetchone()[
            0
        ]
    )


# ---------------------------------------------------------------------------
# get_pending_transactions
# ---------------------------------------------------------------------------


def test_get_pending_transactions_returns_only_pending(tmp_agentic_db: Path) -> None:
    """get_pending_transactions returns only rows with enrichment_status = 'pending'."""
    with sqlite3.connect(tmp_agentic_db) as db:
        pending_id = _insert_rfn(
            db, reference="REF-PENDING", enrichment_status="pending"
        )
        _insert_rfn(db, reference="REF-DONE", enrichment_status="enriched")

        rows = list(agentic.get_pending_transactions(db))

    assert len(rows) == 1
    # column order: id, date, merchant_normalized, booking_text, reference, amount, is_person
    assert rows[0][0] == pending_id
    assert rows[0][4] == "REF-PENDING"


# ---------------------------------------------------------------------------
# mark_transaction_enriched
# ---------------------------------------------------------------------------


def test_mark_transaction_enriched_updates_status(tmp_agentic_db: Path) -> None:
    """mark_transaction_enriched sets enrichment_status to 'enriched' for the given id."""
    with sqlite3.connect(tmp_agentic_db) as db:
        row_id = _insert_rfn(db, reference="REF-ENRICH", enrichment_status="pending")

        agentic.mark_transaction_enriched(db, refined_id=row_id)
        db.commit()

        status = db.execute(
            "SELECT enrichment_status FROM transactions_rfn WHERE id = ?", (row_id,)
        ).fetchone()[0]

    assert status == "enriched"


# ---------------------------------------------------------------------------
# insert_merchant_metadata_raw
# ---------------------------------------------------------------------------


def test_insert_and_fetch_merchant_metadata_raw(tmp_agentic_db: Path) -> None:
    """insert_merchant_metadata_raw persists one row; SELECT COUNT confirms it."""
    with sqlite3.connect(tmp_agentic_db) as db:
        agentic.insert_merchant_metadata_raw(db, **_make_merchant_raw_row())
        db.commit()

        count = db.execute("SELECT COUNT(*) FROM merchant_metadata_raw").fetchone()[0]

    assert count == 1


# ---------------------------------------------------------------------------
# get_latest_raw_merchant_rows (deduplication)
# ---------------------------------------------------------------------------


def test_get_latest_raw_merchant_rows_deduplication(tmp_agentic_db: Path) -> None:
    """Two rows for the same reference_id; only the one with the higher id is returned."""
    with sqlite3.connect(tmp_agentic_db) as db:
        agentic.insert_merchant_metadata_raw(
            db,
            **_make_merchant_raw_row(
                reference_id="REF-DUP",
                created_at="2026-01-10T08:00:00",
                matched_merchant="Old Merchant",
            ),
        )
        agentic.insert_merchant_metadata_raw(
            db,
            **_make_merchant_raw_row(
                reference_id="REF-DUP",
                created_at="2026-01-20T09:00:00",
                matched_merchant="New Merchant",
            ),
        )
        db.commit()

        rows = list(agentic.get_latest_raw_merchant_rows(db))

    assert len(rows) == 1
    # column order: id, created_at, reference_id, matched_merchant, ...
    assert rows[0][3] == "New Merchant"


# ---------------------------------------------------------------------------
# insert_merchant_metadata_rfn / update_merchant_metadata_rfn_categories
# ---------------------------------------------------------------------------


def test_insert_merchant_metadata_rfn_and_update_categories(
    tmp_agentic_db: Path,
) -> None:
    """insert_merchant_metadata_rfn stores the row; update_merchant_metadata_rfn_categories changes fields."""
    with sqlite3.connect(tmp_agentic_db) as db:
        agentic.insert_merchant_metadata_rfn(db, **_make_merchant_rfn_row())
        db.commit()

        rfn_id = db.execute("SELECT id FROM merchant_metadata_rfn LIMIT 1").fetchone()[
            0
        ]

        # Confirm the row was inserted
        count = db.execute("SELECT COUNT(*) FROM merchant_metadata_rfn").fetchone()[0]
        assert count == 1

        # Update categories
        agentic.update_merchant_metadata_rfn_categories(
            db,
            id=rfn_id,
            category_main="Transport",
            category_second="Public Transport",
            city="Basel",
        )
        db.commit()

        row = db.execute(
            "SELECT category_main, category_second, city FROM merchant_metadata_rfn WHERE id = ?",
            (rfn_id,),
        ).fetchone()

    assert row[0] == "Transport"
    assert row[1] == "Public Transport"
    assert row[2] == "Basel"


# ---------------------------------------------------------------------------
# get_enriched_transactions_not_in_use
# ---------------------------------------------------------------------------


def test_get_enriched_transactions_not_in_use_skips_existing(
    tmp_agentic_db: Path,
) -> None:
    """Only enriched rfn rows whose reference is not already in transactions_use are returned."""
    with sqlite3.connect(tmp_agentic_db) as db:
        # Two enriched rfn rows
        id_a = _insert_rfn(db, reference="REF-A", enrichment_status="enriched")
        id_b = _insert_rfn(db, reference="REF-B", enrichment_status="enriched")

        # rfn metadata for both
        agentic.insert_merchant_metadata_rfn(
            db, **_make_merchant_rfn_row(reference_id="REF-A")
        )
        agentic.insert_merchant_metadata_rfn(
            db, **_make_merchant_rfn_row(reference_id="REF-B")
        )
        db.commit()

        # REF-A is already in transactions_use
        agentic.insert_transactions_use(
            db,
            [_make_use_row(transaction_id=id_a, reference="REF-A")],
        )
        db.commit()

        rows = list(agentic.get_enriched_transactions_not_in_use(db))

    # Only REF-B should be returned
    assert len(rows) == 1
    # column order: transaction_id, source_type, date, amount, transaction_type,
    #               currency, reference, merchant, category_main, category_second, city, balance_chf
    assert rows[0][6] == "REF-B"
    _ = id_b  # referenced to confirm fixture setup


# ---------------------------------------------------------------------------
# insert_transactions_use (batch)
# ---------------------------------------------------------------------------


def test_insert_transactions_use_batch(tmp_agentic_db: Path) -> None:
    """Batch-inserting 2 transactions_use rows yields COUNT = 2."""
    with sqlite3.connect(tmp_agentic_db) as db:
        rows = [
            _make_use_row(transaction_id=1, reference="REF-USE-1"),
            _make_use_row(transaction_id=2, reference="REF-USE-2"),
        ]
        agentic.insert_transactions_use(db, rows)
        db.commit()

        count = db.execute("SELECT COUNT(*) FROM transactions_use").fetchone()[0]

    assert count == 2


# ---------------------------------------------------------------------------
# backfill_transactions_use_balance_chf
# ---------------------------------------------------------------------------


def test_backfill_transactions_use_balance_chf(tmp_agentic_db: Path) -> None:
    """backfill_transactions_use_balance_chf copies balance_chf from transactions_rfn by reference."""
    with sqlite3.connect(tmp_agentic_db) as db:
        # Insert rfn row with a known balance
        _insert_rfn(
            db,
            reference="REF-BAL",
            enrichment_status="enriched",
            balance_chf=100.0,
        )

        # Insert transactions_use row with balance_chf=None
        agentic.insert_transactions_use(
            db,
            [_make_use_row(reference="REF-BAL", balance_chf=None)],
        )
        db.commit()

        transactions.backfill_transactions_use_balance_chf(db)
        db.commit()

        balance = db.execute(
            "SELECT balance_chf FROM transactions_use WHERE reference = 'REF-BAL'"
        ).fetchone()[0]

    assert balance == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# get_pending_groceries
# ---------------------------------------------------------------------------


def test_get_pending_groceries_returns_only_pending(tmp_agentic_db: Path) -> None:
    """get_pending_groceries returns only groceries_rfn rows with status 'pending'."""
    with sqlite3.connect(tmp_agentic_db) as db:
        pending_id = _insert_groceries_rfn(db, article="Banana")
        enriched_id = _insert_groceries_rfn(db, article="Apple")
        gq.mark_grocery_enriched(db, rfn_id=enriched_id)
        db.commit()

        rows = list(gq.get_pending_groceries(db))

    assert len(rows) == 1
    # column order: id, article, article_normalized, location
    assert rows[0][0] == pending_id
    assert rows[0][1] == "Banana"


# ---------------------------------------------------------------------------
# mark_grocery_enriched
# ---------------------------------------------------------------------------


def test_mark_grocery_enriched_updates_status(tmp_agentic_db: Path) -> None:
    """mark_grocery_enriched sets enrichment_status = 'enriched' for the given rfn_id."""
    with sqlite3.connect(tmp_agentic_db) as db:
        rfn_id = _insert_groceries_rfn(db, article="Carrot")

        gq.mark_grocery_enriched(db, rfn_id=rfn_id)
        db.commit()

        status = db.execute(
            "SELECT enrichment_status FROM groceries_rfn WHERE id = ?", (rfn_id,)
        ).fetchone()[0]

    assert status == "enriched"


# ---------------------------------------------------------------------------
# check_merchant_metadata_raw_exists
# ---------------------------------------------------------------------------


def test_check_merchant_metadata_raw_exists_returns_scalar(
    tmp_agentic_db: Path,
) -> None:
    """check_merchant_metadata_raw_exists returns 1 (integer) after table creation."""
    with sqlite3.connect(tmp_agentic_db) as db:
        result = agentic.check_merchant_metadata_raw_exists(db)

    assert result == 1


# ---------------------------------------------------------------------------
# upsert_api_usage (agentic namespace)
# ---------------------------------------------------------------------------


def test_upsert_api_usage_inserts_and_updates(tmp_agentic_db: Path) -> None:
    """upsert_api_usage creates a row; second call updates the used count."""
    with sqlite3.connect(tmp_agentic_db) as db:
        agentic.upsert_api_usage(
            db,
            provider="openai",
            period="2026-01",
            used=3,
            credit_limit=100,
            updated_at="2026-01-01T10:00:00",
        )
        db.commit()

        agentic.upsert_api_usage(
            db,
            provider="openai",
            period="2026-01",
            used=17,
            credit_limit=100,
            updated_at="2026-01-01T11:00:00",
        )
        db.commit()

        result = agentic.get_api_usage(db, provider="openai", period="2026-01")

    assert result == 17
