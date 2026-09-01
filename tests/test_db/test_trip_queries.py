"""Tests for trip-related SQL queries: trips table, trip_transactions table, CRUD operations."""

from __future__ import annotations

import sqlite3

from pathlib import Path

import pandas as pd
import pytest

from swiss_exp_tracker.db.sql import transactions

# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _make_trip_row(**overrides: object) -> dict[str, object]:
    """Return a valid trips insert dict; each test states only what it varies."""
    base: dict[str, object] = {
        "name": "Ibiza Summer",
        "year": 2026,
        "created_at": "2026-01-01T00:00:00",
        "updated_at": "2026-01-01T00:00:00",
    }
    base.update(overrides)
    return base


def _make_tt_row(**overrides: object) -> dict[str, object]:
    """Return a valid trip_transactions batch-insert dict."""
    base: dict[str, object] = {
        "trip_id": 1,
        "transaction_id": 42,
        "assigned_at": "2026-01-01T00:00:00",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def trip_db(tmp_path: Path) -> Path:
    """Temp SQLite DB with trips and trip_transactions tables created."""
    db_path = tmp_path / "trip_test.db"
    with sqlite3.connect(db_path) as db:
        transactions.create_trips_table(db)
        transactions.create_trip_transactions_table(db)
        transactions.create_idx_trip_transactions_trip(db)
        db.commit()
    return db_path


# ---------------------------------------------------------------------------
# create_trips_table / create_trip_transactions_table — idempotency
# ---------------------------------------------------------------------------


def test_create_trips_table_idempotent(tmp_path: Path) -> None:
    """create_trips_table can be called twice without error (IF NOT EXISTS)."""
    db_path = tmp_path / "idempotent.db"
    with sqlite3.connect(db_path) as db:
        transactions.create_trips_table(db)
        transactions.create_trips_table(db)
        db.commit()
        rows = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='trips'"
        ).fetchall()
    assert len(rows) == 1


def test_create_trip_transactions_table_idempotent(tmp_path: Path) -> None:
    """create_trip_transactions_table can be called twice without error."""
    db_path = tmp_path / "idempotent2.db"
    with sqlite3.connect(db_path) as db:
        transactions.create_trips_table(db)
        transactions.create_trip_transactions_table(db)
        transactions.create_trip_transactions_table(db)
        db.commit()
        rows = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='trip_transactions'"
        ).fetchall()
    assert len(rows) == 1


def test_create_idx_trip_transactions_trip_idempotent(tmp_path: Path) -> None:
    """create_idx_trip_transactions_trip can be called twice without error."""
    db_path = tmp_path / "idempotent3.db"
    with sqlite3.connect(db_path) as db:
        transactions.create_trips_table(db)
        transactions.create_trip_transactions_table(db)
        transactions.create_idx_trip_transactions_trip(db)
        transactions.create_idx_trip_transactions_trip(db)
        db.commit()
        rows = db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_trip_transactions_trip'"
        ).fetchall()
    assert len(rows) == 1


# ---------------------------------------------------------------------------
# insert_trip! + get_trips — round-trip
# ---------------------------------------------------------------------------


def test_insert_trip_and_get_trips_roundtrip_name_and_year(trip_db: Path) -> None:
    """insert_trip! followed by get_trips returns the correct name and year."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row(name="Ibiza Summer", year=2026))
        db.commit()
        df = pd.read_sql(transactions.get_trips.sql, db)
    assert len(df) == 1
    assert df.iloc[0]["name"] == "Ibiza Summer"
    assert int(df.iloc[0]["year"]) == 2026


def test_insert_trip_preserves_created_at(trip_db: Path) -> None:
    """Inserted trip's created_at is stored exactly as supplied."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row(created_at="2025-06-15T10:00:00"))
        db.commit()
        df = pd.read_sql(transactions.get_trips.sql, db)
    assert df.iloc[0]["created_at"] == "2025-06-15T10:00:00"


def test_insert_two_trips_creates_distinct_rows(trip_db: Path) -> None:
    """Two trips with different names each create a separate row."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row(name="Trip A", year=2025))
        transactions.insert_trip(db, **_make_trip_row(name="Trip B", year=2026))
        db.commit()
        df = pd.read_sql(transactions.get_trips.sql, db)
    assert len(df) == 2
    assert set(df["name"]) == {"Trip A", "Trip B"}


def test_insert_trip_same_name_different_year_succeeds(trip_db: Path) -> None:
    """Two trips may share a name as long as their years differ (UNIQUE is (name, year))."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row(name="Madrid", year=2020))
        transactions.insert_trip(db, **_make_trip_row(name="Madrid", year=2026))
        db.commit()
        df = pd.read_sql(transactions.get_trips.sql, db)
    assert len(df) == 2
    assert set(df["year"]) == {2020, 2026}


def test_insert_trip_same_name_same_year_raises_integrity_error(trip_db: Path) -> None:
    """Two trips with the same name AND year violate the composite UNIQUE constraint."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row(name="Madrid", year=2020))
        db.commit()
        with pytest.raises(sqlite3.IntegrityError):
            transactions.insert_trip(db, **_make_trip_row(name="Madrid", year=2020))


# ---------------------------------------------------------------------------
# migrate_trips_unique_name_year — legacy name-only UNIQUE -> (name, year)
# ---------------------------------------------------------------------------


def test_migrate_trips_unique_name_year_rebuilds_legacy_table(tmp_path: Path) -> None:
    """A trips table with the old name-only UNIQUE is rebuilt to allow same-name/different-year."""
    from swiss_exp_tracker.pipeline_ingestion.db import migrate_trips_unique_name_year

    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as db:
        db.execute(
            """
            CREATE TABLE trips (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                year INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        transactions.insert_trip(db, **_make_trip_row(name="Madrid", year=2020))
        db.commit()

        migrate_trips_unique_name_year(db)
        db.commit()

        # Old row survived the rebuild.
        df = pd.read_sql(transactions.get_trips.sql, db)
        assert len(df) == 1
        assert df.iloc[0]["name"] == "Madrid"
        assert int(df.iloc[0]["year"]) == 2020

        # Same name, different year now succeeds.
        transactions.insert_trip(db, **_make_trip_row(name="Madrid", year=2026))
        db.commit()
        df = pd.read_sql(transactions.get_trips.sql, db)
        assert len(df) == 2

        # Same name, same year still rejected.
        with pytest.raises(sqlite3.IntegrityError):
            transactions.insert_trip(db, **_make_trip_row(name="Madrid", year=2026))


def test_migrate_trips_unique_name_year_noop_on_already_migrated_table(
    trip_db: Path,
) -> None:
    """Calling the migration twice on an already-migrated table is a no-op."""
    from swiss_exp_tracker.pipeline_ingestion.db import migrate_trips_unique_name_year

    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row(name="Madrid", year=2020))
        db.commit()

        migrate_trips_unique_name_year(db)
        migrate_trips_unique_name_year(db)
        db.commit()

        df = pd.read_sql(transactions.get_trips.sql, db)
    assert len(df) == 1


def test_migrate_trips_unique_name_year_noop_when_table_missing(tmp_path: Path) -> None:
    """Calling the migration before the trips table exists does not raise."""
    from swiss_exp_tracker.pipeline_ingestion.db import migrate_trips_unique_name_year

    db_path = tmp_path / "no_trips_yet.db"
    with sqlite3.connect(db_path) as db:
        migrate_trips_unique_name_year(db)
        db.commit()


def test_get_trips_empty_when_no_rows(trip_db: Path) -> None:
    """get_trips returns an empty DataFrame when no trips have been inserted."""
    with sqlite3.connect(trip_db) as db:
        df = pd.read_sql(transactions.get_trips.sql, db)
    assert df.empty


def test_get_trips_returns_expected_columns(trip_db: Path) -> None:
    """get_trips result has exactly the five expected columns."""
    with sqlite3.connect(trip_db) as db:
        df = pd.read_sql(transactions.get_trips.sql, db)
    assert set(df.columns) == {"id", "name", "year", "created_at", "updated_at"}


# ---------------------------------------------------------------------------
# update_trip_year! — changes only year, name untouched
# ---------------------------------------------------------------------------


def test_update_trip_year_changes_year_only(trip_db: Path) -> None:
    """update_trip_year! changes year and leaves name untouched."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row(name="Barcelona", year=2025))
        db.commit()
        row_id = db.execute("SELECT id FROM trips WHERE name='Barcelona'").fetchone()[0]
        transactions.update_trip_year(
            db, year=2027, updated_at="2027-01-01T00:00:00", id=row_id
        )
        db.commit()
        df = pd.read_sql(transactions.get_trips.sql, db)
    assert df.iloc[0]["name"] == "Barcelona"
    assert int(df.iloc[0]["year"]) == 2027


def test_update_trip_year_stores_updated_at(trip_db: Path) -> None:
    """update_trip_year! stores the supplied updated_at timestamp."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row())
        db.commit()
        row_id = db.execute("SELECT id FROM trips").fetchone()[0]
        transactions.update_trip_year(
            db, year=2028, updated_at="2028-03-10T12:00:00", id=row_id
        )
        db.commit()
        stored = db.execute(
            "SELECT updated_at FROM trips WHERE id=?", (row_id,)
        ).fetchone()[0]
    assert stored == "2028-03-10T12:00:00"


def test_update_trip_year_row_count_unchanged(trip_db: Path) -> None:
    """update_trip_year! does not add or remove rows."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row())
        db.commit()
        row_id = db.execute("SELECT id FROM trips").fetchone()[0]
        transactions.update_trip_year(
            db, year=2029, updated_at="2029-01-01T00:00:00", id=row_id
        )
        db.commit()
        count = db.execute("SELECT COUNT(*) FROM trips").fetchone()[0]
    assert count == 1


# ---------------------------------------------------------------------------
# rename_trip!
# ---------------------------------------------------------------------------


def test_rename_trip_changes_name_leaves_year(trip_db: Path) -> None:
    """rename_trip! changes name and leaves year untouched."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row(name="Old Name", year=2024))
        db.commit()
        row_id = db.execute("SELECT id FROM trips").fetchone()[0]
        transactions.rename_trip(
            db, name="New Name", updated_at="2024-06-01T00:00:00", id=row_id
        )
        db.commit()
        df = pd.read_sql(transactions.get_trips.sql, db)
    assert df.iloc[0]["name"] == "New Name"
    assert int(df.iloc[0]["year"]) == 2024


# ---------------------------------------------------------------------------
# assign_transactions_to_trip*! — upsert / move semantics
# ---------------------------------------------------------------------------


def test_assign_transactions_to_trip_inserts_row(trip_db: Path) -> None:
    """assign_transactions_to_trip*! inserts a row into trip_transactions."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row(name="Trip A", year=2025))
        db.commit()
        trip_id = db.execute("SELECT id FROM trips").fetchone()[0]
        transactions.assign_transactions_to_trip(
            db,
            [
                {
                    "trip_id": trip_id,
                    "transaction_id": 99,
                    "assigned_at": "2025-01-01T00:00:00",
                }
            ],
        )
        db.commit()
        count = db.execute("SELECT COUNT(*) FROM trip_transactions").fetchone()[0]
    assert count == 1


def test_assign_transactions_to_trip_upsert_moves_not_duplicates(trip_db: Path) -> None:
    """Re-assigning a transaction to a new trip moves it (ON CONFLICT update), not duplicates it."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row(name="Trip A", year=2025))
        transactions.insert_trip(db, **_make_trip_row(name="Trip B", year=2025))
        db.commit()
        trip_a_id = db.execute("SELECT id FROM trips WHERE name='Trip A'").fetchone()[0]
        trip_b_id = db.execute("SELECT id FROM trips WHERE name='Trip B'").fetchone()[0]

        # Assign tx 42 to Trip A
        transactions.assign_transactions_to_trip(
            db,
            [
                {
                    "trip_id": trip_a_id,
                    "transaction_id": 42,
                    "assigned_at": "2025-01-01T00:00:00",
                }
            ],
        )
        db.commit()

        # Move tx 42 to Trip B via upsert
        transactions.assign_transactions_to_trip(
            db,
            [
                {
                    "trip_id": trip_b_id,
                    "transaction_id": 42,
                    "assigned_at": "2025-01-02T00:00:00",
                }
            ],
        )
        db.commit()

        total_rows = db.execute(
            "SELECT COUNT(*) FROM trip_transactions WHERE transaction_id=42"
        ).fetchone()[0]
        stored_trip_id = db.execute(
            "SELECT trip_id FROM trip_transactions WHERE transaction_id=42"
        ).fetchone()[0]

    assert total_rows == 1
    assert stored_trip_id == trip_b_id


def test_assign_transactions_to_trip_batch_inserts_multiple(trip_db: Path) -> None:
    """Batch insert assigns multiple transactions at once."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row())
        db.commit()
        trip_id = db.execute("SELECT id FROM trips").fetchone()[0]
        transactions.assign_transactions_to_trip(
            db,
            [
                {
                    "trip_id": trip_id,
                    "transaction_id": 1,
                    "assigned_at": "2026-01-01T00:00:00",
                },
                {
                    "trip_id": trip_id,
                    "transaction_id": 2,
                    "assigned_at": "2026-01-01T00:00:00",
                },
                {
                    "trip_id": trip_id,
                    "transaction_id": 3,
                    "assigned_at": "2026-01-01T00:00:00",
                },
            ],
        )
        db.commit()
        count = db.execute("SELECT COUNT(*) FROM trip_transactions").fetchone()[0]
    assert count == 3


# ---------------------------------------------------------------------------
# delete_trip_transactions_by_trip! + delete_trip! — no orphaned rows
# ---------------------------------------------------------------------------


def test_delete_trip_transactions_by_trip_then_delete_trip_leaves_no_orphans(
    trip_db: Path,
) -> None:
    """Deleting junction rows then the trip leaves both tables empty with no orphans."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row())
        db.commit()
        trip_id = db.execute("SELECT id FROM trips").fetchone()[0]
        transactions.assign_transactions_to_trip(
            db,
            [
                {
                    "trip_id": trip_id,
                    "transaction_id": 10,
                    "assigned_at": "2026-01-01T00:00:00",
                },
                {
                    "trip_id": trip_id,
                    "transaction_id": 11,
                    "assigned_at": "2026-01-01T00:00:00",
                },
            ],
        )
        db.commit()
        transactions.delete_trip_transactions_by_trip(db, trip_id=trip_id)
        transactions.delete_trip(db, id=trip_id)
        db.commit()

        trips_count = db.execute("SELECT COUNT(*) FROM trips").fetchone()[0]
        tt_count = db.execute("SELECT COUNT(*) FROM trip_transactions").fetchone()[0]

    assert trips_count == 0
    assert tt_count == 0


def test_delete_trip_transactions_by_trip_leaves_other_trips_intact(
    trip_db: Path,
) -> None:
    """delete_trip_transactions_by_trip! only removes rows for the specified trip_id."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row(name="Trip A", year=2025))
        transactions.insert_trip(db, **_make_trip_row(name="Trip B", year=2025))
        db.commit()
        trip_a_id = db.execute("SELECT id FROM trips WHERE name='Trip A'").fetchone()[0]
        trip_b_id = db.execute("SELECT id FROM trips WHERE name='Trip B'").fetchone()[0]
        transactions.assign_transactions_to_trip(
            db,
            [
                {
                    "trip_id": trip_a_id,
                    "transaction_id": 1,
                    "assigned_at": "2025-01-01T00:00:00",
                }
            ],
        )
        transactions.assign_transactions_to_trip(
            db,
            [
                {
                    "trip_id": trip_b_id,
                    "transaction_id": 2,
                    "assigned_at": "2025-01-01T00:00:00",
                }
            ],
        )
        db.commit()

        transactions.delete_trip_transactions_by_trip(db, trip_id=trip_a_id)
        db.commit()

        remaining_count = db.execute(
            "SELECT COUNT(*) FROM trip_transactions"
        ).fetchone()[0]
        remaining_trip_id = db.execute(
            "SELECT trip_id FROM trip_transactions"
        ).fetchone()[0]

    assert remaining_count == 1
    assert remaining_trip_id == trip_b_id


# ---------------------------------------------------------------------------
# unassign_transaction_from_trip!
# ---------------------------------------------------------------------------


def test_unassign_transaction_from_trip_removes_row(trip_db: Path) -> None:
    """unassign_transaction_from_trip! removes the matching trip_transactions row."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row())
        db.commit()
        trip_id = db.execute("SELECT id FROM trips").fetchone()[0]
        transactions.assign_transactions_to_trip(
            db,
            [
                {
                    "trip_id": trip_id,
                    "transaction_id": 55,
                    "assigned_at": "2026-01-01T00:00:00",
                }
            ],
        )
        db.commit()
        transactions.unassign_transaction_from_trip(db, transaction_id=55)
        db.commit()
        count = db.execute(
            "SELECT COUNT(*) FROM trip_transactions WHERE transaction_id=55"
        ).fetchone()[0]
    assert count == 0


def test_unassign_transaction_from_trip_leaves_others_intact(trip_db: Path) -> None:
    """unassign_transaction_from_trip! does not affect other transaction assignments."""
    with sqlite3.connect(trip_db) as db:
        transactions.insert_trip(db, **_make_trip_row())
        db.commit()
        trip_id = db.execute("SELECT id FROM trips").fetchone()[0]
        transactions.assign_transactions_to_trip(
            db,
            [
                {
                    "trip_id": trip_id,
                    "transaction_id": 55,
                    "assigned_at": "2026-01-01T00:00:00",
                },
                {
                    "trip_id": trip_id,
                    "transaction_id": 56,
                    "assigned_at": "2026-01-01T00:00:00",
                },
            ],
        )
        db.commit()
        transactions.unassign_transaction_from_trip(db, transaction_id=55)
        db.commit()
        remaining = db.execute("SELECT COUNT(*) FROM trip_transactions").fetchone()[0]
    assert remaining == 1


# ---------------------------------------------------------------------------
# get_trip_transactions
# ---------------------------------------------------------------------------


def test_get_trip_transactions_returns_expected_columns(trip_db: Path) -> None:
    """get_trip_transactions result has exactly the four expected columns."""
    with sqlite3.connect(trip_db) as db:
        df = pd.read_sql(transactions.get_trip_transactions.sql, db)
    assert set(df.columns) == {"id", "trip_id", "transaction_id", "assigned_at"}


def test_get_trip_transactions_empty_initially(trip_db: Path) -> None:
    """get_trip_transactions returns an empty DataFrame when no assignments exist."""
    with sqlite3.connect(trip_db) as db:
        df = pd.read_sql(transactions.get_trip_transactions.sql, db)
    assert df.empty
