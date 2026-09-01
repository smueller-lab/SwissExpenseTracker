"""Tests for DataLoader trip-related attributes and CRUD methods.

Uses a fresh fixture DB built from deterministic seed data for each test
(writable_loader) — never touches the real transactions.db.
"""

from __future__ import annotations

import sqlite3

from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def writable_loader(tmp_path: Path) -> object:
    """Fresh DB + DataLoader per test — safe for tests that call trip mutators."""
    from swiss_exp_tracker.app.data.loader import DataLoader
    from swiss_exp_tracker.pipeline_dash.pipeline import run_dashboard_pipeline
    from tests.fixtures.db_builder import build_fixture_db
    from tests.fixtures.seed_data import make_seed_groceries
    from tests.fixtures.seed_data import make_seed_transactions

    db_path = tmp_path / "transactions.db"
    build_fixture_db(db_path, make_seed_transactions(), make_seed_groceries())
    run_dashboard_pipeline(db_path=db_path)
    return DataLoader(db_path=db_path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_some_tx_ids(loader: object, n: int = 3) -> list[int]:
    """Return n transaction IDs from pdf_Master for use in trip assignments."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    return [int(x) for x in loader.pdf_Master["id"].head(n).tolist()]


def _sum_amounts(loader: object, tx_ids: list[int]) -> float:
    """Compute the sum of amount_CHF for the given transaction IDs in pdf_Master."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(loader, DataLoader)
    mask = loader.pdf_Master["id"].isin(tx_ids)
    return float(loader.pdf_Master.loc[mask, "amount_CHF"].sum())


# ---------------------------------------------------------------------------
# Table creation — defensive create inside DataLoader
# ---------------------------------------------------------------------------


def test_loader_creates_trip_tables_without_create_all_tables(
    writable_loader: object,
) -> None:
    """DataLoader initialises without error and exposes pdf_Trips even when
    create_all_tables() was never explicitly called (fixture uses build_fixture_db +
    run_dashboard_pipeline only).
    """
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    assert writable_loader.pdf_Trips is not None


def test_loader_pdf_trips_has_expected_columns(writable_loader: object) -> None:
    """pdf_Trips always has the seven expected columns after loader init."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    expected = {
        "id",
        "name",
        "year",
        "created_at",
        "updated_at",
        "total_chf",
        "n_transactions",
    }
    assert set(writable_loader.pdf_Trips.columns) == expected


def test_loader_pdf_trip_transactions_detail_has_expected_columns(
    writable_loader: object,
) -> None:
    """pdf_TripTransactionsDetail always has the twelve expected columns after loader init."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    expected = {
        "tt_id",
        "trip_id",
        "transaction_id",
        "assigned_at",
        "date",
        "Merchant",
        "amount_CHF",
        "category_main",
        "category_second",
        "transaction_type",
        "trip_name",
        "year",
    }
    assert set(writable_loader.pdf_TripTransactionsDetail.columns) == expected


def test_loader_pdf_trips_by_category_year_has_expected_columns(
    writable_loader: object,
) -> None:
    """pdf_TripsByCategoryYear always has the five expected columns after loader init."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    expected = {"year", "trip_id", "trip_name", "category_main", "total_chf"}
    assert set(writable_loader.pdf_TripsByCategoryYear.columns) == expected


# ---------------------------------------------------------------------------
# Empty-trips case does not raise
# ---------------------------------------------------------------------------


def test_loader_empty_trips_no_exception(writable_loader: object) -> None:
    """When no trips exist, all three trip DataFrames are empty and no exception is raised."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    assert writable_loader.pdf_Trips.empty
    assert writable_loader.pdf_TripTransactionsDetail.empty
    assert writable_loader.pdf_TripsByCategoryYear.empty


# ---------------------------------------------------------------------------
# create_trip
# ---------------------------------------------------------------------------


def test_create_trip_appears_in_pdf_trips(writable_loader: object) -> None:
    """After create_trip, the new trip appears in pdf_Trips with correct name and year."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    writable_loader.create_trip("Ibiza Summer", 2026)
    assert len(writable_loader.pdf_Trips) == 1
    row = writable_loader.pdf_Trips.iloc[0]
    assert row["name"] == "Ibiza Summer"
    assert int(row["year"]) == 2026


def test_create_trip_initial_total_chf_is_zero(writable_loader: object) -> None:
    """Newly created trip has total_chf=0 and n_transactions=0 before any assignment."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    writable_loader.create_trip("Empty Trip", 2025)
    row = writable_loader.pdf_Trips.iloc[0]
    assert row["total_chf"] == pytest.approx(0.0)
    assert int(row["n_transactions"]) == 0


def test_create_multiple_trips_all_appear(writable_loader: object) -> None:
    """Creating two trips results in exactly two rows in pdf_Trips."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    writable_loader.create_trip("Trip A", 2025)
    writable_loader.create_trip("Trip B", 2026)
    assert len(writable_loader.pdf_Trips) == 2
    assert set(writable_loader.pdf_Trips["name"]) == {"Trip A", "Trip B"}


def test_create_trip_duplicate_name_same_year_raises_integrity_error(
    writable_loader: object,
) -> None:
    """Creating two trips with the same name AND year raises sqlite3.IntegrityError."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    writable_loader.create_trip("Unique Name", 2025)
    with pytest.raises(sqlite3.IntegrityError):
        writable_loader.create_trip("Unique Name", 2025)


def test_create_trip_duplicate_name_different_year_succeeds(
    writable_loader: object,
) -> None:
    """Two trips may share a name as long as their years differ."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    writable_loader.create_trip("Madrid", 2020)
    writable_loader.create_trip("Madrid", 2026)
    assert len(writable_loader.pdf_Trips) == 2
    assert set(writable_loader.pdf_Trips["year"]) == {2020, 2026}


# ---------------------------------------------------------------------------
# rename_trip
# ---------------------------------------------------------------------------


def test_rename_trip_updates_pdf_trips(writable_loader: object) -> None:
    """After rename_trip, pdf_Trips reflects the new name."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    writable_loader.create_trip("Old Name", 2025)
    trip_id = int(writable_loader.pdf_Trips.iloc[0]["id"])
    writable_loader.rename_trip(trip_id, "New Name")
    assert writable_loader.pdf_Trips.iloc[0]["name"] == "New Name"


def test_rename_trip_preserves_year(writable_loader: object) -> None:
    """rename_trip changes only the name; year is untouched."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    writable_loader.create_trip("My Trip", 2024)
    trip_id = int(writable_loader.pdf_Trips.iloc[0]["id"])
    writable_loader.rename_trip(trip_id, "Renamed Trip")
    assert int(writable_loader.pdf_Trips.iloc[0]["year"]) == 2024


# ---------------------------------------------------------------------------
# update_trip_year
# ---------------------------------------------------------------------------


def test_update_trip_year_updates_pdf_trips(writable_loader: object) -> None:
    """After update_trip_year, pdf_Trips reflects the new year."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    writable_loader.create_trip("Annual Trip", 2025)
    trip_id = int(writable_loader.pdf_Trips.iloc[0]["id"])
    writable_loader.update_trip_year(trip_id, 2027)
    assert int(writable_loader.pdf_Trips.iloc[0]["year"]) == 2027


def test_update_trip_year_preserves_name(writable_loader: object) -> None:
    """update_trip_year changes only the year; name is untouched."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    writable_loader.create_trip("Named Trip", 2025)
    trip_id = int(writable_loader.pdf_Trips.iloc[0]["id"])
    writable_loader.update_trip_year(trip_id, 2028)
    assert writable_loader.pdf_Trips.iloc[0]["name"] == "Named Trip"


# ---------------------------------------------------------------------------
# delete_trip
# ---------------------------------------------------------------------------


def test_delete_trip_removes_from_pdf_trips(writable_loader: object) -> None:
    """After delete_trip, pdf_Trips no longer contains the deleted trip."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    writable_loader.create_trip("To Delete", 2025)
    trip_id = int(writable_loader.pdf_Trips.iloc[0]["id"])
    writable_loader.delete_trip(trip_id)
    assert writable_loader.pdf_Trips.empty


def test_delete_trip_removes_assigned_transactions(writable_loader: object) -> None:
    """delete_trip also cleans up trip_transactions so pdf_TripTransactionsDetail is empty."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    tx_ids = _get_some_tx_ids(writable_loader, 2)
    writable_loader.create_trip("Will Be Deleted", 2025)
    trip_id = int(writable_loader.pdf_Trips.iloc[0]["id"])
    writable_loader.assign_transactions_to_trip(trip_id, tx_ids)
    writable_loader.delete_trip(trip_id)
    assert writable_loader.pdf_TripTransactionsDetail.empty


# ---------------------------------------------------------------------------
# assign_transactions_to_trip — move-not-duplicate semantics
# ---------------------------------------------------------------------------


def test_assign_transactions_updates_pdf_trips_n_transactions(
    writable_loader: object,
) -> None:
    """After assigning N transactions, pdf_Trips shows n_transactions == N."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    n = 3
    tx_ids = _get_some_tx_ids(writable_loader, n)
    writable_loader.create_trip("Summer Trip", 2026)
    trip_id = int(writable_loader.pdf_Trips.iloc[0]["id"])
    writable_loader.assign_transactions_to_trip(trip_id, tx_ids)
    row = writable_loader.pdf_Trips.iloc[0]
    assert int(row["n_transactions"]) == n


def test_assign_transactions_updates_pdf_trips_total_chf(
    writable_loader: object,
) -> None:
    """After assigning transactions, pdf_Trips shows total_chf matching the sum from pdf_Master."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    tx_ids = _get_some_tx_ids(writable_loader, 3)
    expected_total = _sum_amounts(writable_loader, tx_ids)
    writable_loader.create_trip("Budget Trip", 2025)
    trip_id = int(writable_loader.pdf_Trips.iloc[0]["id"])
    writable_loader.assign_transactions_to_trip(trip_id, tx_ids)
    actual_total = float(writable_loader.pdf_Trips.iloc[0]["total_chf"])
    assert actual_total == pytest.approx(expected_total)


def test_assign_transactions_move_not_duplicate(writable_loader: object) -> None:
    """Re-assigning a transaction to a different trip moves it, not duplicates it;
    pdf_TripTransactionsDetail has exactly one row for that transaction_id.
    """
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    tx_id = int(_get_some_tx_ids(writable_loader, 1)[0])
    writable_loader.create_trip("Trip A", 2025)
    writable_loader.create_trip("Trip B", 2025)
    trips = writable_loader.pdf_Trips
    trip_a_id = int(trips.loc[trips["name"] == "Trip A", "id"].iloc[0])
    trip_b_id = int(trips.loc[trips["name"] == "Trip B", "id"].iloc[0])

    writable_loader.assign_transactions_to_trip(trip_a_id, [tx_id])
    writable_loader.assign_transactions_to_trip(trip_b_id, [tx_id])

    detail = writable_loader.pdf_TripTransactionsDetail
    rows_for_tx = detail[detail["transaction_id"] == tx_id]
    assert len(rows_for_tx) == 1
    assert int(rows_for_tx.iloc[0]["trip_id"]) == trip_b_id


def test_assign_transactions_detail_contains_expected_columns(
    writable_loader: object,
) -> None:
    """pdf_TripTransactionsDetail contains Merchant and amount_CHF after assignment."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    tx_ids = _get_some_tx_ids(writable_loader, 1)
    writable_loader.create_trip("Details Trip", 2025)
    trip_id = int(writable_loader.pdf_Trips.iloc[0]["id"])
    writable_loader.assign_transactions_to_trip(trip_id, tx_ids)
    detail = writable_loader.pdf_TripTransactionsDetail
    assert "Merchant" in detail.columns
    assert "amount_CHF" in detail.columns
    assert "trip_name" in detail.columns


# ---------------------------------------------------------------------------
# unassign_transactions
# ---------------------------------------------------------------------------


def test_unassign_transactions_removes_from_detail(writable_loader: object) -> None:
    """After unassign_transactions, the transaction no longer appears in pdf_TripTransactionsDetail."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    tx_ids = _get_some_tx_ids(writable_loader, 2)
    writable_loader.create_trip("Unassign Test", 2025)
    trip_id = int(writable_loader.pdf_Trips.iloc[0]["id"])
    writable_loader.assign_transactions_to_trip(trip_id, tx_ids)
    writable_loader.unassign_transactions([tx_ids[0]])
    detail = writable_loader.pdf_TripTransactionsDetail
    assert tx_ids[0] not in detail["transaction_id"].tolist()


def test_unassign_transactions_reappears_in_get_unassigned(
    writable_loader: object,
) -> None:
    """A transaction that is unassigned reappears in get_unassigned_transactions()."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    tx_id = int(_get_some_tx_ids(writable_loader, 1)[0])
    writable_loader.create_trip("Temp Trip", 2025)
    trip_id = int(writable_loader.pdf_Trips.iloc[0]["id"])
    writable_loader.assign_transactions_to_trip(trip_id, [tx_id])

    # Confirm it's now absent from unassigned
    unassigned_before = writable_loader.get_unassigned_transactions()
    assert tx_id not in unassigned_before["id"].tolist()

    # Unassign and confirm it reappears
    writable_loader.unassign_transactions([tx_id])
    unassigned_after = writable_loader.get_unassigned_transactions()
    assert tx_id in unassigned_after["id"].tolist()


# ---------------------------------------------------------------------------
# get_unassigned_transactions
# ---------------------------------------------------------------------------


def test_get_unassigned_transactions_all_when_no_trips(writable_loader: object) -> None:
    """With no trips, get_unassigned_transactions returns all pdf_Master rows."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    unassigned = writable_loader.get_unassigned_transactions()
    assert len(unassigned) == len(writable_loader.pdf_Master)


def test_get_unassigned_transactions_sorted_by_date_descending(
    writable_loader: object,
) -> None:
    """get_unassigned_transactions returns rows sorted by date descending."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    unassigned = writable_loader.get_unassigned_transactions()
    dates = unassigned["date"].reset_index(drop=True)
    assert (dates == dates.sort_values(ascending=False).reset_index(drop=True)).all()


def test_get_unassigned_transactions_excludes_assigned(writable_loader: object) -> None:
    """Assigned transaction IDs do not appear in get_unassigned_transactions()."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)
    tx_ids = _get_some_tx_ids(writable_loader, 2)
    writable_loader.create_trip("Assign Some", 2025)
    trip_id = int(writable_loader.pdf_Trips.iloc[0]["id"])
    writable_loader.assign_transactions_to_trip(trip_id, tx_ids)
    unassigned = writable_loader.get_unassigned_transactions()
    for tx_id in tx_ids:
        assert tx_id not in unassigned["id"].tolist()


# ---------------------------------------------------------------------------
# pdf_TripsByCategoryYear — grouping / multi-trip / multi-year
# ---------------------------------------------------------------------------


def test_pdf_trips_by_category_year_grouping(writable_loader: object) -> None:
    """pdf_TripsByCategoryYear has one row per trip-per-category combination."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)

    # Pick 3 expense transactions that may cover different categories
    expense_mask = writable_loader.pdf_Master["transaction_type"] == "EXPENSE"
    expense_ids = writable_loader.pdf_Master.loc[expense_mask, "id"].head(3).tolist()

    writable_loader.create_trip("Cat Trip", 2025)
    trip_id = int(writable_loader.pdf_Trips.iloc[0]["id"])
    writable_loader.assign_transactions_to_trip(trip_id, expense_ids)

    bycat = writable_loader.pdf_TripsByCategoryYear
    assert not bycat.empty
    # Each (trip_id, category_main) pair should be unique
    deduped = bycat.drop_duplicates(subset=["trip_id", "category_main"])
    assert len(bycat) == len(deduped)


def test_pdf_trips_by_category_year_two_trips_same_year_one_different(
    writable_loader: object,
) -> None:
    """Two trips in year 2025 and one in 2026 appear correctly grouped in pdf_TripsByCategoryYear."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    assert isinstance(writable_loader, DataLoader)

    # Get expense transactions with known categories for hand-computation
    expense_mask = writable_loader.pdf_Master["transaction_type"] == "EXPENSE"
    all_expense = writable_loader.pdf_Master[expense_mask].reset_index(drop=True)
    # Take first 6 expense rows: 2 for each trip
    ids_a = all_expense["id"].iloc[0:2].tolist()
    ids_b = all_expense["id"].iloc[2:4].tolist()
    ids_c = all_expense["id"].iloc[4:6].tolist()

    writable_loader.create_trip("Alpha", 2025)
    writable_loader.create_trip("Beta", 2025)
    writable_loader.create_trip("Gamma", 2026)

    trips = writable_loader.pdf_Trips
    id_alpha = int(trips.loc[trips["name"] == "Alpha", "id"].iloc[0])
    id_beta = int(trips.loc[trips["name"] == "Beta", "id"].iloc[0])
    id_gamma = int(trips.loc[trips["name"] == "Gamma", "id"].iloc[0])

    writable_loader.assign_transactions_to_trip(id_alpha, ids_a)
    writable_loader.assign_transactions_to_trip(id_beta, ids_b)
    writable_loader.assign_transactions_to_trip(id_gamma, ids_c)

    bycat = writable_loader.pdf_TripsByCategoryYear

    # Both 2025 trips appear in the bycat frame
    assert set(bycat.loc[bycat["year"] == 2025, "trip_name"]).issuperset(
        {"Alpha", "Beta"}
    )
    # 2026 trip appears in the bycat frame
    assert "Gamma" in bycat.loc[bycat["year"] == 2026, "trip_name"].tolist()

    # Hand-compute totals for trip Alpha and verify
    master_sub = writable_loader.pdf_Master
    alpha_expected = (
        master_sub[master_sub["id"].isin(ids_a)]
        .groupby("category_main")["amount_CHF"]
        .sum()
    )
    for cat, expected_total in alpha_expected.items():
        actual_rows = bycat[
            (bycat["trip_id"] == id_alpha) & (bycat["category_main"] == cat)
        ]
        assert len(actual_rows) == 1
        assert float(actual_rows.iloc[0]["total_chf"]) == pytest.approx(
            float(expected_total)
        )
