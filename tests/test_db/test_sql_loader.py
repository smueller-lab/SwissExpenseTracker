"""Tests that verify the aiosql loader and query namespaces load correctly."""

from __future__ import annotations

from pathlib import Path


def test_sql_loader_imports_without_error() -> None:
    """All four aiosql namespaces can be imported with no exception."""
    from swiss_exp_tracker.db.sql import agentic  # noqa: F401
    from swiss_exp_tracker.db.sql import groceries  # noqa: F401
    from swiss_exp_tracker.db.sql import positions  # noqa: F401
    from swiss_exp_tracker.db.sql import transactions  # noqa: F401


def test_all_query_namespaces_have_expected_functions() -> None:
    """Each namespace exposes at least one key query function."""
    from swiss_exp_tracker.db.sql import agentic
    from swiss_exp_tracker.db.sql import groceries
    from swiss_exp_tracker.db.sql import positions
    from swiss_exp_tracker.db.sql import transactions

    assert hasattr(transactions, "get_known_files_for_source")
    assert hasattr(groceries, "get_pending_groceries")
    assert hasattr(positions, "get_positions_use")
    assert hasattr(agentic, "get_pending_transactions")


def test_sql_files_exist_on_disk() -> None:
    """All four .sql query files are present under src/.../db/queries/."""
    queries_dir = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "swiss_exp_tracker"
        / "db"
        / "queries"
    )

    for name in ("transactions.sql", "groceries.sql", "positions.sql", "agentic.sql"):
        assert (queries_dir / name).exists(), f"Missing SQL file: {name}"
