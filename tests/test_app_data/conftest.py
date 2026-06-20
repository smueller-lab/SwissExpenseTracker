"""Shared fixtures for test_app_data."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def populated_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build a fixture DB, run the dashboard pipeline, and return the DB path.

    Module-scoped so each test module gets its own populated DB without touching
    the real transactions.db.
    """
    from swiss_exp_tracker.pipeline_dash.pipeline import run_dashboard_pipeline
    from tests.fixtures.db_builder import build_fixture_db
    from tests.fixtures.seed_data import make_seed_groceries
    from tests.fixtures.seed_data import make_seed_transactions

    tmp_path = tmp_path_factory.mktemp("app_data")
    dest = tmp_path / "transactions.db"
    build_fixture_db(dest, make_seed_transactions(), make_seed_groceries())
    run_dashboard_pipeline(db_path=dest)
    return dest
