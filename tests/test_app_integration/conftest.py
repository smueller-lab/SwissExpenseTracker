from __future__ import annotations

import copy
import unittest.mock

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Column-schema constants — kept in sync with loader.py / groceries_detail.py
# ---------------------------------------------------------------------------

_GROCERY_ITEMS_COLS: list[str] = [
    "id",
    "rfn_id",
    "date",
    "time",
    "location",
    "article",
    "unit",
    "quantity",
    "price_chf",
    "discount_chf",
    "category_main",
    "category_detail",
]

_GROCERY_CAT_COLS: list[str] = [
    "category_main",
    "total_CHF",
    "totalPeriod_CHF",
    "pct",
    "Freq",
    "Period",
]

_GROCERY_HEALTH_COLS: list[str] = ["Period", "score"]

_GROCERY_TOP_ARTICLES_COLS: list[str] = [
    "article",
    "category_main",
    "count",
    "total_chf",
    "avg_chf",
]

# ---------------------------------------------------------------------------
# Session-scoped fixtures — created once per test session
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def real_data() -> Any:
    """DataLoader initialised from the real transactions DB (read-only)."""
    from swiss_exp_tracker.app.data.loader import DataLoader

    return DataLoader()


@pytest.fixture(scope="session")
def real_pos() -> Any:
    """PositionsLoader from the real positions DB; returns empty frame when DB is absent."""
    from swiss_exp_tracker.app.data.loader_positions import PositionsLoader

    return PositionsLoader()


# ---------------------------------------------------------------------------
# Function-scoped fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def real_app(real_data: Any, real_pos: Any) -> Any:
    """Configured Dash app wired to the real data loaders.

    Patches run_dashboard_pipeline before app.py is first imported so the
    module-level side effect (pipeline rebuild) is suppressed during tests.
    """
    with unittest.mock.patch(
        "swiss_exp_tracker.pipeline_dash.pipeline.run_dashboard_pipeline"
    ):
        from swiss_exp_tracker.app.app import create_app_factory

    return create_app_factory(real_data, real_pos)


@pytest.fixture
def empty_pos(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    """PositionsLoader backed by a non-existent DB — exercises the OperationalError guard.

    Asserts pdf.empty so callers can rely on the fixture being correctly set up.
    """
    monkeypatch.setattr(
        "swiss_exp_tracker.app.data.loader_positions.POSITIONS_DB_PATH",
        tmp_path / "missing.db",
    )
    from swiss_exp_tracker.app.data.loader_positions import PositionsLoader

    pos = PositionsLoader()
    assert pos.pdf.empty
    return pos


@pytest.fixture
def empty_cumulus_data(real_data: Any) -> Any:
    """Shallow-copy of the real DataLoader with grocery item-level data swapped out.

    Uses copy.copy so the session-scoped real_data object is never mutated.
    """
    data = copy.copy(real_data)

    # Replace item-level grocery data with structurally correct empty frames
    empty_items = pd.DataFrame(columns=_GROCERY_ITEMS_COLS)
    empty_items["date"] = pd.to_datetime(empty_items["date"])
    data.pdf_GroceryItems = empty_items
    data.pdf_GroceryCat = pd.DataFrame(columns=_GROCERY_CAT_COLS)
    data.pdf_GroceryHealth = pd.DataFrame(columns=_GROCERY_HEALTH_COLS)
    data.pdf_GroceryTopArticles = pd.DataFrame(columns=_GROCERY_TOP_ARTICLES_COLS)

    # Reset derived KPI scalars
    data.n_GroceryMonthlySpend = 0.0
    data.n_GroceryVisitsMonth = 0
    data.n_GroceryAvgPerVisit = 0.0
    data.s_GroceryCurrentMonth = ""
    data.n_HealthScore_latest = 50.0

    return data
