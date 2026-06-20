"""Unit tests for ApiUsageRow model, _make_api_usage_row factory, and load_*_usage loaders.

Covers:
- ApiUsageRow happy path and round-trip via model_dump / model_validate
- Factory override: only the overridden field changes
- Seed fixture → load functions return the seeded values
- Unseeded provider → load function returns 0
- Period isolation: a row seeded for a past period does not affect the current-period loader
"""

from __future__ import annotations

import datetime

from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_agentic.web_search import load_brave_usage
from swiss_exp_tracker.pipeline_agentic.web_search import load_exa_usage
from swiss_exp_tracker.pipeline_agentic.web_search import load_scrape_do_usage
from swiss_exp_tracker.pipeline_agentic.web_search import load_tavily_usage
from swiss_exp_tracker.pipeline_ingestion.db import get_connection
from tests.test_web_search.conftest import ApiUsageRow
from tests.test_web_search.conftest import _make_api_usage_row

# ── Scenario 1: ApiUsageRow happy path and round-trip ───────────────────────


def test_api_usage_row_happy_path_all_fields_populated() -> None:
    """model_validate builds a valid row with all 5 fields set."""
    row = _make_api_usage_row()

    assert row.provider == "tavily"
    assert row.period == datetime.date.today().strftime("%Y-%m")
    assert row.used == 0
    assert row.credit_limit == 1000
    assert row.updated_at != ""


def test_api_usage_row_roundtrip_model_dump_and_validate() -> None:
    """model_dump then model_validate produces an equal ApiUsageRow."""
    original = _make_api_usage_row()
    dumped = original.model_dump()
    restored = ApiUsageRow.model_validate(dumped)

    assert restored.provider == original.provider
    assert restored.period == original.period
    assert restored.used == original.used
    assert restored.credit_limit == original.credit_limit
    assert restored.updated_at == original.updated_at


# ── Scenario 2: Factory override ────────────────────────────────────────────


def test_api_usage_row_factory_override_used_field() -> None:
    """Overriding `used` changes only that field; all others keep their defaults."""
    row = _make_api_usage_row(used=42)

    assert row.used == 42
    # Remaining defaults are unchanged
    assert row.provider == "tavily"
    assert row.period == datetime.date.today().strftime("%Y-%m")
    assert row.credit_limit == 1000


def test_api_usage_row_factory_override_provider_field() -> None:
    """Overriding `provider` changes only that field."""
    row = _make_api_usage_row(provider="exa")

    assert row.provider == "exa"
    assert row.used == 0
    assert row.credit_limit == 1000


def test_api_usage_row_factory_override_multiple_fields() -> None:
    """Multiple overrides are all applied simultaneously."""
    row = _make_api_usage_row(provider="brave_search", used=75, credit_limit=500)

    assert row.provider == "brave_search"
    assert row.used == 75
    assert row.credit_limit == 500


# ── Scenario 3: Seed → load ──────────────────────────────────────────────────


def test_load_tavily_usage_returns_seeded_value(
    seed_api_usage: dict[str, ApiUsageRow],
) -> None:
    """load_tavily_usage returns the used count seeded for tavily."""
    seeded_used = seed_api_usage["tavily"].used
    assert load_tavily_usage() == seeded_used


def test_load_brave_usage_returns_seeded_value(
    seed_api_usage: dict[str, ApiUsageRow],
) -> None:
    """load_brave_usage returns the used count seeded for brave_search."""
    seeded_used = seed_api_usage["brave_search"].used
    assert load_brave_usage() == seeded_used


def test_load_exa_usage_returns_seeded_value(
    seed_api_usage: dict[str, ApiUsageRow],
) -> None:
    """load_exa_usage returns the used count seeded for exa."""
    seeded_used = seed_api_usage["exa"].used
    assert load_exa_usage() == seeded_used


def test_load_scrape_do_usage_returns_seeded_value(
    seed_api_usage: dict[str, ApiUsageRow],
) -> None:
    """load_scrape_do_usage returns the used count seeded for scrape_do."""
    seeded_used = seed_api_usage["scrape_do"].used
    assert load_scrape_do_usage() == seeded_used


# ── Scenario 3b: Unseeded state → returns 0 ─────────────────────────────────
# tmp_api_usage_db is autouse, so the DB exists but has no rows for any provider.


def test_load_tavily_usage_returns_zero_when_not_seeded() -> None:
    """load_tavily_usage returns 0 when no row exists for the current period."""
    # No seed_api_usage fixture used here — DB is empty
    assert load_tavily_usage() == 0


def test_load_exa_usage_returns_zero_when_not_seeded() -> None:
    """load_exa_usage returns 0 when no row exists for the current period."""
    assert load_exa_usage() == 0


def test_load_brave_usage_returns_zero_when_not_seeded() -> None:
    """load_brave_usage returns 0 when no row exists for the current period."""
    assert load_brave_usage() == 0


def test_load_scrape_do_usage_returns_zero_when_not_seeded() -> None:
    """load_scrape_do_usage returns 0 when no row exists for the current period."""
    assert load_scrape_do_usage() == 0


# ── Scenario 4: Period isolation ────────────────────────────────────────────


def test_load_exa_usage_ignores_past_period_row() -> None:
    """A row seeded for a past period does not affect the current-period loader."""
    past_row = _make_api_usage_row(provider="exa", period="2000-01", used=999)

    with get_connection() as db:
        data = past_row.model_dump()
        transactions.upsert_api_usage(db, **data)

    # Current period has no row → must return 0, not 999
    result = load_exa_usage()
    assert result != 999
    assert result == 0


def test_load_exa_usage_returns_current_period_not_past_when_both_exist() -> None:
    """When rows exist for both past and current periods, only the current period is returned."""
    current_period = datetime.date.today().strftime("%Y-%m")

    past_row = _make_api_usage_row(provider="exa", period="2000-01", used=999)
    current_row = _make_api_usage_row(provider="exa", period=current_period, used=7)

    with get_connection() as db:
        transactions.upsert_api_usage(db, **past_row.model_dump())
        transactions.upsert_api_usage(db, **current_row.model_dump())

    assert load_exa_usage() == 7


def test_load_tavily_usage_ignores_future_period_row() -> None:
    """A row seeded for a future period does not affect the current-period loader."""
    future_row = _make_api_usage_row(provider="tavily", period="2099-12", used=888)

    with get_connection() as db:
        transactions.upsert_api_usage(db, **future_row.model_dump())

    assert load_tavily_usage() == 0
