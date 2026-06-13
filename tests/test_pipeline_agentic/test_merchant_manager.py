"""Tests for MerchantManager.run() covering person shortcuts, vector-cache
hits/misses, agent retry fallbacks, and the AllProvidersExhaustedError halt.

Runner.run is always mocked — no real LLM or API calls are made.
The tmp_db fixture is used for DB assertions (merchant_metadata_raw,
enrichment_status on transactions_rfn).
"""

from __future__ import annotations

import sqlite3

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from agents.exceptions import MaxTurnsExceeded
from agents.exceptions import OutputGuardrailTripwireTriggered

from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_agentic.agents_.agent_summary import (
    AllProvidersExhaustedError,
)
from swiss_exp_tracker.pipeline_agentic.agents_.agent_summary import SearchToolResult
from swiss_exp_tracker.pipeline_agentic.agents_.agent_summary import WebSearchTool
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategoryMain
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategorySecond
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import MerchantMetaData
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import Transaction
from swiss_exp_tracker.pipeline_agentic.merchant_manager import MerchantManager

# ---------------------------------------------------------------------------
# Module-level factories
# ---------------------------------------------------------------------------


def _make_rfn_row(**overrides: object) -> dict[str, object]:
    """Return a valid transactions_rfn insert dict with sensible defaults."""
    base: dict[str, object] = {
        "raw_id": 1,
        "source_type": "ZKB_DEBIT",
        "date": "2026-01-15",
        "amount": 42.50,
        "transaction_type": "EXPENSE",
        "booking_text": "Test booking text",
        "merchant_normalized": "Freshmart",
        "is_person": 0,
        "currency": "CHF",
        "reference": "REF-001",
        "enrichment_status": "pending",
        "created_at": "2026-01-15T10:00:00",
        "balance_chf": None,
    }
    base.update(overrides)
    return base


def _make_transaction(**overrides: object) -> Transaction:
    """Return a valid Transaction model with sensible defaults."""
    base: dict[str, object] = {
        "refined_id": 1,
        "Date": None,
        "merchant": "Freshmart",
        "Booking text": "Freshmart_1",
        "ZKB reference": "REF-001",
        "Balance CHF": None,
        "amount_chf": -42.50,
        "is_person": False,
    }
    base.update(overrides)
    return Transaction.model_validate(base)


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


class _MockRunResult:
    """Minimal stand-in for agents.RunResult."""

    def __init__(self, output: Any) -> None:
        self._output = output

    def final_output_as(self, cls: type) -> Any:
        """Return the stored output regardless of cls."""
        return self._output


def _make_mock_runner(*results_or_exceptions: Any) -> type:
    """Return a class that mimics agents.Runner with pre-defined call results.

    Each positional argument is returned in order.  Pass an Exception subclass
    *instance* to make that call raise.
    """
    results = list(results_or_exceptions)
    call_log: list[tuple[Any, str]] = []

    class _MockRunner:
        @staticmethod
        async def run(
            agent: Any, input_data: str, max_turns: int = 10
        ) -> _MockRunResult:
            idx = len(call_log)
            call_log.append((agent, input_data))
            if idx >= len(results):
                raise AssertionError(
                    f"Unexpected Runner.run call #{idx + 1} "
                    f"(only {len(results)} result(s) configured)"
                )
            result = results[idx]
            if isinstance(result, BaseException):
                raise result
            return _MockRunResult(result)

    _MockRunner.call_log = call_log  # type: ignore[attr-defined]
    return _MockRunner


class _MockStore:
    """Minimal MerchantStore stand-in with configurable search() return value."""

    def __init__(self, search_return: Any = None) -> None:
        self._search_return = search_return
        self.save_calls: list[tuple[str, str, MerchantMetaData]] = []
        self.search_calls: list[str] = []

    def search(self, merchant_raw: str) -> Any:
        """Return the pre-configured search result."""
        self.search_calls.append(merchant_raw)
        return self._search_return

    def save(self, merchant_raw: str, summary: str, metadata: MerchantMetaData) -> None:
        """Record call for assertion; no real persistence."""
        self.save_calls.append((merchant_raw, summary, metadata))


def _insert_rfn(db_path: Path, **overrides: object) -> int:
    """Insert one transactions_rfn row and return its auto-assigned id."""
    with sqlite3.connect(db_path) as db:
        transactions.insert_transactions_rfn(db, **_make_rfn_row(**overrides))
        db.commit()
        row_id = db.execute(
            "SELECT id FROM transactions_rfn ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
    return int(row_id)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_run_person_transaction_sets_friend_category(
    tmp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Person transaction is categorised as FRIEND without calling Runner.run."""
    rfn_id = _insert_rfn(
        tmp_db,
        reference="REF-PERSON-001",
        is_person=1,
        merchant_normalized="John Smith",
        enrichment_status="pending",
    )

    tx = _make_transaction(
        refined_id=rfn_id,
        merchant="John Smith",
        is_person=True,
        **{"ZKB reference": "REF-PERSON-001"},
    )

    runner_called = [False]

    class _NeverCalledRunner:
        @staticmethod
        async def run(agent: Any, input_data: str, max_turns: int = 10) -> None:
            runner_called[0] = True
            raise AssertionError(
                "Runner.run must not be called for person transactions"
            )

    mock_store = _MockStore(search_return=None)
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.MerchantStore",
        lambda: mock_store,
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.Runner",
        _NeverCalledRunner,
    )

    manager = MerchantManager()

    async for _ in manager.run(tx):
        pass

    assert not runner_called[0], "Runner.run was unexpectedly called"

    with sqlite3.connect(tmp_db) as db:
        row = db.execute(
            "SELECT category_main FROM merchant_metadata_raw WHERE reference_id = ?",
            ("REF-PERSON-001",),
        ).fetchone()
    assert row is not None, "No merchant_metadata_raw row was inserted"
    assert row[0] == CategoryMain.FRIEND.value

    with sqlite3.connect(tmp_db) as db:
        status = db.execute(
            "SELECT enrichment_status FROM transactions_rfn WHERE id = ?",
            (rfn_id,),
        ).fetchone()[0]
    assert status == "enriched"


async def test_run_vector_cache_hit_skips_agent_calls(
    tmp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A vector-cache hit avoids Runner.run; saves with cache_hit = 1."""
    rfn_id = _insert_rfn(
        tmp_db,
        reference="REF-CACHE-001",
        enrichment_status="pending",
        merchant_normalized="Freshmart",
    )

    tx = _make_transaction(
        refined_id=rfn_id,
        merchant="Freshmart",
        is_person=False,
        **{"ZKB reference": "REF-CACHE-001"},
    )

    cached_metadata = MerchantMetaData(
        name="Freshmart",
        category_main=CategoryMain.GROCERIES,
        category_second=CategorySecond.GROCERIES_SUPERMARKET,
        city="Zurich",
    )

    runner_called = [False]

    class _NeverCalledRunner:
        @staticmethod
        async def run(agent: Any, input_data: str, max_turns: int = 10) -> None:
            runner_called[0] = True
            raise AssertionError("Runner.run must not be called on a cache hit")

    mock_store = _MockStore(search_return=(cached_metadata, pytest.approx(0.95)))
    # Monkey-patch similarity check: store.search returns a plain tuple, not approx
    mock_store._search_return = (cached_metadata, 0.95)

    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.MerchantStore",
        lambda: mock_store,
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.Runner",
        _NeverCalledRunner,
    )

    manager = MerchantManager()

    async for _ in manager.run(tx):
        pass

    assert not runner_called[0], "Runner.run was unexpectedly called on cache hit"

    with sqlite3.connect(tmp_db) as db:
        row = db.execute(
            "SELECT cache_hit, category_main FROM merchant_metadata_raw WHERE reference_id = ?",
            ("REF-CACHE-001",),
        ).fetchone()
    assert row is not None
    assert int(row[0]) == 1, "cache_hit should be 1"
    assert row[1] == CategoryMain.GROCERIES.value

    with sqlite3.connect(tmp_db) as db:
        status = db.execute(
            "SELECT enrichment_status FROM transactions_rfn WHERE id = ?",
            (rfn_id,),
        ).fetchone()[0]
    assert status == "enriched"


async def test_run_vector_cache_miss_calls_summary_and_metadata_agents(
    tmp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cache miss triggers both summary and metadata Runner.run calls."""
    rfn_id = _insert_rfn(
        tmp_db,
        reference="REF-MISS-001",
        enrichment_status="pending",
        merchant_normalized="DailyStore",
    )

    tx = _make_transaction(
        refined_id=rfn_id,
        merchant="DailyStore",
        is_person=False,
        **{"ZKB reference": "REF-MISS-001"},
    )

    summary_result = SearchToolResult(
        summary="Coop is a Swiss supermarket chain.",
        tool_used=WebSearchTool.TAVILY,
    )
    metadata_result = MerchantMetaData(
        name="DailyStore",
        category_main=CategoryMain.GROCERIES,
        category_second=CategorySecond.GROCERIES_SUPERMARKET,
        city="Basel",
    )

    mock_runner = _make_mock_runner(summary_result, metadata_result)

    mock_store = _MockStore(search_return=None)
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.MerchantStore",
        lambda: mock_store,
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.Runner",
        mock_runner,
    )

    manager = MerchantManager()

    async for _ in manager.run(tx):
        pass

    assert len(mock_runner.call_log) == 2, (  # type: ignore[attr-defined]
        f"Expected 2 Runner.run calls, got {len(mock_runner.call_log)}"  # type: ignore[attr-defined]
    )
    assert len(mock_store.save_calls) == 1, "store.save should be called once"
    assert mock_store.save_calls[0][0] == "DailyStore"

    with sqlite3.connect(tmp_db) as db:
        count = db.execute(
            "SELECT COUNT(*) FROM merchant_metadata_raw WHERE reference_id = ?",
            ("REF-MISS-001",),
        ).fetchone()[0]
    assert count == 1

    with sqlite3.connect(tmp_db) as db:
        status = db.execute(
            "SELECT enrichment_status FROM transactions_rfn WHERE id = ?",
            (rfn_id,),
        ).fetchone()[0]
    assert status == "enriched"


async def test_run_metadata_all_retries_fail_saves_with_null_category(
    tmp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """All 3 metadata retries fail; result is saved with category_main = NULL."""
    rfn_id = _insert_rfn(
        tmp_db,
        reference="REF-RETRY-001",
        enrichment_status="pending",
    )

    tx = _make_transaction(
        refined_id=rfn_id,
        merchant="UnknownBrand",
        **{"ZKB reference": "REF-RETRY-001"},
    )

    summary_result = SearchToolResult(
        summary="UnknownBrand summary.",
        tool_used=WebSearchTool.TAVILY,
    )
    guardrail_exc = OutputGuardrailTripwireTriggered(MagicMock())

    mock_runner = _make_mock_runner(
        summary_result,
        guardrail_exc,
        guardrail_exc,
        guardrail_exc,
    )

    warn_messages: list[str] = []
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.tqdm.write",
        warn_messages.append,
    )

    mock_store = _MockStore(search_return=None)
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.MerchantStore",
        lambda: mock_store,
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.Runner",
        mock_runner,
    )

    manager = MerchantManager()

    async for _ in manager.run(tx):
        pass

    with sqlite3.connect(tmp_db) as db:
        row = db.execute(
            "SELECT category_main FROM merchant_metadata_raw WHERE reference_id = ?",
            ("REF-RETRY-001",),
        ).fetchone()
    assert row is not None
    assert row[0] is None, "category_main must be NULL after all retries fail"

    with sqlite3.connect(tmp_db) as db:
        status = db.execute(
            "SELECT enrichment_status FROM transactions_rfn WHERE id = ?",
            (rfn_id,),
        ).fetchone()[0]
    assert status == "enriched"

    assert any(
        "[WARN]" in msg for msg in warn_messages
    ), f"Expected at least one [WARN] line; got: {warn_messages}"


async def test_run_all_providers_exhausted_raises(
    tmp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NO_WEBSEARCH summary result propagates as AllProvidersExhaustedError."""
    rfn_id = _insert_rfn(
        tmp_db,
        reference="REF-EXHAUST-001",
        enrichment_status="pending",
    )

    tx = _make_transaction(
        refined_id=rfn_id,
        merchant="ValueShop",
        **{"ZKB reference": "REF-EXHAUST-001"},
    )

    no_websearch_result = SearchToolResult(
        summary="SEARCH_UNAVAILABLE: all providers exhausted.",
        tool_used=WebSearchTool.NO_WEBSEARCH,
    )

    mock_runner = _make_mock_runner(no_websearch_result)

    mock_store = _MockStore(search_return=None)
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.MerchantStore",
        lambda: mock_store,
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.Runner",
        mock_runner,
    )

    manager = MerchantManager()

    with pytest.raises(AllProvidersExhaustedError):
        async for _ in manager.run(tx):
            pass


async def test_run_empty_merchant_name_returns_immediately(
    tmp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Transaction with empty merchant name produces no DB writes and no Runner calls."""
    rfn_id = _insert_rfn(
        tmp_db,
        reference="REF-EMPTY-001",
        merchant_normalized="",
        enrichment_status="pending",
    )

    tx = _make_transaction(
        refined_id=rfn_id,
        merchant="",
        **{"Booking text": "", "ZKB reference": "REF-EMPTY-001"},
    )

    runner_called = [False]

    class _NeverCalledRunner:
        @staticmethod
        async def run(agent: Any, input_data: str, max_turns: int = 10) -> None:
            runner_called[0] = True
            raise AssertionError("Runner.run must not be called for empty merchant")

    mock_store = _MockStore(search_return=None)
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.MerchantStore",
        lambda: mock_store,
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.Runner",
        _NeverCalledRunner,
    )

    manager = MerchantManager()

    async for _ in manager.run(tx):
        pass

    assert not runner_called[0]

    with sqlite3.connect(tmp_db) as db:
        count = db.execute("SELECT COUNT(*) FROM merchant_metadata_raw").fetchone()[0]
    assert count == 0, "No rows should be inserted for an empty merchant name"


async def test_run_max_turns_exceeded_for_summary_returns_fallback_and_continues(
    tmp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MaxTurnsExceeded for summary returns NO_WEBSEARCH fallback; pipeline continues with metadata."""
    rfn_id = _insert_rfn(
        tmp_db,
        reference="REF-MAXTURNS-001",
        enrichment_status="pending",
    )

    tx = _make_transaction(
        refined_id=rfn_id,
        merchant="BudgetMart",
        **{"ZKB reference": "REF-MAXTURNS-001"},
    )

    metadata_result = MerchantMetaData(
        name="BudgetMart",
        category_main=CategoryMain.GROCERIES,
        category_second=CategorySecond.GROCERIES_SUPERMARKET,
        city="Bern",
    )

    # First call (summary) raises MaxTurnsExceeded; second call (metadata) succeeds.
    mock_runner = _make_mock_runner(
        MaxTurnsExceeded("max turns exceeded in summary"),
        metadata_result,
    )

    mock_store = _MockStore(search_return=None)
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.MerchantStore",
        lambda: mock_store,
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.Runner",
        mock_runner,
    )

    warn_messages: list[str] = []
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.tqdm.write",
        warn_messages.append,
    )

    manager = MerchantManager()

    # Must NOT raise — MaxTurnsExceeded is caught internally.
    async for _ in manager.run(tx):
        pass

    # Summary call raised MaxTurnsExceeded (caught by get_merchant_summary).
    # Metadata call succeeded.
    assert len(mock_runner.call_log) == 2  # type: ignore[attr-defined]

    # Warn message should mention the max-turns fallback.
    assert any(
        "WARN" in msg or "MaxTurns" in msg for msg in warn_messages
    ), f"Expected a warning for MaxTurnsExceeded; got: {warn_messages}"

    with sqlite3.connect(tmp_db) as db:
        status = db.execute(
            "SELECT enrichment_status FROM transactions_rfn WHERE id = ?",
            (rfn_id,),
        ).fetchone()[0]
    assert status == "enriched"
