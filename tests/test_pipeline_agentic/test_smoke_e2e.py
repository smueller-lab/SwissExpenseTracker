"""End-to-end smoke tests for the full agentic enrichment pipeline.

These tests exercise the complete flow:
    transactions_rfn (pending) →
    load_pending_transactions() →
    run_all_transactions() →
    [manual rfn-metadata migration step] →
    run_transactions_use() →
    transactions_use (populated)

Runner.run and MerchantStore are always mocked — no real API calls.
The real DB (database/transactions.db) is never touched.
"""

from __future__ import annotations

import sqlite3

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from agents.exceptions import OutputGuardrailTripwireTriggered

from swiss_exp_tracker.db.sql import agentic
from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_agentic.agents_.agent_summary import SearchToolResult
from swiss_exp_tracker.pipeline_agentic.agents_.agent_summary import WebSearchTool
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategoryMain
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategorySecond
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import MerchantMetaData
from swiss_exp_tracker.pipeline_agentic.pipeline import load_pending_transactions
from swiss_exp_tracker.pipeline_agentic.pipeline import run_all_transactions
from swiss_exp_tracker.pipeline_agentic.transactions_use import run_transactions_use

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
        "reference": "REF-E2E-001",
        "enrichment_status": "pending",
        "created_at": "2026-01-15T10:00:00",
        "balance_chf": None,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


class _MockRunResult:
    """Minimal stand-in for agents.RunResult."""

    def __init__(self, output: Any) -> None:
        self._output = output

    def final_output_as(self, cls: type) -> Any:
        """Return the stored output."""
        return self._output


def _make_mock_runner(*results_or_exceptions: Any) -> type:
    """Factory returning a fresh Runner mock class for each test."""
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
                raise AssertionError(f"Unexpected Runner.run call #{idx + 1}")
            result = results[idx]
            if isinstance(result, BaseException):
                raise result
            return _MockRunResult(result)

    _MockRunner.call_log = call_log  # type: ignore[attr-defined]
    return _MockRunner


class _MockStore:
    """MerchantStore stand-in that records saves and returns None from search."""

    def __init__(self) -> None:
        self.save_calls: list[Any] = []

    def search(self, merchant_raw: str) -> None:
        """Always return None (cache miss)."""
        return None

    def save(self, merchant_raw: str, summary: str, metadata: MerchantMetaData) -> None:
        """No-op; record for assertions."""
        self.save_calls.append((merchant_raw, summary, metadata))


def _insert_rfn_row(db_path: Path, **overrides: object) -> int:
    """Insert a transactions_rfn row (with FK bypass) and return its id."""
    with sqlite3.connect(db_path) as db:
        transactions.insert_transactions_rfn(db, **_make_rfn_row(**overrides))
        db.commit()
        row_id = db.execute(
            "SELECT id FROM transactions_rfn ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
    return int(row_id)


def _insert_rfn_metadata(
    db_path: Path,
    *,
    reference_id: str,
    category_main: str,
    category_second: str = "Supermarket",
    matched_merchant: str = "Freshmart",
    city: str = "Zurich",
) -> None:
    """Insert a merchant_metadata_rfn row directly (simulates the raw→rfn migration step)."""
    with sqlite3.connect(db_path) as db:
        agentic.insert_merchant_metadata_rfn(
            db,
            created_at="2026-01-15T10:00:00",
            reference_id=reference_id,
            matched_merchant=matched_merchant,
            cache_hit=0,
            similarity=None,
            search_tool="tavily",
            category_main=category_main,
            category_second=category_second,
            city=city,
        )
        db.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_smoke_e2e_pending_transaction_reaches_transactions_use(
    tmp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Full pipeline: pending rfn row → enriched → transactions_use (1 row)."""
    _insert_rfn_row(
        tmp_db,
        reference="REF-E2E-001",
        merchant_normalized="Freshmart",
        is_person=0,
        enrichment_status="pending",
    )

    summary_result = SearchToolResult(
        summary='"Freshmart" is a generic supermarket.', tool_used=WebSearchTool.TAVILY
    )
    metadata_result = MerchantMetaData(
        name="Freshmart",
        category_main=CategoryMain.GROCERIES,
        category_second=CategorySecond.GROCERIES_SUPERMARKET,
        city="Zurich",
    )
    mock_runner = _make_mock_runner(summary_result, metadata_result)

    mock_store = _MockStore()
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.MerchantStore",
        lambda: mock_store,
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.Runner",
        mock_runner,
    )

    # Step 1: load pending
    pending = load_pending_transactions()
    assert len(pending) == 1

    # Step 2: run enrichment
    await run_all_transactions(pending)

    # Verify raw metadata was written
    with sqlite3.connect(tmp_db) as db:
        raw_count = db.execute(
            "SELECT COUNT(*) FROM merchant_metadata_raw WHERE reference_id = 'REF-E2E-001'"
        ).fetchone()[0]
    assert raw_count == 1

    # Verify enrichment_status updated
    with sqlite3.connect(tmp_db) as db:
        status = db.execute(
            "SELECT enrichment_status FROM transactions_rfn WHERE reference = 'REF-E2E-001'"
        ).fetchone()[0]
    assert status == "enriched"

    # Step 3: simulate raw→rfn migration (normally done by clean_pipeline_output)
    _insert_rfn_metadata(
        tmp_db,
        reference_id="REF-E2E-001",
        category_main=CategoryMain.GROCERIES.value,
        matched_merchant="Freshmart",
    )

    # Step 4: run_transactions_use
    run_transactions_use()

    with sqlite3.connect(tmp_db) as db:
        use_count = db.execute("SELECT COUNT(*) FROM transactions_use").fetchone()[0]
        use_row = db.execute(
            "SELECT category_main, merchant FROM transactions_use WHERE reference = 'REF-E2E-001'"
        ).fetchone()
    assert use_count == 1
    assert use_row is not None
    assert use_row[0] == CategoryMain.GROCERIES.value
    assert use_row[1] == "Freshmart"


async def test_smoke_e2e_person_transaction_reaches_transactions_use(
    tmp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Person transaction is enriched as FRIEND and reaches transactions_use."""
    _insert_rfn_row(
        tmp_db,
        reference="REF-E2E-PERSON-001",
        merchant_normalized="John Doe",
        is_person=1,
        enrichment_status="pending",
    )

    runner_called = [False]

    class _NeverCalledRunner:
        @staticmethod
        async def run(agent: Any, input_data: str, max_turns: int = 10) -> None:
            runner_called[0] = True
            raise AssertionError("Runner.run should not be called for person")

    mock_store = _MockStore()
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.MerchantStore",
        lambda: mock_store,
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.Runner",
        _NeverCalledRunner,
    )

    pending = load_pending_transactions()
    assert len(pending) == 1

    await run_all_transactions(pending)

    assert not runner_called[0]

    # Verify person was tagged as FRIEND
    with sqlite3.connect(tmp_db) as db:
        row = db.execute(
            "SELECT category_main FROM merchant_metadata_raw WHERE reference_id = 'REF-E2E-PERSON-001'"
        ).fetchone()
    assert row is not None
    assert row[0] == CategoryMain.FRIEND.value

    # Simulate raw→rfn migration
    _insert_rfn_metadata(
        tmp_db,
        reference_id="REF-E2E-PERSON-001",
        category_main=CategoryMain.FRIEND.value,
        category_second=CategorySecond.FRIEND_SUPPORT_PAYMENT.value,
        matched_merchant="John Doe",
    )

    run_transactions_use()

    with sqlite3.connect(tmp_db) as db:
        use_row = db.execute(
            "SELECT category_main FROM transactions_use WHERE reference = 'REF-E2E-PERSON-001'"
        ).fetchone()
    assert use_row is not None
    assert use_row[0] == CategoryMain.FRIEND.value


async def test_smoke_e2e_failed_categorisation_enriched_with_null_category(
    tmp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """All metadata retries fail; transaction is enriched with NULL category; transactions_use stays empty."""
    _insert_rfn_row(
        tmp_db,
        reference="REF-E2E-NULL-001",
        merchant_normalized="SomeBrand",
        is_person=0,
        enrichment_status="pending",
    )

    summary_result = SearchToolResult(
        summary="SomeBrand summary.", tool_used=WebSearchTool.TAVILY
    )
    guardrail_exc = OutputGuardrailTripwireTriggered(MagicMock())

    mock_runner = _make_mock_runner(
        summary_result,
        guardrail_exc,
        guardrail_exc,
        guardrail_exc,
    )

    mock_store = _MockStore()
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.MerchantStore",
        lambda: mock_store,
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.Runner",
        mock_runner,
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.tqdm.write",
        lambda _: None,
    )

    pending = load_pending_transactions()
    await run_all_transactions(pending)

    # Enriched despite null category
    with sqlite3.connect(tmp_db) as db:
        status = db.execute(
            "SELECT enrichment_status FROM transactions_rfn WHERE reference = 'REF-E2E-NULL-001'"
        ).fetchone()[0]
    assert status == "enriched"

    with sqlite3.connect(tmp_db) as db:
        row = db.execute(
            "SELECT category_main FROM merchant_metadata_raw WHERE reference_id = 'REF-E2E-NULL-001'"
        ).fetchone()
    assert row is not None
    assert row[0] is None, "category_main must be NULL after all retries fail"

    # transactions_use stays empty (merchant_metadata_rfn is unpopulated for this reference)
    run_transactions_use()
    with sqlite3.connect(tmp_db) as db:
        use_count = db.execute("SELECT COUNT(*) FROM transactions_use").fetchone()[0]
    assert use_count == 0


async def test_smoke_e2e_all_providers_exhausted_stops_pipeline(
    tmp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AllProvidersExhaustedError is caught; pipeline stops; transaction stays pending."""
    _insert_rfn_row(
        tmp_db,
        reference="REF-E2E-EXHAUST-001",
        merchant_normalized="SomeShop",
        is_person=0,
        enrichment_status="pending",
    )

    no_websearch_result = SearchToolResult(
        summary="SEARCH_UNAVAILABLE: all providers exhausted.",
        tool_used=WebSearchTool.NO_WEBSEARCH,
    )
    mock_runner = _make_mock_runner(no_websearch_result)

    mock_store = _MockStore()
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.MerchantStore",
        lambda: mock_store,
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.merchant_manager.Runner",
        mock_runner,
    )
    monkeypatch.setattr(
        "swiss_exp_tracker.pipeline_agentic.pipeline.tqdm.write",
        lambda _: None,
    )

    pending = load_pending_transactions()
    assert len(pending) == 1

    # Should return without raising; AllProvidersExhaustedError is caught internally.
    await run_all_transactions(pending)

    # Transaction must remain pending — it was never successfully enriched.
    with sqlite3.connect(tmp_db) as db:
        status = db.execute(
            "SELECT enrichment_status FROM transactions_rfn WHERE reference = 'REF-E2E-EXHAUST-001'"
        ).fetchone()[0]
    assert (
        status == "pending"
    ), "enrichment_status should remain 'pending' when all providers are exhausted"
