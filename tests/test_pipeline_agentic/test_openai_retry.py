"""Tests for the transient-OpenAI-error retry wrapper in merchant_manager.

Runner.run is always mocked — no real LLM or API calls are made.
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import httpx
import pytest

from agents import Runner
from openai import APITimeoutError

from swiss_exp_tracker.pipeline_agentic import merchant_manager as mm


def _timeout() -> APITimeoutError:
    """Build a real openai.APITimeoutError without hitting the network."""
    return APITimeoutError(
        request=httpx.Request("POST", "https://api.openai.com/v1/responses")
    )


async def test_retry_recovers_after_transient_timeouts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two timeouts then success: the wrapper returns the result on the third try."""
    monkeypatch.setattr(mm, "_OPENAI_RETRY_WAIT_S", 0.0)
    sentinel = object()
    run_mock = AsyncMock(side_effect=[_timeout(), _timeout(), sentinel])
    monkeypatch.setattr(Runner, "run", run_mock)

    result = await mm._run_agent_with_retry(MagicMock(), "{}", max_turns=10)

    assert result is sentinel
    assert run_mock.await_count == 3


async def test_retry_reraises_after_exhausting_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persistent timeouts: the wrapper retries up to the cap, then re-raises."""
    monkeypatch.setattr(mm, "_OPENAI_RETRY_WAIT_S", 0.0)
    run_mock = AsyncMock(side_effect=_timeout())
    monkeypatch.setattr(Runner, "run", run_mock)

    with pytest.raises(APITimeoutError):
        await mm._run_agent_with_retry(MagicMock(), "{}", max_turns=10)

    assert run_mock.await_count == mm._OPENAI_MAX_RETRIES


async def test_non_retryable_error_propagates_immediately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-OpenAI error is not retried — it surfaces on the first attempt."""
    monkeypatch.setattr(mm, "_OPENAI_RETRY_WAIT_S", 0.0)
    run_mock = AsyncMock(side_effect=ValueError("boom"))
    monkeypatch.setattr(Runner, "run", run_mock)

    with pytest.raises(ValueError, match="boom"):
        await mm._run_agent_with_retry(MagicMock(), "{}", max_turns=10)

    assert run_mock.await_count == 1
