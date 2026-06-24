"""Pure unit tests for the search_web fallback logic in agent_summary.

No DB, no network calls, no cassettes.  All provider functions and credit
counters are monkeypatched so the fallback order can be exercised in isolation.
"""

from __future__ import annotations

import json

from typing import Any
from typing import cast

import pytest

from agents.tool_context import ToolContext

from swiss_exp_tracker.pipeline_agentic.agents_.agent_summary import SearchToolResult
from swiss_exp_tracker.pipeline_agentic.agents_.agent_summary import WebSearchTool
from swiss_exp_tracker.pipeline_agentic.agents_.agent_summary import search_web
from swiss_exp_tracker.pipeline_agentic.web_search import BRAVE_SEARCH_FREE_CREDIT_LIMIT
from swiss_exp_tracker.pipeline_agentic.web_search import EXA_FREE_CREDIT_LIMIT
from swiss_exp_tracker.pipeline_agentic.web_search import SCRAPE_DO_FREE_CREDIT_LIMIT
from swiss_exp_tracker.pipeline_agentic.web_search import TAVILY_FREE_CREDIT_LIMIT

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MODULE = "swiss_exp_tracker.pipeline_agentic.agents_.agent_summary"


class _MockToolCtx:
    """Minimal context object accepted by FunctionTool.on_invoke_tool."""

    tool_name: str = "search_web"


async def _call_search_web(query: str) -> SearchToolResult:
    """Invoke search_web via its FunctionTool.on_invoke_tool interface."""
    result = await search_web.on_invoke_tool(
        cast("ToolContext[Any]", _MockToolCtx()), json.dumps({"query": query})
    )
    return cast("SearchToolResult", result)


def _patch_noop_persistence(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable all load/save DB calls so tests have no DB dependency."""
    monkeypatch.setattr(f"{_MODULE}.save_tavily_usage", lambda _: None)
    monkeypatch.setattr(f"{_MODULE}.save_brave_usage", lambda _: None)
    monkeypatch.setattr(f"{_MODULE}.save_scrape_do_usage", lambda _: None)
    monkeypatch.setattr(f"{_MODULE}.save_exa_usage", lambda _: None)
    monkeypatch.setattr(f"{_MODULE}.load_tavily_usage", lambda: 1)
    monkeypatch.setattr(f"{_MODULE}.load_brave_usage", lambda: 1)
    monkeypatch.setattr(f"{_MODULE}.load_scrape_do_usage", lambda: 1)
    monkeypatch.setattr(f"{_MODULE}.load_exa_usage", lambda: 1)


def _set_credits(
    monkeypatch: pytest.MonkeyPatch,
    *,
    tavily: int = 1,
    brave: int = 1,
    scrape_do: int = 1,
    exa: int = 1,
) -> None:
    """Set module-level credit counters to explicit values (non-zero skips lazy-load)."""
    monkeypatch.setattr(f"{_MODULE}._tavily_free_credits_used", tavily)
    monkeypatch.setattr(f"{_MODULE}._brave_free_credits_used", brave)
    monkeypatch.setattr(f"{_MODULE}._scrape_do_free_credits_used", scrape_do)
    monkeypatch.setattr(f"{_MODULE}._exa_free_credits_used", exa)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_search_web_uses_tavily_when_credits_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """search_web returns TAVILY when Tavily is available and credits remain."""
    _patch_noop_persistence(monkeypatch)
    _set_credits(monkeypatch, tavily=1, brave=1, scrape_do=1, exa=1)

    monkeypatch.setattr(
        f"{_MODULE}.tavily_web_search",
        lambda q: "TAVILY_RESULTS\nsome text about Migros",
    )

    result = await _call_search_web("Freshmart")

    assert result.tool_used == WebSearchTool.TAVILY
    assert "Freshmart" in result.summary or "some text" in result.summary


async def test_search_web_falls_through_when_tavily_missing_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When Tavily returns TAVILY_UNAVAILABLE (missing key), brave is tried next."""
    _patch_noop_persistence(monkeypatch)
    _set_credits(monkeypatch, tavily=1, brave=1, scrape_do=1, exa=1)

    monkeypatch.setattr(
        f"{_MODULE}.tavily_web_search",
        lambda q: "TAVILY_UNAVAILABLE: TAVILY_API_KEY is missing.",
    )
    monkeypatch.setattr(
        f"{_MODULE}.brave_web_search",
        lambda q: "BRAVE_RESULTS\nbrave result text",
    )

    result = await _call_search_web("Freshmart")

    assert result.tool_used == WebSearchTool.BRAVE


async def test_search_web_falls_through_on_tavily_credits_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When tavily credits are at the limit, brave free tier is tried next."""
    _patch_noop_persistence(monkeypatch)
    _set_credits(
        monkeypatch,
        tavily=TAVILY_FREE_CREDIT_LIMIT,
        brave=1,
        scrape_do=1,
        exa=1,
    )

    monkeypatch.setattr(
        f"{_MODULE}.brave_web_search",
        lambda q: "BRAVE_RESULTS\nbrave result text",
    )

    result = await _call_search_web("Freshmart")

    assert result.tool_used == WebSearchTool.BRAVE


async def test_search_web_falls_through_on_brave_missing_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When brave returns BRAVE_UNAVAILABLE (missing key), scrape_do is tried."""
    _patch_noop_persistence(monkeypatch)
    _set_credits(
        monkeypatch,
        tavily=TAVILY_FREE_CREDIT_LIMIT,
        brave=1,
        scrape_do=1,
        exa=1,
    )

    monkeypatch.setattr(
        f"{_MODULE}.brave_web_search",
        lambda q: "BRAVE_UNAVAILABLE: BRAVE_SEARCH_API_KEY is missing.",
    )
    monkeypatch.setattr(
        f"{_MODULE}.scrape_do_web_search",
        lambda q: "SCRAPEDO_RESULTS\nscrapedo result text",
    )

    result = await _call_search_web("Freshmart")

    assert result.tool_used == WebSearchTool.SCRAPE_DO


async def test_search_web_falls_through_on_scrape_do_missing_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When scrape_do is unavailable (missing key), exa free tier is tried."""
    _patch_noop_persistence(monkeypatch)
    _set_credits(
        monkeypatch,
        tavily=TAVILY_FREE_CREDIT_LIMIT,
        brave=BRAVE_SEARCH_FREE_CREDIT_LIMIT,
        scrape_do=1,
        exa=1,
    )

    monkeypatch.setattr(
        f"{_MODULE}.scrape_do_web_search",
        lambda q: "SCRAPEDO_UNAVAILABLE: SCRAPE_DO_API_KEY is missing.",
    )
    monkeypatch.setattr(
        f"{_MODULE}.exa_web_search",
        lambda q: "EXA_RESULTS:1\nsome exa text",
    )

    result = await _call_search_web("Freshmart")

    assert result.tool_used == WebSearchTool.EXA


async def test_search_web_returns_no_websearch_when_all_free_exhausted_and_no_payperus_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When all free tiers are exhausted and no pay-per-use keys exist, NO_WEBSEARCH is returned."""
    _patch_noop_persistence(monkeypatch)
    _set_credits(
        monkeypatch,
        tavily=TAVILY_FREE_CREDIT_LIMIT,
        brave=BRAVE_SEARCH_FREE_CREDIT_LIMIT,
        scrape_do=SCRAPE_DO_FREE_CREDIT_LIMIT,
        exa=EXA_FREE_CREDIT_LIMIT,
    )

    # Replace API_KEYS object so both pay-per-use keys are None
    class _MockApiKeys:
        brave_search_api_key: str | None = None
        exa_api_key: str | None = None

    monkeypatch.setattr(f"{_MODULE}.API_KEYS", _MockApiKeys())

    result = await _call_search_web("Freshmart")

    assert result.tool_used == WebSearchTool.NO_WEBSEARCH
    assert "SEARCH_UNAVAILABLE" in result.summary


async def test_search_web_continues_silently_when_one_payperus_key_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When brave pay-per-use key is configured, no AllProvidersExhaustedError is raised."""
    _patch_noop_persistence(monkeypatch)
    _set_credits(
        monkeypatch,
        tavily=TAVILY_FREE_CREDIT_LIMIT,
        brave=BRAVE_SEARCH_FREE_CREDIT_LIMIT,
        scrape_do=SCRAPE_DO_FREE_CREDIT_LIMIT,
        exa=EXA_FREE_CREDIT_LIMIT,
    )

    class _MockApiKeysWithBrave:
        brave_search_api_key: str | None = "some_brave_key"
        exa_api_key: str | None = None

    monkeypatch.setattr(f"{_MODULE}.API_KEYS", _MockApiKeysWithBrave())
    monkeypatch.setattr(
        f"{_MODULE}.brave_web_search",
        lambda q: "BRAVE_RESULTS\nbrave pay-per-use text",
    )

    # Should NOT raise AllProvidersExhaustedError
    result = await _call_search_web("Freshmart")

    assert result.tool_used == WebSearchTool.BRAVE
    assert "brave pay-per-use text" in result.summary
