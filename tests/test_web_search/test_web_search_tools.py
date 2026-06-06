"""Integration tests for each web-search provider.

Normal run (uses recorded cassettes, no real API calls):
    pytest tests/test_web_search/test_web_search_tools.py -v

Re-record cassettes against the live APIs (requires all API keys in .env):
    pytest tests/test_web_search/test_web_search_tools.py -v --record-mode=rewrite

Each test requires the corresponding API key to be present in the environment
(loaded from .env) when recording.  Tests are automatically skipped when the
key is missing and no cassette exists yet.

The query used is "Migros" — a well-known Swiss retailer that all providers
should be able to find.  The tests assert:
  - the result starts with the expected prefix (provider returned data)
  - the result body is non-empty
  - the result body contains at least one keyword that proves it is related to
    the query ("migros", "switzerland", "supermarket", or "retailer")
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from swiss_exp_tracker.pipeline_agentic.web_search import API_KEYS
from swiss_exp_tracker.pipeline_agentic.web_search import brave_web_search
from swiss_exp_tracker.pipeline_agentic.web_search import exa_web_search
from swiss_exp_tracker.pipeline_agentic.web_search import load_brave_usage
from swiss_exp_tracker.pipeline_agentic.web_search import load_exa_usage
from swiss_exp_tracker.pipeline_agentic.web_search import load_scrape_do_usage
from swiss_exp_tracker.pipeline_agentic.web_search import load_tavily_usage
from swiss_exp_tracker.pipeline_agentic.web_search import save_brave_usage
from swiss_exp_tracker.pipeline_agentic.web_search import save_exa_usage
from swiss_exp_tracker.pipeline_agentic.web_search import save_scrape_do_usage
from swiss_exp_tracker.pipeline_agentic.web_search import save_tavily_usage
from swiss_exp_tracker.pipeline_agentic.web_search import scrape_do_web_search
from swiss_exp_tracker.pipeline_agentic.web_search import tavily_web_search

# VCR record modes that mean a real HTTP request was made
_RECORDING_MODES = {"rewrite", "new_episodes", "all"}


QUERY = "Migros"
EXPECTED_KEYWORDS = {"migros", "switzerland", "supermarket", "retailer", "grocery"}


# Scrub API keys from recorded cassettes so they are safe to commit.
_FILTER_HEADERS = [
    "authorization",
    "x-api-key",
    "x-subscription-token",
]
_FILTER_QUERY_PARAMS = ["token", "apikey", "api_key"]


@pytest.fixture(scope="module")
def vcr_config() -> dict[Any, Any]:
    return {
        "cassette_library_dir": str(Path(__file__).parent / "cassettes"),
        "filter_headers": _FILTER_HEADERS,
        "filter_query_parameters": _FILTER_QUERY_PARAMS,
        "filter_post_data_parameters": ["api_key"],
        "decode_compressed_response": True,
    }


def _contains_keyword(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in EXPECTED_KEYWORDS)


# ── Tavily ────────────────────────────────────────────────────────────────────


@pytest.mark.vcr
@pytest.mark.skipif(
    not API_KEYS.tavily_api_key,
    reason="TAVILY_API_KEY not set",
)
def test_tavily_web_search_returns_results(vcr: Any) -> None:
    used_before = load_tavily_usage()
    result = tavily_web_search(QUERY)

    assert not result.startswith("TAVILY_UNAVAILABLE"), (
        f"Tavily returned an error: {result}"
    )
    assert not result.startswith("TAVILY_CREDITS_EXCEEDED"), (
        "Tavily monthly credits exhausted"
    )
    assert result.startswith("TAVILY_RESULTS"), (
        f"Unexpected Tavily response prefix: {result[:80]}"
    )

    body = result.removeprefix("TAVILY_RESULTS\n")
    assert body.strip(), "Tavily result body is empty"
    assert _contains_keyword(body), (
        f"Tavily result does not mention expected keywords. Got:\n{body[:300]}"
    )

    if vcr.record_mode in _RECORDING_MODES:
        save_tavily_usage(used_before + 1)


# ── Exa ───────────────────────────────────────────────────────────────────────


@pytest.mark.vcr
@pytest.mark.skipif(
    not API_KEYS.exa_api_key,
    reason="EXA_API_KEY not set",
)
def test_exa_web_search_returns_results(vcr: Any) -> None:
    used_before = load_exa_usage()
    result = exa_web_search(QUERY)

    assert not result.startswith("EXA_UNAVAILABLE"), f"Exa returned an error: {result}"
    assert not result.startswith("EXA_CREDITS_EXCEEDED"), (
        "Exa monthly credits exhausted"
    )
    assert result.startswith("EXA_RESULTS"), (
        f"Unexpected Exa response prefix: {result[:80]}"
    )

    header, _, body = result.partition("\n")
    num_results = int(header.removeprefix("EXA_RESULTS:"))
    assert body.strip(), "Exa result body is empty"
    assert _contains_keyword(body), (
        f"Exa result does not mention expected keywords. Got:\n{body[:300]}"
    )

    if vcr.record_mode in _RECORDING_MODES:
        save_exa_usage(used_before + num_results * 10)


# ── Brave Search ──────────────────────────────────────────────────────────────


@pytest.mark.vcr
@pytest.mark.skipif(
    not API_KEYS.brave_search_api_key,
    reason="BRAVE_SEARCH_API_KEY not set",
)
def test_brave_web_search_returns_results(vcr: Any) -> None:
    used_before = load_brave_usage()
    result = brave_web_search(QUERY)

    assert not result.startswith("BRAVE_UNAVAILABLE"), (
        f"Brave Search returned an error: {result}"
    )
    assert not result.startswith("BRAVE_CREDITS_EXCEEDED"), (
        "Brave Search monthly credits exhausted"
    )
    assert result.startswith("BRAVE_RESULTS"), (
        f"Unexpected Brave response prefix: {result[:80]}"
    )

    body = result.removeprefix("BRAVE_RESULTS\n")
    assert body.strip(), "Brave result body is empty"
    assert _contains_keyword(body), (
        f"Brave result does not mention expected keywords. Got:\n{body[:300]}"
    )

    if vcr.record_mode in _RECORDING_MODES:
        save_brave_usage(used_before + 1)


# ── Scrape.do ─────────────────────────────────────────────────────────────────


@pytest.mark.vcr
@pytest.mark.skipif(
    not API_KEYS.scrape_do_api_key,
    reason="SCRAPE_DO_API_KEY not set",
)
def test_scrape_do_web_search_returns_results(vcr: Any) -> None:
    used_before = load_scrape_do_usage()
    result = scrape_do_web_search(QUERY)

    assert not result.startswith("SCRAPEDO_UNAVAILABLE"), (
        f"Scrape.do returned an error: {result}"
    )
    assert not result.startswith("SCRAPEDO_CREDITS_EXCEEDED"), (
        "Scrape.do monthly credits exhausted"
    )
    assert result.startswith("SCRAPEDO_RESULTS"), (
        f"Unexpected Scrape.do response prefix: {result[:80]}"
    )

    body = result.removeprefix("SCRAPEDO_RESULTS\n")
    assert body.strip(), "Scrape.do result body is empty"
    assert _contains_keyword(body), (
        f"Scrape.do result does not mention expected keywords. Got:\n{body[:300]}"
    )

    if vcr.record_mode in _RECORDING_MODES:
        save_scrape_do_usage(used_before + 1)
