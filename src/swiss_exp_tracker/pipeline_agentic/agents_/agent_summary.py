from __future__ import annotations

from enum import Enum

from agents import Agent
from agents import ModelSettings
from agents import function_tool
from pydantic import BaseModel

from swiss_exp_tracker.pipeline_agentic.libs import BRAVE_SEARCH_FREE_CREDIT_LIMIT
from swiss_exp_tracker.pipeline_agentic.libs import EXA_FREE_CREDIT_LIMIT
from swiss_exp_tracker.pipeline_agentic.libs import SCRAPE_DO_FREE_CREDIT_LIMIT
from swiss_exp_tracker.pipeline_agentic.libs import TAVILY_FREE_CREDIT_LIMIT
from swiss_exp_tracker.pipeline_agentic.libs import brave_web_search
from swiss_exp_tracker.pipeline_agentic.libs import exa_web_search
from swiss_exp_tracker.pipeline_agentic.libs import load_brave_usage
from swiss_exp_tracker.pipeline_agentic.libs import load_exa_usage
from swiss_exp_tracker.pipeline_agentic.libs import load_scrape_do_usage
from swiss_exp_tracker.pipeline_agentic.libs import load_tavily_usage
from swiss_exp_tracker.pipeline_agentic.libs import save_brave_usage
from swiss_exp_tracker.pipeline_agentic.libs import save_exa_usage
from swiss_exp_tracker.pipeline_agentic.libs import save_scrape_do_usage
from swiss_exp_tracker.pipeline_agentic.libs import save_tavily_usage
from swiss_exp_tracker.pipeline_agentic.libs import scrape_do_web_search
from swiss_exp_tracker.pipeline_agentic.libs import tavily_web_search


class WebSearchTool(Enum):
    TAVILY = "tavily"
    EXA = "exa"
    BRAVE = "brave"
    SCRAPE_DO = "scrape_do"
    PERSON = "person"


class SearchToolResult(BaseModel):
    summary: str
    tool_used: WebSearchTool


def _tool_result(summary: str, tool_used: WebSearchTool) -> SearchToolResult:
    return SearchToolResult(
        summary=summary,
        tool_used=tool_used,
    )


_tavily_free_credits_used: int = 0
_exa_free_credits_used: int = 0
_brave_free_credits_used: int = 0
_scrape_do_free_credits_used: int = 0


@function_tool
def search_web(query: str) -> SearchToolResult:
    """Search the web using the best available provider.

    Policy (in order):
    1) Tavily free tier       — up to 1 000 requests/month (direct REST).
    2) Exa free tier          — up to 1 000 requests/month (direct REST).
    3) Brave Search free tier — up to 1 000 requests/month (direct REST).
    4) Scrape.do free tier    — up to 1 000 requests/month (direct REST).
    5) Exa as-you-go          — pay-per-use fallback.
    6) Tavily as-you-go       — pay-per-use fallback.
    """
    global _tavily_free_credits_used, _exa_free_credits_used, _brave_free_credits_used, _scrape_do_free_credits_used

    # Lazy-load persisted credit counters on first call.
    if _tavily_free_credits_used == 0:
        _tavily_free_credits_used = load_tavily_usage()
    if _exa_free_credits_used == 0:
        _exa_free_credits_used = load_exa_usage()
    if _brave_free_credits_used == 0:
        _brave_free_credits_used = load_brave_usage()
    if _scrape_do_free_credits_used == 0:
        _scrape_do_free_credits_used = load_scrape_do_usage()

    # ── 1. Tavily free tier ───────────────────────────────────────────────────
    if _tavily_free_credits_used < TAVILY_FREE_CREDIT_LIMIT:
        tavily_result = tavily_web_search(query)
        if tavily_result.startswith("TAVILY_RESULTS"):
            _tavily_free_credits_used += 1
            save_tavily_usage(_tavily_free_credits_used)
            return _tool_result(
                summary=tavily_result.removeprefix("TAVILY_RESULTS\n"),
                tool_used=WebSearchTool.TAVILY,
            )
        if tavily_result == "TAVILY_CREDITS_EXCEEDED":
            _tavily_free_credits_used = TAVILY_FREE_CREDIT_LIMIT
            save_tavily_usage(_tavily_free_credits_used)
        # Any TAVILY_UNAVAILABLE → fall through to Exa.

    # ── 2. Exa free tier ─────────────────────────────────────────────────────
    if _exa_free_credits_used < EXA_FREE_CREDIT_LIMIT:
        exa_result = exa_web_search(query)
        if exa_result.startswith("EXA_RESULTS"):
            _exa_free_credits_used += 1
            save_exa_usage(_exa_free_credits_used)
            return _tool_result(
                summary=exa_result.removeprefix("EXA_RESULTS\n"),
                tool_used=WebSearchTool.EXA,
            )
        if exa_result == "EXA_CREDITS_EXCEEDED":
            _exa_free_credits_used = EXA_FREE_CREDIT_LIMIT
            save_exa_usage(_exa_free_credits_used)
        # Any EXA_UNAVAILABLE → fall through to Brave Search.

    # ── 3. Brave Search free tier ────────────────────────────────────────────
    if _brave_free_credits_used < BRAVE_SEARCH_FREE_CREDIT_LIMIT:
        brave_result = brave_web_search(query)
        if brave_result.startswith("BRAVE_RESULTS"):
            _brave_free_credits_used += 1
            save_brave_usage(_brave_free_credits_used)
            return _tool_result(
                summary=brave_result.removeprefix("BRAVE_RESULTS\n"),
                tool_used=WebSearchTool.BRAVE,
            )
        if brave_result == "BRAVE_CREDITS_EXCEEDED":
            _brave_free_credits_used = BRAVE_SEARCH_FREE_CREDIT_LIMIT
            save_brave_usage(_brave_free_credits_used)
        # Any BRAVE_UNAVAILABLE → fall through to Scrape.do.

    # ── 4. Scrape.do free tier ───────────────────────────────────────────────
    if _scrape_do_free_credits_used < SCRAPE_DO_FREE_CREDIT_LIMIT:
        scrapedo_result = scrape_do_web_search(query)
        if scrapedo_result.startswith("SCRAPEDO_RESULTS"):
            _scrape_do_free_credits_used += 1
            save_scrape_do_usage(_scrape_do_free_credits_used)
            return _tool_result(
                summary=scrapedo_result.removeprefix("SCRAPEDO_RESULTS\n"),
                tool_used=WebSearchTool.SCRAPE_DO,
            )
        if scrapedo_result == "SCRAPEDO_CREDITS_EXCEEDED":
            _scrape_do_free_credits_used = SCRAPE_DO_FREE_CREDIT_LIMIT
            save_scrape_do_usage(_scrape_do_free_credits_used)
        # Any SCRAPEDO_UNAVAILABLE → fall through to as-you-go.

    # ── 5. Exa as-you-go (pay-per-use fallback) ──────────────────────────────
    exa_result = exa_web_search(query)
    if exa_result.startswith("EXA_RESULTS"):
        _exa_free_credits_used += 1
        save_exa_usage(_exa_free_credits_used)
        return _tool_result(
            summary=exa_result.removeprefix("EXA_RESULTS\n"),
            tool_used=WebSearchTool.EXA,
        )

    # ── 6. Tavily as-you-go (pay-per-use fallback) ───────────────────────────
    tavily_result = tavily_web_search(query)
    if tavily_result.startswith("TAVILY_RESULTS"):
        _tavily_free_credits_used += 1
        save_tavily_usage(_tavily_free_credits_used)
        return _tool_result(
            summary=tavily_result.removeprefix("TAVILY_RESULTS\n"),
            tool_used=WebSearchTool.TAVILY,
        )

    return _tool_result(
        summary="SEARCH_UNAVAILABLE: all providers exhausted or unavailable.",
        tool_used=WebSearchTool.TAVILY,
    )


BASE_INSTRUCTIONS = """
    You are a merchant intelligence assistant with concentration on Switzerland-based transactions.

    Your task is to analyze a given merchant name (store, gas station, restaurant, bar, golf club, company, or similar business).

    Workflow:
    1. Call `search_web` exactly once using the merchant name as the search query.
    2. Analyze the returned search results.
    3. Generate a concise and structured merchant summary.

    The search tool automatically selects the best provider:
    Tavily free (1,000/month) -> Exa free (1,000/month) -> Brave Search free (1,000/month) -> Scrape.do free (1,000/month) -> Exa as-you-go -> Tavily as-you-go.

    Return the result in a full text summary that includes the following information when available:
    - Name of the Merchant
    - Location of the Merchant
    - What the merchant is doing or selling: What is the main business? Do they have side products or services?
    - If possible, typical products or services they offer.
    - Typical characteristics of the customers.
    - How many stores do they have?

    Rules:
    - Always call `search_web` exactly once before generating the summary.
    - High probability that the merchant is based in Switzerland, but not guaranteed. Focus on Switzerland-relevant information when available.
    - Use only information supported by search results.
    - First search for a match in Switzerland, then expand to other countries if not found.
    - Do not invent facts.
    - If information is unavailable, use "Unknown".
    - Keep responses concise and factual.
"""


summary_agent = Agent(
    name="Summary Agent",
    instructions=BASE_INSTRUCTIONS,
    tools=[search_web],
    model="gpt-4o-mini",
    model_settings=ModelSettings(tool_choice="required"),
    output_type=SearchToolResult,
)
