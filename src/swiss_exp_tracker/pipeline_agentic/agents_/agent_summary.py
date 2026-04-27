from __future__ import annotations

from enum import Enum

from agents import Agent
from agents import ModelSettings
from agents import function_tool
from pydantic import BaseModel

from swiss_exp_tracker.pipeline_agentic.libs import BRIGHT_DATA_FREE_CREDIT_LIMIT
from swiss_exp_tracker.pipeline_agentic.libs import TAVILY_FREE_CREDIT_LIMIT
from swiss_exp_tracker.pipeline_agentic.libs import brightdata_web_search
from swiss_exp_tracker.pipeline_agentic.libs import is_serpapi_quota_exceeded
from swiss_exp_tracker.pipeline_agentic.libs import load_brightdata_usage
from swiss_exp_tracker.pipeline_agentic.libs import load_tavily_usage
from swiss_exp_tracker.pipeline_agentic.libs import save_brightdata_usage
from swiss_exp_tracker.pipeline_agentic.libs import save_tavily_usage
from swiss_exp_tracker.pipeline_agentic.libs import serpapi_web_search
from swiss_exp_tracker.pipeline_agentic.libs import tavily_web_search


class WebSearchTool(Enum):
    BRIGHT_DATA = "bright_data"
    TAVILY = "tavily"
    SERPAPI = "serpapi"
    PERSON = "person"


class SearchToolResult(BaseModel):
    summary: str
    tool_used: WebSearchTool


def _tool_result(summary: str, tool_used: WebSearchTool) -> SearchToolResult:
    return SearchToolResult(
        summary=summary,
        tool_used=tool_used,
    )


_brightdata_free_credits_used: int = 0
_tavily_free_credits_used: int = 0


@function_tool
def search_web(query: str) -> SearchToolResult:
    """Search the web using the best available provider.

    Policy (in order):
    1) Tavily free tier       — up to 1 000 requests/month (direct REST, fast).
    2) SerpAPI                — until monthly quota is exceeded (direct REST, fast).
    3) Bright Data free tier  — up to 5 000 requests/month (MCP, slow fallback).
    """
    global _brightdata_free_credits_used, _tavily_free_credits_used

    # Lazy-load persisted credit counters on first call.
    if _brightdata_free_credits_used == 0:
        _brightdata_free_credits_used = load_brightdata_usage()
    if _tavily_free_credits_used == 0:
        _tavily_free_credits_used = load_tavily_usage()

    # ── 1. Tavily free tier (direct REST) ────────────────────────────────────
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
        # Any TAVILY_UNAVAILABLE → fall through to SerpAPI.

    # ── 2. SerpAPI (direct REST) ──────────────────────────────────────────────
    if not is_serpapi_quota_exceeded():
        serp_result = serpapi_web_search(query)
        if serp_result.startswith("SERPAPI_RESULTS"):
            return _tool_result(
                summary=serp_result.removeprefix("SERPAPI_RESULTS\n"),
                tool_used=WebSearchTool.SERPAPI,
            )
        # SERPAPI_QUOTE_EXCEEDED or SERPAPI_UNAVAILABLE → fall through.

    # ── 3. Bright Data free tier (MCP, slow) ─────────────────────────────────
    if _brightdata_free_credits_used < BRIGHT_DATA_FREE_CREDIT_LIMIT:
        bd_result = brightdata_web_search(query)
        if bd_result.startswith("BRIGHTDATA_RESULTS"):
            _brightdata_free_credits_used += 1
            save_brightdata_usage(_brightdata_free_credits_used)
            return _tool_result(
                summary=bd_result.removeprefix("BRIGHTDATA_RESULTS\n"),
                tool_used=WebSearchTool.BRIGHT_DATA,
            )
        if bd_result == "BRIGHTDATA_CREDITS_EXCEEDED":
            _brightdata_free_credits_used = BRIGHT_DATA_FREE_CREDIT_LIMIT
            save_brightdata_usage(_brightdata_free_credits_used)

    return _tool_result(
        summary="SEARCH_UNAVAILABLE: all providers exhausted or unavailable.",
        tool_used=WebSearchTool.SERPAPI,
    )


BASE_INSTRUCTIONS = """
    You are a merchant intelligence assistant.

    Your task is to analyze a given merchant name (store, gas station, restaurant, bar, golf club, company, or similar business).

    Workflow:
    1. Call `search_web` exactly once using the merchant name as the search query.
    2. Analyze the returned search results.
    3. Generate a concise and structured merchant summary.

    The search tool automatically selects the best provider:
    Tavily free (1,000/month) -> SerpAPI free (250/month) -> Bright Data free (5,000/month, slow).

    Return the result in a full text summary that includes the following information when available:
    - Name of the Merchant
    - Location of the Merchant
    - What the merchant is doing or selling: What is the main business? Do they have side products or services?
    - If possible, typical products or services they offer.
    - Typical characteristics of the customers.
    - How many stores do they have?

    Rules:
    - Always call `search_web` exactly once before generating the summary.
    - Use only information supported by search results.
    - Do not invent facts.
    - If information is unavailable, use "Unknown".
    - Keep responses concise and factual.
    - Focus on usefulness for Switzerland-based transaction classification when relevant.
"""


summary_agent = Agent(
    name="Summary Agent",
    instructions=BASE_INSTRUCTIONS,
    tools=[search_web],
    model="gpt-4o-mini",
    model_settings=ModelSettings(tool_choice="required"),
    output_type=SearchToolResult,
)
